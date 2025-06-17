from concurrent.futures import ThreadPoolExecutor
import threading
import time
import grpc
from gatekv.gateway.client import GateKV_GatewayNode_Client
from gatekv.gateway.service import GateKV_gateway_pb2_grpc
from gatekv.gateway.util import GateKV_GatewayNode_Logger, GateKV_GatewayNode_PairVersionMap, GateKV_GatewayNode_StateMachineMap
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayServicer
from gatekv.gateway.statemachine import GateKV_GatewayNode_Events, GateKV_GatewayNode_TimeoutException
from gatekv.storage.service import GateKV_storage_pb2

class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self, server_conf:dict, client_conf:dict, state_machine_conf:dict):
        super().__init__()
        self.__config = server_conf
        self.__server = grpc.server(thread_pool=ThreadPoolExecutor())
        GateKV_gateway_pb2_grpc.add_GateKV_GatewayServicer_to_server(self, self.__server)
        self.__server.add_insecure_port("0.0.0.0:{}".format(self.__config.get("port")))

        self.__gossip_stop    = threading.Event()
        self.__gossip_lock    = threading.Lock()
        self.__gossip_batch   = GateKV_gateway_pb2.GossipMessage(sets=[], rems=[])
        self.__gossip_period = self.__config.get("gossip")

        self.__client = GateKV_GatewayNode_Client(client_conf)
        self.__machine_map = GateKV_GatewayNode_StateMachineMap(state_machine_conf)
        self.__version_map = GateKV_GatewayNode_PairVersionMap()

        self.__logger = GateKV_GatewayNode_Logger("Server")

    def __remove_duplicates(self, gossip_data):
        for each in self.__gossip_batch.sets:
            if each.key == gossip_data.key:
                self.__gossip_batch.sets.remove(each)
                break

        for each in self.__gossip_batch.rems:
            if each.key == gossip_data.key:
                self.__gossip_batch.rems.remove(each)
                break

    def __append_set(self, gossip_data):
        with self.__gossip_lock:
            self.__remove_duplicates(gossip_data)
            self.__gossip_batch.sets.extend([gossip_data])

    def __append_rem(self, gossip_data):
        with self.__gossip_lock:
            self.__remove_duplicates(gossip_data)
            self.__gossip_batch.rems.extend([gossip_data])
        
    def __append_to_gossip_batch(self, callback, gossip_data):
        threading.Thread(target=callback, args=(gossip_data,), daemon=True).start()

    def Register(self, request, context):
        try:
            self.__client.register_neighbour(request.type,
                                             request.alias,
                                             request.sender.host,
                                             request.sender.port)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))

        return GateKV_gateway_pb2.RegisterResponse(alias = self.__config.get("alias"))

    def Set(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.set_protocol(request.key, request.value)

            if success:
                self.__version_map.setPairVersion(request.key)
                self.__append_to_gossip_batch(self.__append_set,
                                              GateKV_gateway_pb2.GossipData(key=request.key,
                                                                            value=request.value,
                                                                            version=self.__version_map.getPairVersion(request.key)))

            else: # Roll-back
                self.__machine_map.removeStateMachine(request.key)
                self.__version_map.removePairVersion(request.key)

            machine.send_event(GateKV_GatewayNode_Events.DONE)

        except GateKV_GatewayNode_TimeoutException as e:
            self.__logger.log(e.with_traceback(None))

        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            machine.send_event(GateKV_GatewayNode_Events.DONE)
        
        return GateKV_gateway_pb2.SetResponse(success = success)       
    
    def Get(self, request, context):
        success = False
        value = None

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.READ)
            success, value = self.__client.get_protocol(request.key)

            if success:
                machine.send_event(GateKV_GatewayNode_Events.DONE)

            else: # Pair does not exists
                self.__machine_map.removeStateMachine(request.key)

        except GateKV_GatewayNode_TimeoutException as e:
            self.__logger.log(e.with_traceback(None))

        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            machine.send_event(GateKV_GatewayNode_Events.DONE)
        
        return GateKV_gateway_pb2.GetResponse(success = success, value = value)
    
    def Rem(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.rem_protocol(request.key)

            self.__machine_map.removeStateMachine(request.key)
            self.__version_map.removePairVersion(request.key)
            self.__append_to_gossip_batch(self.__append_rem,
                                          GateKV_gateway_pb2.GossipData(key=request.key))
        
        except GateKV_GatewayNode_TimeoutException as e:
            self.__logger.log(e.with_traceback(None))

        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            machine.send_event(GateKV_GatewayNode_Events.DONE)

        return GateKV_gateway_pb2.RemResponse(success = success)
        
    def Gossip(self, request, context):
        set_success = False
        rem_success = False
        
        try:
            batch_set = GateKV_storage_pb2.BatchSetRequest(pairs = [])
            batch_rem = GateKV_storage_pb2.BatchRemRequest(pairs = [])
            
            for each in request.sets:
                try:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.WRITE)
                    batch_set.pairs.extend([GateKV_storage_pb2.SetRequest(key=each.key, value=each.value)])

                except Exception as e:
                    self.__logger.log(e.with_traceback(None))

            for each in request.rems:
                try:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.WRITE)
                    batch_rem.pairs.extend([GateKV_storage_pb2.RemRequest(key=each.key)])

                except Exception as e:
                    self.__logger.log(e.with_traceback(None))
            
            set_success = self.__client.batch_set_protocol(batch_set)
            rem_success = self.__client.batch_rem_protocol(batch_rem)

            if set_success:
                for each in batch_set.pairs:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.DONE)
                    # self.__version_map.setPairVersion(each.key, each.version)

            for each in batch_rem.pairs:
                self.__machine_map.removeStateMachine(each.key)
                self.__version_map.removePairVersion(each.key)

        except Exception as e:
            self.__logger.log(e.with_traceback(None))
        
        return GateKV_gateway_pb2.GossipAck(success = set_success and rem_success)
    
    def __start_server(self):
        self.__server.start()

    def __register(self):
        self.__client.register_protocol("gateway",
                                        self.__config.get("alias"),
                                        self.__config.get("host"),
                                        self.__config.get("port"))

    def __start_gossip(self):
        def __loop():
            while not self.__gossip_stop.is_set():
                try:
                    self.__logger.log("Sending gossip message to neighbours...")
                    with self.__gossip_lock:
                        success = self.__client.gossip_protocol(self.__gossip_batch)
                        if success:
                            del self.__gossip_batch.sets[:]
                            del self.__gossip_batch.rems[:]
                    time.sleep(self.__gossip_period)

                except Exception as e:
                    self.__logger.log(e.with_traceback(None))

        threading.Thread(target = __loop, daemon = True).start()

    def __infinite_loop(self):
        self.__server.wait_for_termination()
        self.__gossip_stop.set()

    def start(self):
        self.__start_server()
        self.__register()
        self.__start_gossip()
        self.__infinite_loop()

