from concurrent.futures import ThreadPoolExecutor
import threading
import time
import grpc
from gatekv.gateway.client import GateKV_GatewayNode_Client
from gatekv.gateway.service import GateKV_gateway_pb2_grpc
from gatekv.gateway.util import GateKV_GatewayNode_Logger, GateKV_GatewayNode_PairVersionMap, GateKV_GatewayNode_StateMachineMap
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayServicer
from gatekv.gateway.statemachine import GateKV_GatewayNode_Events
from gatekv.storage.service import GateKV_storage_pb2

class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self, server_conf:dict, client_conf:dict, state_machine_conf:dict):
        super().__init__()
        self.__config = server_conf
        self.__server = grpc.server(thread_pool=ThreadPoolExecutor(max_workers=self.__config.get("workers")))
        GateKV_gateway_pb2_grpc.add_GateKV_GatewayServicer_to_server(self, self.__server)
        self.__server.add_insecure_port("0.0.0.0:{}".format(self.__config.get("port")))

        self.__gossip_event = threading.Event()
        self.__gossip_lock  = threading.Lock()
        self.__gossip_batch = GateKV_gateway_pb2.GossipMessage(sets=[], rems=[])
        self.__gossip_period = self.__config.get("gossip")

        self.__client = GateKV_GatewayNode_Client(client_conf)
        self.__machine_map = GateKV_GatewayNode_StateMachineMap(state_machine_conf)
        self.__version_map = GateKV_GatewayNode_PairVersionMap()

        self.__logger = GateKV_GatewayNode_Logger("Server")

    def __remove_duplicates(self, gossip_data):
        for each in self.__gossip_batch.sets:
            if each.key == gossip_data.key: self.__gossip_batch.sets.remove(each)

        for each in self.__gossip_batch.rems:
            if each.key == gossip_data.key: self.__gossip_batch.rems.remove(each)

    def __append_set(self, gossip_data):
        self.__gossip_lock.acquire()
        self.__remove_duplicates(gossip_data)
        self.__gossip_batch.sets.extend([gossip_data])
        self.__gossip_lock.release()

    def __append_rem(self, gossip_data):
        self.__gossip_lock.acquire()
        self.__remove_duplicates(gossip_data)
        self.__gossip_batch.rems.extend([gossip_data])
        self.__gossip_lock.release()
        
    def __append_to_gossip_batch(self, callback, gossip_data):
        threading.Thread(callback(gossip_data)).start()

    def Register(self, request, context):
        self.__logger.log("Registering new neighbour...")
        try:
            self.__client.register_neighbour(request.type,
                                             request.alias,
                                             request.sender.host,
                                             request.sender.port)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))

        return GateKV_gateway_pb2.RegisterResponse(alias = self.__config.get("alias"))

    def Set(self, request, context):
        self.__logger.log("Setting new key-value pair...")
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

            else:
                self.__machine_map.removeStateMachine(request.key)
                self.__version_map.removePairVersion(request.key)
                # Roll-back

        except Exception as e:
            self.__logger.log(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.SetResponse(success = success)       
    
    def Get(self, request, context):
        self.__logger.log("Getting value for key...")
        success = False
        value = None

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.READ)
            success, value = self.__client.get_protocol(request.key)
            
            if not success:
                self.__machine_map.removeStateMachine(request.key)

        except Exception as e:
            self.__logger.log(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.GetResponse(success = success, value = value)
    
    def Rem(self, request, context):
        self.__logger.log("Removing key-value pair...")
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.rem_protocol(request.key)

            if success:
                self.__machine_map.removeStateMachine(request.key)
                self.__version_map.removePairVersion(request.key)
                self.__append_to_gossip_batch(self.__append_rem,
                                              GateKV_gateway_pb2.GossipData(key=request.key))

            else:
                # Roll-back
                pass
        
        except Exception as e:
            self.__logger.log(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.RemResponse(success = success)
        
    def Gossip(self, request, context):
        self.__logger.log("Gossiping with neighbours...")
        set_success = False
        rem_success = False

        self.__logger.log(f"Remove batch for gossipn = {request.rems}")

        try:
            batch_set = GateKV_storage_pb2.BatchSetRequest(pairs = [])
            batch_rem = GateKV_storage_pb2.BatchRemRequest(pairs = [])
            
            for each in request.sets:
                try:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.WRITE)
                    batch_set.pairs.extend([GateKV_storage_pb2.SetRequest(key=each.key, value=each.value)])
                except Exception as e:
                    request.sets.remove(each)
                    self.__logger.log(e.with_traceback(None))

            for each in request.rems:
                try:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.WRITE)
                    batch_rem.pairs.extend([GateKV_storage_pb2.RemRequest(key=each.key)])
                except Exception as e:
                    request.rems.remove(each)
                    self.__logger.log(e.with_traceback(None))
            
            set_success = self.__client.batch_set_protocol(batch_set)
            rem_success = self.__client.batch_rem_protocol(batch_rem)
            
            if set_success:
                for each in request.sets:
                    machine = self.__machine_map.getStateMachine(each.key)
                    self.__version_map.setPairVersion(each.key, each.version)
                    machine.send_event(GateKV_GatewayNode_Events.DONE)
            
            if rem_success:
                for each in request.rems:
                    self.__machine_map.removeStateMachine(each.key)
                    self.__version_map.removePairVersion(each.key)
            
            else:
                for each in batch_rem.pairs:
                    machine = self.__machine_map.getStateMachine(each.key)
                    machine.send_event(GateKV_GatewayNode_Events.DONE)

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
            while not self.__gossip_event.is_set():
                try:
                    self.__logger.log("Gossiping with neighbours...")
                    self.__gossip_lock.acquire()
                    success = self.__client.gossip_protocol(self.__gossip_batch)
                    if success:
                        self.__gossip_batch = GateKV_gateway_pb2.GossipMessage(sets=[], rems=[])
                    self.__gossip_lock.release()
                    time.sleep(self.__gossip_period)

                except:
                    pass

        threading.Thread(target = __loop, daemon = True).start()

    def __infinite_loop(self):
        self.__server.wait_for_termination()
        self.__gossip_event.set()

    def start(self):
        self.__start_server()
        self.__register()
        self.__start_gossip()
        self.__infinite_loop()

