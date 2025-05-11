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

class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self, server_conf:dict, client_conf:dict, state_machine_conf:dict):
        super().__init__()
        self.__config = server_conf
        self.__server = grpc.server(thread_pool=ThreadPoolExecutor(max_workers=self.__config.get("workers")))
        GateKV_gateway_pb2_grpc.add_GateKV_GatewayServicer_to_server(self, self.__server)
        self.__server.add_insecure_port("0.0.0.0:{}".format(self.__config.get("port")))

        self.__gossip_event = threading.Event()
        self.__gossip_batch = GateKV_gateway_pb2.GossipMessage()
        self.__gossip_period = self.__config.get("gossip")

        self.__client = GateKV_GatewayNode_Client(client_conf)
        self.__machine_map = GateKV_GatewayNode_StateMachineMap(state_machine_conf)
        self.__version_map = GateKV_GatewayNode_PairVersionMap()

        self.__logger = GateKV_GatewayNode_Logger("Server")

    def Register(self, request, context):
        try:
            self.__client.registerNeighbour(request.type,
                                            request.alias,
                                            request.host,
                                            request.port)
        except Exception as e:
            print(e.with_traceback(None))

        return GateKV_gateway_pb2.RegisterResponse(alias = self.__config.get("alias"))

    def Set(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.set_protocol(request.key, request.value)

            if success:
                self.__version_map.setPairVersion(request.key)
                gossip_data = GateKV_gateway_pb2.GossipData(key = request.key,
                                                            value = request.value,
                                                            version = self.__version_map.getPairVersion(request.key))
                self.__gossip_batch.sets.extend([gossip_data])

            else:
                self.__machine_map.removeStateMachine(request.key)
                self.__version_map.removePairVersion(request.key)
                # Roll-back

        except Exception as e:
            print(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.SetResponse(success = success)       
    
    def Get(self, request, context):
        success = False
        value = None

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.READ)
            success, value = self.__client.get_protocol(request.key)
            
            if not success:
                self.__machine_map.removeStateMachine(request.key)

        except Exception as e:
            print(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.GetResponse(success = success, value = value)
    
    def Rem(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.rem_protocol(request.key)

            if success:
                self.__machine_map.removeStateMachine(request.key)
                self.__version_map.removePairVersion(request.key)
                gossip_data = GateKV_gateway_pb2.GossipData(key = request.key)
                self.__gossip_batch.rems.extend([gossip_data])

            else:
                # Roll-back
                pass
        
        except Exception as e:
            print(e.with_traceback(None))
        
        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.RemResponse(success = success)
        
    def Gossip(self, request, context):
        try:
            # Set Phase
            for each in request.sets:
                machine = self.__machine_map.getStateMachine(each.key)
                machine.send_event(GateKV_GatewayNode_Events.WRITE)

            for each in request.rems:
                machine = self.__machine_map.getStateMachine(each.key)
                machine.send_event(GateKV_GatewayNode_Events.WRITE)

            set_success = self.__client.batch_set_protocol(request.sets)
            rem_success = self.__client.batch_rem_protocol(request.rems)
            
            if set_success:
                for each in request.sets:
                    self.__version_map.setPairVersion(each.key, each.version)
            
            if rem_success:
                for each in request.rems:
                    self.__machine_map.removeStateMachine(each.key)
                    self.__version_map.removePairVersion(each.key)

        except Exception as e:
            print(e.with_traceback(None))
        
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
                    success = self.__client.gossip_protocol(self.__gossip_batch)
                    if success:
                        del self.__gossip_batch.sets
                        del self.__gossip_batch.rems
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

