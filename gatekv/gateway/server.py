from gatekv.gateway.client import GateKV_GatewayNode_Client
from gatekv.gateway.util import GateKV_GatewayNode_PairVersionMap, GateKV_GatewayNode_StateMachineMap
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayServicer
from gatekv.gateway.statemachine import GateKV_GatewayNode_Events

class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self, server_conf:dict, client_conf:dict):
        super().__init__()
        self.__config = server_conf

        self.__client = GateKV_GatewayNode_Client(client_conf)
        self.__machine_map = GateKV_GatewayNode_StateMachineMap()
        self.__version_map = GateKV_GatewayNode_PairVersionMap()
        self.__gossip_batch = GateKV_gateway_pb2.GossipMessage()

    def Register(self, request, context):
        return super().Register(request, context)

    def Set(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.set_protocol(request.key, request.value)

            if success:
                gossip_data = GateKV_gateway_pb2.GossipData(...)
                self.__gossip_batch.sets.extend([])
                pass

            else:
                self.__machine_map.removeStateMachine(request.key)
                # Roll-back
                pass

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
        return GateKV_gateway_pb2.GetResponse(success=success, value=value)
    
    def Rem(self, request, context):
        success = False

        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)
            success = self.__client.rem_protocol(request.key)

            if success:
                self.__machine_map.removeStateMachine(request.key)
                # Add to gossip map

            else:
                # Roll-back
                pass
        
        except Exception as e:
            print(e.with_traceback(None))
        
        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.RemResponse(success = success)
        
    def Gossip(self, request, context):
        success = False

        try:
            # Set Phase
            set_data = request.sets
            

            # Remove Phase

        except Exception as e:
            print(e.with_traceback(None))
        
        return GateKV_gateway_pb2.GossipAck(success = False)
