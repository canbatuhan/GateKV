from gatekv.gateway.client import GateKV_GatewayNode_Client
from gatekv.gateway.maps import GateKV_GatewayNode_PairInProcessMap, GateKV_GatewayNode_PairOwnerMap, GateKV_GatewayNode_StateMachineMap
from gatekv.gateway.protocols import GateKV_GatewayNode_Protocols
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayServicer
from gatekv.gateway.statemachine import GateKV_GatewayNode_Events

class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self, server_conf:dict, client_conf:dict, protocol_conf:dict):
        super().__init__()
        self.__config = server_conf

        self.__client = GateKV_GatewayNode_Client(client_conf)
        self.__machine_map = GateKV_GatewayNode_StateMachineMap()
        self.__owner_map = GateKV_GatewayNode_PairOwnerMap()

    def Register(self, request, context):
        return super().Register(request, context)

    def Set(self, request, context):
        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)

            current_owners = self.__owner_map.getPairOwners(request.key)
            new_owners, success = self.__client.set_protocol(current_owners)
            if current_owners == None:
                self.__owner_map.addPairOwners(request.key, new_owners)

            machine.send_event(GateKV_GatewayNode_Events.DONE)
            return GateKV_gateway_pb2.SetResponse(success = success)
        
        except Exception as e:
            print(e.with_traceback(None))
            return GateKV_gateway_pb2.SetReponse(success = False)        
    
    def Get(self, request, context):
        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.READ)

            current_owners = self.__owner_map.getPairOwners(request.key)
            success, value = self.__client.get_protocol(current_owners)

        except Exception as e:
            print(e.with_traceback(None))

        machine.send_event(GateKV_GatewayNode_Events.DONE)
        return GateKV_gateway_pb2.GetResponse(success=success, value=value)
    
    def Rem(self, request, context):
        try:
            machine = self.__machine_map.getStateMachine(request.key)
            machine.send_event(GateKV_GatewayNode_Events.WRITE)

            current_owners = self.__owner_map.getPairOwners(request.key)
            new_owners, success = self.__client.rem_protocol(current_owners)
            if current_owners != None:
                self.__owner_map.removePairOwners(request.key, current_owners)
                self.__machine_map.removeStateMachine(request.key)

            machine.send_event(GateKV_GatewayNode_Events.DONE)
            return GateKV_gateway_pb2.RemResponse(success = success)
        
        except Exception as e:
            print(e.with_traceback(None))
            return GateKV_gateway_pb2.RemResponse(success = False) 