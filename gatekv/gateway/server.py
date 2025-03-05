from gatekv.gateway.client import GateKV_GatewayNode_Client
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayServicer


class GateKV_GatewayNode_Server(GateKV_GatewayServicer):
    def __init__(self):
        super().__init__()

    def Set(self, request, context):
        return super().Set(request, context)
    
    def Get(self, request, context):
        return super().Get(request, context)
    
    def Rem(self, request, context):
        return super().Rem(request, context)