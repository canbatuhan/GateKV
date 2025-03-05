from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageServicer


class GateKV_StorageNode_Server(GateKV_StorageServicer):
    def __init__(self, server_conf:dict):
        super().__init__()
        self.__config = server_conf

    def Set(self, request, context):
        # TODO #2 : Implement Set service
        return super().Set(request, context)
    
    def SetData(self, request, context):
        # TODO #1 : Implement SetData service
        return super().SetData(request, context)
    
    def Get(self, request, context):
        # TODO #2 : Implement Get service
        return super().Get(request, context)
    
    def GetData(self, request, context):
        # TODO #1 : Implement GetData service
        return super().GetData(request, context)
    
    def Rem(self, request, context):
        # TODO #2 : Implement Rem service
        return super().Rem(request, context)
    
    def RemData(self, request, context):
        # TODO #1 : Implement RemData service
        return super().RemData(request, context)