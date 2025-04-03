import grpc
from concurrent import futures
from gatekv.storage.client import GateKV_StorageNode_Client
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc
from gatekv.storage.store import GateKV_StorageNode_LocalStore

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, server_conf:dict, client_conf:dict):
        super().__init__()
        self.__config  = server_conf
        self.__client  = GateKV_StorageNode_Client("localhost:50051",{"storage-01": "localhost:50052", "storage-02": "localhost:50053"})
        self.__storage = GateKV_StorageNode_LocalStore({})

    def Register(self, request, context: grpc.ServicerContext):
        return GateKV_storage_pb2.RegisterResponse(alias=request.alias)
    
    def Set(self, request, context: grpc.ServicerContext):
        self.__client.callSetOnGateway(request.key, request.value, context.peer())
        return GateKV_storage_pb2.Empty() 
    
    def SetData(self, request, alias):
        self.__storage.set(request.key, request.value)
        return GateKV_storage_pb2.SetResponse(success=True)
    
    def Get(self, request, context: grpc.ServicerContext):
        value = self.__storage.get(request.key)
        print(value)
        if value != None:
            return GateKV_storage_pb2.GetResponse(success=True, value=value[0])
        else: 
            print("Key value does not exist")
            return self.__client.callGetOnStorage(request.key, context.peer())
    
    def GetData(self, request, alias):
        value = self.__storage.get(request.key)
        if value != None:
            return GateKV_storage_pb2.GetResponse(success=True, value=value[0])
        return GateKV_storage_pb2.GetResponse(success=False, value=None)
    
    def Rem(self, request, context: grpc.ServicerContext):
        self.__client.callRemOnGateway(request.key, context.peer())
        return GateKV_storage_pb2.Empty() 
    
    def RemData(self, request, alias):
        value = self.__storage.get(request.key)
        if value != None:
            self.__storage.rem(request.key)
            return GateKV_storage_pb2.RemResponse(success=True)
        return GateKV_storage_pb2.RemResponse(success=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(GateKV_StorageNode_Server({}, {}), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Storage Server running on port 50052")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
