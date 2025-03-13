import grpc
from concurrent import futures
from gatekv.storage.client import GateKV_StorageNode_Client
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, server_conf:dict, client_conf:dict):
        super().__init__()
        self.__config  = server_conf
        self.__client  = GateKV_StorageNode_Client(client_conf)
        self.__storage = {}
        # MAIN HOMEWORK : Update message formats with UserInfo for necessary functions.
        # MAIN HOMEWORK : Are there any way to get hostname (ip-address) from a rpc request
        # For example, context.src ?
        # MAIN HOMEWORK : Convert storage to GateKV_StorageNode_LocalStore object
        # AIM : Aim is to came up with a storage node that we can simulate with some requests
        # using Bloom RPC

    def Register(self, request, context):
        # maybe print out smth "node registered: {request.alias} ({request.type})"
        return GateKV_storage_pb2.RegisterResponse(alias=request.alias)
    
    def Set(self, request, context):
        """response = self.__gateway_stub.Set(
            GateKV_gateway_pb2.SetRequest(key=request.key, value=request.value)
        )"""
        self.__client.callSetOnGateway(request.key, ..., request.value)
        return GateKV_storage_pb2.Empty() # TODO : Return some request id to follow... (advise Batu, later)
    
    def SetData(self, request, context):
        self.__storage[request.key] = request.value
        return GateKV_storage_pb2.SetResponse(success=True)
    
    def Get(self, request, context):
        value = self.__storage.get(request.key)
        if value != None:
            return GateKV_storage_pb2.GetResponse(success=True, ..., value=value)
        else: # Key-value does not exist
            return self.__client.callGetOnStorage(request.key, ...)
    
    def GetData(self, request, context):
        value = self.__storage.get(request.key)
        if value != None:
            return GateKV_storage_pb2.GetResponse(success=True, value=value)
        # response must be success because call is coming from gateway
        return GateKV_storage_pb2.GetResponse(success=False, value=value)
    
    def Rem(self, request, context):
        self.__client.callSetOnGateway(request.key, ..., request.value)
        return GateKV_storage_pb2.Empty() # TODO : Return some request id to follow... (advise Batu, later)
    
    def RemData(self, request, context):
        value = self.__storage.get(request.key)
        if value != None:
            del self.__storage[request.key]
            print(f"RemData: {request.key} removed")
            return GateKV_storage_pb2.RemResponse(success=True)
        return GateKV_storage_pb2.RemResponse(success=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(GateKV_StorageNode_Server({}), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Storage Server running on port 50052")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
