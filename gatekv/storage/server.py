import grpc
from concurrent import futures
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, server_conf: dict):
        super().__init__()
        self.__config = server_conf
        self.__storage = {}

    def Register(self, request, context):
        # maybe print out smth "node registered: {request.alias} ({request.type})"
        return GateKV_storage_pb2.RegisterResponse(alias=request.alias)
    
    def Set(self, request, context):
        response = self.__gateway_stub.Set(
            GateKV_gateway_pb2.SetRequest(key=request.key, value=request.value)
        )
        return GateKV_storage_pb2.Empty()
    
    def SetData(self, request, context):
        self.__storage[request.key] = request.value
        return GateKV_storage_pb2.SetResponse(success=True)
    
    def Get(self, request, context):
        value = self.__storage.get(request.key, "")
        success = request.key in self.__storage
        # if not in self.__storage then it looks to its neighbors
        # implement that
        return GateKV_storage_pb2.GetResponse(success=success, value=value)
    
    def GetData(self, request, context):
        value = self.__storage.get(request.key, "")
        success = request.key in self.__storage
        # response must be success because call is coming from gateway
        return GateKV_storage_pb2.GetResponse(success=success, value=value)
    
    def Rem(self, request, context):
        response = self.__gateway_stub.Rem(
            GateKV_gateway_pb2.RemRequest(key=request.key)
        )
        return GateKV_storage_pb2.Empty()
    
    def RemData(self, request, context):
        success = request.key in self.__storage
        if success:
            del self.__storage[request.key]
            print(f"RemData: {request.key} removed")
        return GateKV_storage_pb2.RemResponse(success=success)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(GateKV_StorageNode_Server({}), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Storage Server running on port 50052")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
