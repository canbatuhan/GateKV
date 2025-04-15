import grpc
from concurrent import futures
from gatekv.storage.client import GateKV_StorageNode_Client
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc
from gatekv.storage.store import GateKV_StorageNode_LocalStore
import yaml
import argparse

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, client_conf:dict, store_config:dict):
        super().__init__()
        self.__client  = GateKV_StorageNode_Client(f"{client_conf['gateway']['host']}:{client_conf['gateway']['port']}",
                                                   {item['alias']: f"{item['host']}:{item['port']}" for item in client_conf["storageNodes"]})
        self.__storage = GateKV_StorageNode_LocalStore(store_config)

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
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", default="docs/storage-00.yaml", type=str)

    args = vars(parser.parse_args())
    CONFIG_FILE = yaml.safe_load(open(args["config"], "r"))
    SERVER_CONFIG = CONFIG_FILE["server"]
    CLIENT_CONFIG = CONFIG_FILE["client"]
    STORE_CONFIG = CONFIG_FILE["store"]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=int(SERVER_CONFIG["maxWorkers"])))
    GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(GateKV_StorageNode_Server(CLIENT_CONFIG, STORE_CONFIG), server)
    server.add_insecure_port(f'[::]:{SERVER_CONFIG["port"]}')
    server.start()
    print(f"Storage Server running on port {SERVER_CONFIG['port']}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
