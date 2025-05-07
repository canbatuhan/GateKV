import grpc
from concurrent import futures
from gatekv.storage.client import GateKV_StorageNode_Client
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc
from gatekv.storage.store import GateKV_StorageNode_LocalStore
import yaml
import argparse

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, client_conf:dict, store_config:dict, node_alias: str):
        super().__init__()
        self.node_alias = node_alias 
        print(f"[INIT] Node alias is: {self.node_alias}") 
        self.__client  = GateKV_StorageNode_Client(f"{client_conf['gateway']['host']}:{client_conf['gateway']['port']}",
                                                   {item['alias']: f"{item['host']}:{item['port']}" for item in client_conf["storageNodes"]})
        self.__storage = GateKV_StorageNode_LocalStore(store_config)


    def Register(self, request):
        return GateKV_storage_pb2.RegisterResponse(alias=request.alias)
    
    def Set(self, request, context):
        gateway_response = self.__client.callSetOnGateway(request.key, request.value)
        return GateKV_storage_pb2.SetResponse(success=gateway_response)
    
    def SetData(self, request, context):
        self.__storage.set(request.key, request.value)
        return GateKV_storage_pb2.SetResponse(success=True)
    
    def Get(self, request, context):
        visited_nodes = set(request.visitedNodes)
        visited_nodes.add(self.node_alias)

        value = self.__storage.get(request.key)
        if value is not None:
            print(f"Node '{self.node_alias}' found key '{request.key}' locally.")
            return GateKV_storage_pb2.GetResponse(success=True, value=value[0], visitedNodes=list(visited_nodes))

        print(f"Node '{self.node_alias}' didn't find key '{request.key}', querying other nodes...")
        response = self.__client.callGetOnStorage(request.key, visited_nodes)

        return GateKV_storage_pb2.GetResponse(
            success=response.success,
            value=response.value,
            visitedNodes=response.visitedNodes
        )

    def GetData(self, request, context):
        value = self.__storage.get(request.key)
        if value == None:
            return GateKV_storage_pb2.GetResponse(success=False, value=None)
        return GateKV_storage_pb2.GetResponse(success=True, value=value[0])
    
    def Rem(self, request, context):
        gateway_response = self.__client.callRemOnGateway(request.key)
        return GateKV_storage_pb2.RemResponse(success=gateway_response)
    
    def RemData(self, request, context):
        value = self.__storage.get(request.key)
        if value == None:
            return GateKV_storage_pb2.RemResponse(success=False)
        self.__storage.rem(request.key)
        return GateKV_storage_pb2.RemResponse(success=True)

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", default="docs/storage-00.yaml", type=str)
    parser.add_argument("-alias", required=True, type=str)

    args = vars(parser.parse_args())
    CONFIG_FILE = yaml.safe_load(open(args["config"], "r"))
    SERVER_CONFIG = CONFIG_FILE["server"]
    CLIENT_CONFIG = CONFIG_FILE["client"]
    STORE_CONFIG = CONFIG_FILE["store"]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=int(SERVER_CONFIG["maxWorkers"])))
    GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(GateKV_StorageNode_Server(CLIENT_CONFIG, STORE_CONFIG, args["alias"]), server)
    server.add_insecure_port(f'[::]:{SERVER_CONFIG["port"]}')
    server.start()
    print(f"Storage Server running on port {SERVER_CONFIG['port']}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
