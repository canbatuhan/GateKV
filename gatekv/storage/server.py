from concurrent.futures import ThreadPoolExecutor
import threading
import time
import grpc

from gatekv.gateway.util import GateKV_GatewayNode_Logger
from gatekv.storage.client import GateKV_StorageNode_Client
from gatekv.storage.service import GateKV_storage_pb2, GateKV_storage_pb2_grpc
from gatekv.gateway.service import GateKV_gateway_pb2, GateKV_gateway_pb2_grpc
from gatekv.storage.store import GateKV_StorageNode_LocalStore

class GateKV_StorageNode_Server(GateKV_storage_pb2_grpc.GateKV_StorageServicer):
    def __init__(self, server_conf:dict, client_conf:dict, store_conf:dict):
        super().__init__()
        self.__config = server_conf
        self.node_alias = server_conf["alias"]
        self.__server = grpc.server(thread_pool=ThreadPoolExecutor())
        GateKV_storage_pb2_grpc.add_GateKV_StorageServicer_to_server(self, self.__server)
        self.__server.add_insecure_port("0.0.0.0:{}".format(self.__config.get("port")))
        
        self.__dump_event = threading.Event()
        self.__dump_period = store_conf["dump"]

        self.__client = GateKV_StorageNode_Client(client_conf)
        self.__storage = GateKV_StorageNode_LocalStore(store_conf)

        self.__logger = GateKV_GatewayNode_Logger("Server")

    def Register(self, request, context):

        try:
            self.__client.register_neighbour(request.type,
                                             request.alias,
                                             request.sender.host,
                                             request.sender.port)
        except Exception as e:
            print(e.with_traceback(None))

        return GateKV_storage_pb2.RegisterResponse(alias = self.__config.get("alias"))
    
    def Set(self, request, context):
        try:
            gateway_response = self.__client.callSetOnGateway(request.key, request.value)
            return GateKV_storage_pb2.SetResponse(success=gateway_response)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.SetResponse(success=False)
    
    def SetData(self, request, context):
        try:
            success = self.__storage.set(request.key, request.value)
            return GateKV_storage_pb2.SetResponse(success=success)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.SetResponse(success=False)
    
    def Get(self, request, context):
        try:
            visited_nodes = set(request.visitedNodes)
            visited_nodes.add(self.node_alias)

            value = self.__storage.get(request.key)
            if value is not None:
                print(f"Node '{self.node_alias}' found key '{request.key}' locally.")
                return GateKV_storage_pb2.GetResponse(success=True, value=value, visitedNodes=list(visited_nodes))

            print(f"Node '{self.node_alias}' didn't find key '{request.key}', querying other nodes...")
            response = self.__client.callGetOnStorage(request.key, visited_nodes)

            return GateKV_storage_pb2.GetResponse(
                success=response.success,
                value=response.value,
                visitedNodes=response.visitedNodes)
        
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.GetResponse(
                success=False,
                value=None,
                visitedNodes=response.visitedNodes)
    
    def Rem(self, request, context):
        try:
            gateway_response = self.__client.callRemOnGateway(request.key)
            return GateKV_storage_pb2.RemResponse(success=gateway_response)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.RemResponse(success=False)
    
    def RemData(self, request, context):
        try:
            self.__storage.rem(request.key)
            return GateKV_storage_pb2.RemResponse(success=True)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.RemResponse(success=False)
    
    def BatchSet(self, request, context):
        try:
            for item in request.pairs:
                self.__storage.set(item.key, item.value)
            return GateKV_storage_pb2.BatchSetResponse(success=True)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.BatchSetResponse(success=False)
    
    def BatchRem(self, request, context):
        try:
            for item in request.pairs:
                self.__storage.rem(item.key)
            return GateKV_storage_pb2.BatchRemResponse(success=True)
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return GateKV_storage_pb2.BatchRemResponse(success=False)
        
    def __start_server(self):
        self.__server.start()

    def __register(self):
        self.__client.register_protocol("storage",
                                        self.__config.get("alias"),
                                        self.__config.get("host"),
                                        self.__config.get("port"))
        
    def __start_dump(self):
        def __loop():
            while not self.__dump_event.is_set():
                try:
                    self.__storage.dump()
                    time.sleep(self.__dump_period)

                except Exception as e:
                    self.__logger.log(e.with_traceback(None))

        threading.Thread(target = __loop, daemon = True).start()

    def __infinite_loop(self):
        self.__server.wait_for_termination()
        self.__dump_event.set()

    def start(self):
        self.__start_server()
        self.__register()
        self.__start_dump()
        self.__infinite_loop()
