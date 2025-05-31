import threading
import grpc

from queue import Queue
from typing import Dict, List 

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.util import GateKV_GatewayNode_Logger

from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2

class GateKV_StorageNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stub:GateKV_GatewayStub = None
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = dict()

        self.__logger = GateKV_GatewayNode_Logger("Client")

    def register_neighbour(self, type, alias, host, port):
        self.__logger.log("Registering new neighbour...")

        channel = grpc.insecure_channel("{}:{}".format(host, port))
        if type == "gateway":
            stub = GateKV_GatewayStub(channel)
            self.__gateway_stub = stub
        elif type == "storage":
            stub = GateKV_StorageStub(channel)
            self.__storage_stubs.update({alias : stub})

    def register_protocol(self, type, alias, host, port):
        self.__logger.log("Registering new protocol...")

        gateway = self.__config.get("gateway")
        try:
            stub = GateKV_GatewayStub(grpc.insecure_channel("{}:{}".format(
                gateway.get("host"), gateway.get("port"))))
            request = GateKV_gateway_pb2.RegisterRequest(
                type = type,
                alias = alias,
                sender = GateKV_gateway_pb2.Address(
                    host = host,
                    port = port))
            response = stub.Register(request)
            self.__gateway_stub = stub

        except Exception as e:
            print(e.with_traceback(None))
        
        for storage in self.__config.get("storage"):
            try:
                stub = GateKV_StorageStub(grpc.insecure_channel("{}:{}".format(
                    storage.get("host"), storage.get("port"))))
                request = GateKV_storage_pb2.RegisterRequest(
                    type = type,
                    alias = alias,
                    sender = GateKV_storage_pb2.Address(
                        host = host,
                        port = port))
                response = stub.Register(request)
                self.__storage_stubs.update({response.alias : stub})

            except Exception as e:
                print(e.with_traceback(None))
            
    def callSetOnGateway(self, key, value):
        self.__logger.log("Setting new key-value pair on gateway...")
        request = GateKV_gateway_pb2.SetRequest(key=key, value=value)
        response = self.__gateway_stub.Set(request)
        return response.success

    def callRemOnGateway(self, key):
        self.__logger.log("Removing key-value pair on gateway...")  
        request = GateKV_gateway_pb2.RemRequest(key=key)
        response = self.__gateway_stub.Rem(request) 
        return response.success

    def callGetOnStorage(self, key, visited_nodes):
        self.__logger.log("Getting value for key on storage...")
        
        visited_nodes = set(visited_nodes)
        result = {'response': None}
        lock = threading.Lock()
        stop_event = threading.Event()
        q = Queue()

        for alias, stub in self.__storage_stubs.items():
            if alias not in visited_nodes:
                q.put((alias, stub))

        def worker():
            while not q.empty() and not stop_event.is_set():
                alias, stub = q.get()
                with lock:
                    if alias in visited_nodes:
                        return
                    visited_nodes.add(alias)
                new_visited = set(visited_nodes)

                print(f"visiting: {alias}, current visited: {new_visited}")

                request = GateKV_storage_pb2.GetRequest(
                    key=key,
                    visitedNodes=list(new_visited)
                )
                try:
                    response = stub.Get(request)
                    if response.success:
                        with lock:
                            if result['response'] is None:
                                result['response'] = response
                                stop_event.set()
                except Exception as e:
                    print(f"[Client] Error querying node '{alias}': {e}")

        threads: List[threading.Thread] = []
        for _ in range(min(5, q.qsize())):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=2)

        if result['response']:
            return result['response']

        return GateKV_storage_pb2.GetResponse(success=False, value="", visitedNodes=list(visited_nodes))