from queue import Queue
import random
import threading
import grpc
import time
from typing import Dict, List 
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2
from gatekv.storage.service import GateKV_storage_pb2_grpc

class GateKV_StorageNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stub:GateKV_GatewayStub = None # stubs["gateway-01"].Set(...)
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = dict() # stubs["storage-01"].Set(...)
        
    # Util Methods

    def register_neighbour(self, type, alias, host, port):
        channel = grpc.insecure_channel("{}:{}".format(host, port))
        if type == "gateway":
            stub = GateKV_GatewayStub(channel)
            self.__gateway_stub = stub
        elif type == "storage":
            stub = GateKV_StorageStub(channel)
            self.__storage_stubs.update({alias : stub})

    def register_protocol(self, type, alias, host, port):
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
                    type = "gateway",
                    alias = alias,
                    sender = GateKV_storage_pb2.Address(
                        host = host,
                        port = port))
                response = stub.Register(request)
                self.__storage_stubs.update({response.alias : stub})

            except Exception as e:
                print(e.with_traceback(None))
            
    # Gateway Calls
    def callSetOnGateway(self, key, value):
        request = GateKV_gateway_pb2.SetRequest(key=key, value=value)
        response = self.__gateway_stub.Set(request)
        return response.success

    # not important function
    def callGetOnGateway(self, key):
        # request = GateKV_gateway_pb2.GetRequest(key=key)
        # response = self.__gateway_stub.Get(request)
        # return response.success, response.value
        return True

    def callRemOnGateway(self, key):
        request = GateKV_gateway_pb2.RemRequest(key=key)  # Correct message name
        response = self.__gateway_stub.Rem(request)  # Adjusted method name

        return response.success

    # Storage Calls
    def callSetOnStorage(self, key, value):
        pass

    def callGetOnStorage(self, key, visited_nodes):
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

    def callRemOnStorage(self, key):
        pass