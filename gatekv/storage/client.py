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
    def __init__(self, gateway_address, storage_addresses: Dict[str, str]):
        # Connect to Gateway
        # self.gateway_channel = grpc.insecure_channel(gateway_address)
        # self.__gateway_stub = GateKV_GatewayStub(self.gateway_channel)
        
        # Connect to multiple Storage Nodes
        self.__storage_stubs = {}
        for alias, address in storage_addresses.items():
            channel = grpc.insecure_channel(address)
            self.__storage_stubs[alias] = GateKV_storage_pb2_grpc.GateKV_StorageStub(channel)
            
    # Gateway Calls
    def callSetOnGateway(self, key, value):
        # request = GateKV_gateway_pb2.SetRequest(key=key, value=value, alias=alias)
        # response = self.__gateway_stub.Set(request)
        # return response.success

        # Simulate loooooong time to wait for gateway
        time.sleep(1)

        return True

    # not important function
    def callGetOnGateway(self, key):
        # request = GateKV_gateway_pb2.GetRequest(key=key)
        # response = self.__gateway_stub.Get(request)
        # return response.success, response.value
        return True

    def callRemOnGateway(self, key):
        # request = GateKV_gateway_pb2.RemRequest(key=key)  # Correct message name
        # response = self.__gateway_stub.Rem(request)  # Adjusted method name
        # print(f"Rem Response: {response.success}")
        # return response.success
        time.sleep(1)
        return True

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

if __name__ == "__main__":
    gateway_address = "localhost:50051"
    storage_addresses = {"storage-01": "localhost:50052", "storage-02": "localhost:50053"}
    
