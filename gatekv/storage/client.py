import random
import grpc
import time
from typing import Dict 
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
    def callSetOnGateway(self, key, value, alias):
        # request = GateKV_gateway_pb2.SetRequest(key=key, value=value, alias=alias)
        # response = self.__gateway_stub.Set(request)
        # return response.success

        # Simulate loooooong time to wait for gateway
        time.sleep(1)

        return True

    # not important function
    def callGetOnGateway(self, key, alias):
        # request = GateKV_gateway_pb2.GetRequest(key=key)
        # response = self.__gateway_stub.Get(request)
        # return response.success, response.value
        return True

    def callRemOnGateway(self, key, alias):
        # request = GateKV_gateway_pb2.RemRequest(key=key)  # Correct message name
        # response = self.__gateway_stub.Rem(request)  # Adjusted method name
        # print(f"Rem Response: {response.success}")
        # return response.success
        time.sleep(1)
        return True

    # Storage Calls
    def callSetOnStorage(self, key, value):
        pass

    def callGetOnStorage(self, key, alias):
        """if alias not in self.__storage_stubs:
            print(f"Error: Storage node {alias} not found!")
            return False, ""
        request = GateKV_storage_pb2.GetRequest(key=key)
        response = self.__storage_stubs[alias].Get(request)
        print(f"GetData Response from {alias}: {response.success}, Value: {response.value}")"""
        # Select a random neighbour
        stubs:list[GateKV_storage_pb2_grpc.GateKV_StorageStub] = list(self.__storage_stubs.values())
        stub = random.choice(stubs)

        response = stub.Get(GateKV_storage_pb2.GetRequest(key=key))

        return response

    def callRemOnStorage(self, alias, key):
        pass

if __name__ == "__main__":
    gateway_address = "localhost:50051"
    storage_addresses = {"storage-01": "localhost:50052", "storage-02": "localhost:50053"}
    
