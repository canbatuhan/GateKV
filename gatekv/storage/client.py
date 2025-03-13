import grpc
import time
from typing import Dict 
from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2

class GateKV_StorageNode_Client:
    def __init__(self, gateway_address, storage_addresses: Dict[str, str]):
        # Connect to Gateway
        self.gateway_channel = grpc.insecure_channel(gateway_address)
        self.__gateway_stub = GateKV_GatewayStub(self.gateway_channel)
        
        # Connect to multiple Storage Nodes
        self.__storage_stubs = {}
        for alias, address in storage_addresses.items():
            channel = grpc.insecure_channel(address)
            self.__storage_stubs[alias] = GateKV_StorageStub(channel)

    # Gateway Calls
    def callSetOnGateway(self, key, value):
        request = GateKV_gateway_pb2.SetRequest(key=key, value=value)  # Correct message name
        response = self.__gateway_stub.Set(request)  # Adjusted method name
        print(f"Set Response: {response.success}")
        return response.success

    def callGetOnGateway(self, key):
        request = GateKV_gateway_pb2.GetRequest(key=key)  # Correct message name
        response = self.__gateway_stub.Get(request)  # Adjusted method name
        print(f"Get Response: {response.success}, Value: {response.value}")
        return response.success, response.value

    def callRemOnGateway(self, key):
        request = GateKV_gateway_pb2.RemRequest(key=key)  # Correct message name
        response = self.__gateway_stub.Rem(request)  # Adjusted method name
        print(f"Rem Response: {response.success}")
        return response.success

    # Storage Calls
    def callSetOnStorage(self, alias, key, value):
        if alias not in self.__storage_stubs:
            print(f"Error: Storage node {alias} not found!")
            return False
        request = GateKV_storage_pb2.SetRequest(key=key, value=value)
        response = self.__storage_stubs[alias].Set(request)
        print(f"SetData Response from {alias}: {response.success}")
        return response.success

    def callGetOnStorage(self, alias, key):
        if alias not in self.__storage_stubs:
            print(f"Error: Storage node {alias} not found!")
            return False, ""
        request = GateKV_storage_pb2.GetRequest(key=key)
        response = self.__storage_stubs[alias].Get(request)
        print(f"GetData Response from {alias}: {response.success}, Value: {response.value}")
        return response.success, response.value

    def callRemOnStorage(self, alias, key):
        if alias not in self.__storage_stubs:
            print(f"Error: Storage node {alias} not found!")
            return False
        request = GateKV_storage_pb2.RemRequest(key=key)
        response = self.__storage_stubs[alias].Rem(request)
        print(f"RemData Response from {alias}: {response.success}")
        return response.success

if __name__ == "__main__":
    # Example configuration
    gateway_address = "localhost:50051"
    storage_addresses = {"storage-01": "localhost:50052", "storage-02": "localhost:50053"}
    
    client = GateKV_StorageNode_Client(gateway_address, storage_addresses)
    
    # Test calls to Gateway
    client.callSetOnGateway("testKey", "testValue")
    client.callGetOnGateway("testKey")
    client.callRemOnGateway("testKey")
    
    # Test calls to Storage Nodes
    client.callSetOnStorage("storage-01", "testKey", "testValue")
    client.callGetOnStorage("storage-01", "testKey")
    client.callRemOnStorage("storage-01", "testKey")
