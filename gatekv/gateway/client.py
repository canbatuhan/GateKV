import random
from typing import Dict

import grpc

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.util import GateKV_GatewayNode_Logger
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2


class GateKV_GatewayNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stubs:Dict[str:GateKV_GatewayStub] = dict() # stubs["gateway-01"].Set(...)
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = dict() # stubs["storage-01"].Set(...)
        self.__logger = GateKV_GatewayNode_Logger("Client")

    # Callbacks for Gateway Servers
    
    def __callGossipOnGateway(self, stub:GateKV_GatewayStub, request):
        try:
            response = stub.Gossip(request)
            return response.success
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
            return (False)

    # Callbacks for Storage Servers

    def __callSetOnStorage(self, stub:GateKV_StorageStub, key, value):
        try:
            response = stub.SetData(GateKV_storage_pb2.SetRequest(key=key, value=value))
            return response.success
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
        return (False)

    def __callGetOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        try:
            response = stub.Get(GateKV_storage_pb2.GetRequest(key = key))
            return response.success, response.value
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
        return (False, None)

    def __callRemOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        try:
            response = stub.RemData(GateKV_storage_pb2.RemRequest(key=key))
            return response.success
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
        return (False)
    
    def __callBatchSetOnStorage(self, stub:GateKV_StorageStub, batch):
        try:
            response = stub.BatchSet(batch)
            return response.success
        except Exception as e:
            self.__logger.log(e.with_traceback(None))
        return (False)

    def __callBatchRemOnStorage(self, stub:GateKV_StorageStub, batch):
        try:
            response = stub.BatchRem(batch)
            return response.success
        except Exception as e:  
            self.__logger.log(e.with_traceback(None))
        return (False) 

    # Broadcasting Methods

    def __broadcast_to_storage(self, callback, key=None, value=None):
        responses = []
        for _, stub in self.__storage_stubs.items():
            responses.append(callback(stub, key, value))
        return responses
    
    def __broadcast_batch_to_storage(self, callback, batch):
        responses = []
        for _, stub in self.__storage_stubs.items():
            responses.append(callback(stub, batch))
        return responses

    def __broadcast_to_gateway(self, callback, request):
        responses = []
        for _, stub in self.__gateway_stubs.items():
            responses.append(callback(stub, request))
        return responses
    
    # Util Methods

    def register_neighbour(self, type, alias, host, port):
        channel = grpc.insecure_channel("{}:{}".format(host, port))
        if type == "gateway":
            stub = GateKV_GatewayStub(channel)
            self.__gateway_stubs.update({alias : stub})
        elif type == "storage":
            stub = GateKV_StorageStub(channel)
            self.__storage_stubs.update({alias : stub})

    # Protocols

    def register_protocol(self, type, alias, host, port):
        for gateway in self.__config.get("gateway"):
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
                self.__gateway_stubs.update({response.alias : stub})

            except Exception as e:
                self.__logger.log(e.with_traceback(None))
        
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
                self.__logger.log(e.with_traceback(None))

    def set_protocol(self, key, value):
        responses = self.__broadcast_to_storage(self.__callSetOnStorage, key, value)
        return responses.count(True) > len(responses) // 2

    def get_protocol(self, key):
        storage_stub = random.choice(list(self.__storage_stubs.values()))
        success, value = self.__callGetOnStorage(storage_stub, key)
        return success, value
        
    def rem_protocol(self, key):
        responses = self.__broadcast_to_storage(self.__callRemOnStorage, key)
        return responses.count(True) > len(responses) // 2
    
    def batch_set_protocol(self, batch):
        responses = self.__broadcast_batch_to_storage(self.__callBatchSetOnStorage, batch)
        return responses.count(True) > len(responses) // 2

    def batch_rem_protocol(self, batch):
        responses = self.__broadcast_batch_to_storage(self.__callBatchRemOnStorage, batch)
        return responses.count(True) > len(responses) // 2
    
    def gossip_protocol(self, batch):
        responses = self.__broadcast_to_gateway(self.__callGossipOnGateway, batch)
        return responses.count(True) > len(responses) // 2