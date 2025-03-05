from typing import Dict

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2


class GateKV_GatewayNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stubs:Dict[str:GateKV_GatewayStub] = None # stubs["gateway-01"].Set(...)
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = None # stubs["storage-01"].Set(...)

    def callSetOnGateway(self):
        pass

    def callGetOnGateway(self):
        pass

    def callRemOnGateway(self):
        pass

    def callSetOnStorage(self):
        pass

    def callGetOnStorage(self):
        pass

    def callRemOnStorage(self):
        pass