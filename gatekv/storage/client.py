from typing import Dict

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2


class GateKV_StorageNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stub:GateKV_GatewayStub = None
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = None # stubs["storage-01"].Set(...)

    def callSetOnGateway(self):
        # TODO #3 : Implement 
        pass

    def callGetOnGateway(self):
        # TODO #3 : Implement 
        pass

    def callRemOnGateway(self):
        # TODO #3 : Implement 
        pass

    def callSetOnStorage(self):
        # TODO #3 : Implement 
        pass

    def callGetOnStorage(self):
        # TODO #3 : Implement 
        pass

    def callRemOnStorage(self):
        # TODO #3 : Implement 
        pass