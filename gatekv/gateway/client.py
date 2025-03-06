import random
from threading import Thread
from typing import Dict, List

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2


class GateKV_GatewayNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stubs:Dict[str:GateKV_GatewayStub] = None # stubs["gateway-01"].Set(...)
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = None # stubs["storage-01"].Set(...)

    def __callSetOnGateway(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __callGetOnGateway(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __callRemOnGateway(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __callSetOnStorage(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __callGetOnStorage(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __callRemOnStorage(self):
        try:
            pass
        except Exception as e:
            print(e.with_traceback(None))

    def __broadcast(self, stub, callback, response_pool, idx):
        pass

    def __broadcast_to_storage(self, owners:dict, callback):
        response_pool = [False for _ in owners]
        threads:List[Thread] = list()
        for idx, (_, stub) in enumerate(owners.items()):
            threads.append(Thread(target = self.__broadcast,
                                  args = (stub, callback, response_pool, idx)))
        for each in threads: each.start()
        for each in threads: each.join()
        return all(response_pool)

    def __broadcast_to_gateway(self, callback):
        threads:List[Thread] = list()
        for idx, (_, stub) in enumerate(self.__gateway_stubs.items()):
            threads.append(Thread(target = self.__broadcast,
                                  args = (stub, callback, [], -1)))
        for each in threads: each.start()

    def set_protocol(self, owners):
        if owners == None:
            storage_stubs:dict = random.sample(self.__storage_stubs,
                                          len(self.__storage_stubs)//2 + 1)

        else: # Owners exist
            storage_stubs:dict = [self.__storage_stubs.get(each)
                                  for each in owners]

        # Broadcasting to storage nodes
        need_to_rollback = self.__broadcast_to_storage(storage_stubs, self.__callSetOnStorage)

        # Broadcasting to gateway nodes
        self.__broadcast_to_gateway(self.__callSetOnGateway)

        # Rollback if necessary
        if need_to_rollback:
            # Rollback protocol
            pass

        return not need_to_rollback
    
    def get_protocol(self, owners):
        pass

    def rem_protocol(self, owners):
        pass