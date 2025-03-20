import random
import asyncio
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

    # Callbacks for Gateway Servers

    async def __callSetOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            response = await stub.Set(GateKV_gateway_pb2.SetRequest(key = key, value = value))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Set on Gateway Neighbour...")
        return True

    async def __callGetOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            pass
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Get on Gateway Neighbour...")
        return True, -1

    async def __callRemOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            response = await stub.Rem(GateKV_gateway_pb2.RemRequest(key = key))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Remove on Gateway Neighbour...")
        return True

    # Callbacks for Storage Servers

    async def __callSetOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        """try:
            response = await stub.SetData(GateKV_storage_pb2.SetRequest(key = key, value = value))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Set on Storage Neighbour...")
        return True

    async def __callGetOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        """try:
            response = await stub.GetData(GateKV_storage_pb2.GetRequest(key = key))
            response.success, response.value
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Get on Storage Neighbour...")
        return True, 0

    async def __callRemOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        """try:
            response = await stub.RemData(GateKV_storage_pb2.RemRequest(key))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Remove on Storage Neighbour...")
        return True

    # Broadcasting Methods

    async def __broadcast_to_storage(self, storage_stubs, callback, key=None, value=None):
        tasks = [callback(stub, key, value) for stub in storage_stubs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return all(results)

    async def __broadcast_to_gateway(self, callback, key=None, value=None):
        tasks = [asyncio.create_task(callback(stub, key, value))
                 for stub in self.__gateway_stubs.values()]
        
    # Protocols

    async def set_protocol(self, owners, key, value):
        if owners == None:
            storage_stubs:dict = dict(random.sample(list(self.__storage_stubs.items()),
                                                    len(self.__storage_stubs)//2 + 1))
            owners = list(storage_stubs.keys())

        else: # Owners exist
            storage_stubs:dict = dict([(each, self.__storage_stubs.get(each))
                                       for each in owners])

        completed = await self.__broadcast_to_storage(storage_stubs.values(), self.__callSetOnStorage, key, value)
        await self.__broadcast_to_gateway(self.__callSetOnGateway, key, value)

        if not completed:
            pass # Roll-back

        return owners, completed
    
    async def get_protocol(self, owners, key):
        if owners == None:
            return False, None
        
        storage_stub:GateKV_StorageStub = self.__storage_stubs.get(random.choice(owners))
        success, value = await self.__callGetOnStorage(storage_stub, key)
        return success, value
        
    async def rem_protocol(self, owners, key):
        if owners == None:
            return None, False

        else: # Owners exist
            storage_stubs:dict = dict([(each, self.__storage_stubs.get(each))
                                       for each in owners])
            
        completed = await self.__broadcast_to_storage(storage_stubs.values(), self.__callRemOnStorage, key)
        await self.__broadcast_to_gateway(self.__callRemOnGateway, key)

        if not completed:
            pass # Roll-back

        return owners, completed