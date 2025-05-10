import random
from threading import Thread
from typing import Dict, List

from gatekv.gateway.service.GateKV_gateway_pb2_grpc import GateKV_GatewayStub
from gatekv.gateway.service import GateKV_gateway_pb2
from gatekv.gateway.util import GateKV_GatewayNode_GossipMap
from gatekv.storage.service.GateKV_storage_pb2_grpc import GateKV_StorageStub
from gatekv.storage.service import GateKV_storage_pb2


class GateKV_GatewayNode_Client:
    def __init__(self, client_conf:dict):
        self.__config = client_conf
        self.__gateway_stubs:Dict[str:GateKV_GatewayStub] = None # stubs["gateway-01"].Set(...)
        self.__storage_stubs:Dict[str:GateKV_StorageStub] = None # stubs["storage-01"].Set(...)

    # Callbacks for Gateway Servers

    def __callSetOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            response = stub.Set(GateKV_gateway_pb2.SetRequest(key = key, value = value))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Set on Gateway Neighbour...")
        return (True)

    def __callGetOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            pass
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Get on Gateway Neighbour...")
        return (True, -1)

    def __callRemOnGateway(self, stub:GateKV_GatewayStub, key, value=None):
        """try:
            response = stub.Rem(GateKV_gateway_pb2.RemRequest(key = key))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Remove on Gateway Neighbour...")
        return (True)

    # Callbacks for Storage Servers

    def __callSetOnStorage(self, stub:GateKV_StorageStub, request):
        """try:
            response = stub.SetData(request)
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Set on Storage Neighbour...")
        return (True)

    def __callGetOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        """try:
            response = stub.GetData(GateKV_storage_pb2.GetRequest(key = key))
            response.success, response.value
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Get on Storage Neighbour...")
        return (True, 0)

    def __callRemOnStorage(self, stub:GateKV_StorageStub, key, value=None):
        """try:
            response = stub.RemData(GateKV_storage_pb2.RemRequest(key))
            return response.success
        except Exception as e:
            print(e.with_traceback(None))"""
        print("Calling Remove on Storage Neighbour...")
        return (True)
    
    def __callBatchSetOnStorage(self, stub:GateKV_StorageStub, batch):
        pass

    def __callBatchRemOnStorage(self, stub:GateKV_StorageStub, batch):
        pass

    # Broadcasting Methods

    def __broadcast_to_storage(self, callback:function, key=None, value=None):
        responses = []
        for stub in self.__storage_stubs:
            responses.append(callback(stub, key, value))
        return responses

    def __broadcast_to_gateway(self, callback:function, key=None, value=None):
        responses = []
        for stub in self.__gateway_stubs.values():
            responses.append(callback(stub, key, value))
        return responses

    # Protocols

    def set_protocol(self, key, value):
        request = GateKV_storage_pb2.SetRequest(key = key, value = value)
        responses = self.__broadcast_to_storage(self.__callSetOnStorage, request)
        # Count successes
        return True

    def get_protocol(self, owners, key):
        if owners == None: # Owners does not exist
            return False, None
        
        storage_stub = random.choice(self.__storage_stubs.values())
        success, value = self.__callGetOnStorage(storage_stub, key)
        return success, value
        
    def rem_protocol(self, key):
        success = self.__broadcast_to_storage(self.__callRemOnStorage, key)
        # Count successes
        return True
    
    def batch_set_protocol(self, batch):
        responses = self.__broadcast_to_storage(self.__callBatchSetOnStorage, batch)
        # Count successes
        return True

    def batch_rem_protocol(self, batch):
        responses = self.__broadcast_to_storage(self.__callBatchRemOnStorage, batch)
        # Count successes
        return True
    
    def gossip_protocol(self):
        pass