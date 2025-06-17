import pickledb

from gatekv.gateway.util import GateKV_GatewayNode_Logger

class GateKV_StorageNode_LocalStore:
    def __init__(self, store_conf:dict):
        self.__store = pickledb.PickleDB(store_conf["path"])
        self.__logger = GateKV_GatewayNode_Logger("LocalStore")

    def set(self, key, value):
        return self.__store.set(key, value)

    def get(self, key):
        return self.__store.get(key)

    def rem(self, key):
        return self.__store.remove(key)
    
    def dump(self):
        return self.__store.save()