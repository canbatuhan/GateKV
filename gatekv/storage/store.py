import pickledb

class GateKV_StorageNode_LocalStore:
    def __init__(self, store_conf:dict):
        self.__store = pickledb.PickleDB(store_conf["path"])

    def set(self, key, value):
        return self.__store.set(key, value)

    def get(self, key):
        entry = self.__store.get(key)
        
        if entry == None:
            return None # Key-value pair does not exist
        
        else: # Key-value pair exists
            return entry # (value, version)

    def rem(self, key):
        return self.__store.remove(key)
    
    def dump(self):
        return self.__store.save()