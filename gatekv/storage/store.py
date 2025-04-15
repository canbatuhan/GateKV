import pickledb

class GateKV_StorageNode_LocalStore:
    def __init__(self, conf:dict):
        self.__store = pickledb.PickleDB(conf["path"])

    def set(self, key, value):
        entry = self.__store.get(key) # Entry is (value, version)

        if entry == None: # First time setting
            self.__store.set(key, (value, 0))

        else: # Writing on existing 
            _, version = entry
            self.__store.set(key, (value, version+1))

        return True

    def get(self, key):
        entry = self.__store.get(key)
        
        if entry == None:
            return None # Key-value pair does not exist
        
        else: # Key-value pair exists
            return entry # (value, version)

    def rem(self, key):
        return self.__store.remove(key)