class GateKV_GatewayNode_PairInProcessMap:
    def __init__(self):
        self.__set     = set()
        self.__size     = 0
        self.__capacity = ...

    def addProcessingPair(self, key):
        self.__set.add(key)
        self.__size += 1

    def removeProcessingPair(self, key):
        self.__set.remove(key)
        self.__size -= 1

class GateKV_GatewayNode_KeyToNodeMap:
    def __init__(self):
        self.__dict = dict()

    def addKeyOwners(self, key, owners):
        self.__dict.update({key : owners})

    def getKeyOwners(self, key):
        return self.__dict.get(key)

    def removeKeyOwners(self, key):
        self.__dict.pop(key)
