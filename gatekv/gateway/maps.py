from typing import Dict
from gatekv.gateway.statemachine import GateKV_GatewayNode_ReplicatedStateMachine

class GateKV_GatewayNode_PairOwnerMap:
    def __init__(self):
        self.__dict = dict()

    def addPairOwners(self, key, owners):
        self.__dict.update({key : owners})

    def getPairOwners(self, key):
        return self.__dict.get(key)

    def removePairOwners(self, key):
        self.__dict.pop(key)

class GateKV_GatewayNode_StateMachineMap:
    def __init__(self):
        self.__dict:Dict[str, GateKV_GatewayNode_ReplicatedStateMachine] = dict()
    
    def getStateMachine(self, key):
        machine = self.__dict.get(key)
        if machine == None:
            machine = GateKV_GatewayNode_ReplicatedStateMachine()
            self.__dict.update({key : machine})
        return machine

    def removeStateMachine(self, key):
        self.__dict.pop(key)