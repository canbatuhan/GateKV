from typing import Dict
from gatekv.gateway.statemachine import GateKV_GatewayNode_ReplicatedStateMachine

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

class GateKV_GatewayNode_PairVersionMap:
    def __init__(self):
        self.__dict:Dict[str, int] = dict()

    def __addPairVersion(self, key):
        self.__dict.update({key : 1})
    
    def __incrementPairVersion(self, key):
        version = self.__dict.get(key)
        self.__dict.update({key : version+1})

    def getPairVersion(self, key):
        return self.__dict.get(key)
    
    def setPairVersion(self, key):
        pair = self.__dict.get(key)
        if pair != None:
            self.__incrementPairVersion(key)
        else: # Pair does not exist
            self.__addPairVersion(key)
        