import copy
from typing import Dict
from gatekv.gateway.statemachine import GateKV_GatewayNode_ReplicatedStateMachine

class GateKV_GatewayNode_StateMachineMap:
    def __init__(self, state_machine_conf):
        self.__dict:Dict[str, GateKV_GatewayNode_ReplicatedStateMachine] = dict()
        self.__config = state_machine_conf
        self.__sample_machine = GateKV_GatewayNode_ReplicatedStateMachine(self.__config)

    def getStateMachine(self, key):
        machine = self.__dict.get(key)
        if machine == None:
            machine = copy.deepcopy(self.__sample_machine)
            self.__dict.update({key : machine})
        return self.__dict.get(key)

    def removeStateMachine(self, key):
        try: self.__dict.pop(key)
        except: pass

class GateKV_GatewayNode_PairVersionMap:
    def __init__(self):
        self.__dict:Dict[str, int] = dict()

    def __addPairVersion(self, key):
        self.__dict.update({key : 1})
    
    def __setPairVersionTo(self, key, version):
        self.__dict.update({key : version})
    
    def __incrementPairVersion(self, key):
        version = self.__dict.get(key)
        self.__dict.update({key : version+1})

    def getPairVersion(self, key):
        return self.__dict.get(key)
    
    def setPairVersion(self, key, version=None):
        pair = self.__dict.get(key)
        if version != None:
            self.__setPairVersionTo(key, version)
        elif pair != None:
            self.__incrementPairVersion(key)
        else: # Pair does not exist
            self.__addPairVersion(key)

    def removePairVersion(self, key):
        try: self.__dict.pop(key)
        except: pass

class GateKV_GatewayNode_Logger:
    def __init__(self, component):
        self.__component = component

    def log(self, message):
        print("[Logger-{}] : {}".format(self.__component, message))