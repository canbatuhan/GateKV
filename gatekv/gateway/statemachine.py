import json
import smpai

STATEMACHINE_CONFIG = json.load(open("./gatekv/resources/statemachine.json"))

class GateKV_GatewayNode_ReplicatedStateMachine:
    def __init__(self, state_machine_conf:dict):
        self.__machine = smpai.fsm.FiniteStateMachine(STATEMACHINE_CONFIG)