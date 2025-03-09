from enum import Enum
import json
import time
import smpai

STATEMACHINE_CONFIG = json.load(open("./gatekv/resources/statemachine.json"))

class GateKV_GatewayNode_Events(Enum):
    READ  = "READ"
    WRITE = "WRITE"
    DONE  = "DONE"
    EMPTY = "EMPTY"

class GateKV_GatewayNode_States(Enum):
    IDLE = "IDLE"
    READING = "READING"
    WRITING = "WRITING"

class GateKV_GatewayNode_TimeoutException(Exception):
    pass

class GateKV_GatewayNode_ReplicatedStateMachine:
    def __init__(self, state_machine_conf:dict):
        self.__config = state_machine_conf
        self.__machine = smpai.fsm.FiniteStateMachine(STATEMACHINE_CONFIG)
        self.__stack = list()

    def send_event(self, event:GateKV_GatewayNode_Events):
        timeout = self.__config.get("min_timeout")
        while not self.__machine.check_event(event.name):
            if timeout >= self.__config.get("max_timeout"):
                raise GateKV_GatewayNode_TimeoutException()
            time.sleep(timeout)
            timeout *= 2

        # Trigger the machine
        self.__machine.send_event(event.name)

        # Check for multi-read case
        current_state = self.__machine.get_context().get_current_state().get_id()
        if current_state == GateKV_GatewayNode_States.READING.name:
            if event == GateKV_GatewayNode_Events.READ:
                self.__stack.append("x") # Add another reading flag
            elif event == GateKV_GatewayNode_Events.DONE:
                self.__stack.pop() # Pop one of the reading flags
                if len(self.__stack) != 0:
                    return # There are some read requests left