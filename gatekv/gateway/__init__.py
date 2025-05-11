from gatekv.gateway.server import GateKV_GatewayNode_Server

class GateKV_GatewayNode_Runner:
    def __init__(self, node_config:dict):
        self.__server = GateKV_GatewayNode_Server(node_config.get("server"),
                                                  node_config.get("client"),
                                                  node_config.get("statemachine"))

    def run(self):
        self.__server.start()

    