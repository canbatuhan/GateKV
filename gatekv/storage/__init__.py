from gatekv.storage.server import GateKV_StorageNode_Server

class GateKV_StorageNode_Runner:
    def __init__(self, node_config:dict):
        self.__server = GateKV_StorageNode_Server(node_config.get("server"),
                                                  node_config.get("client"),
                                                  node_config.get("store"))

    def run(self):
        self.__server.start()