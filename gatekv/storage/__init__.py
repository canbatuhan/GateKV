import multiprocessing

class GateKV_StorageNode_Runner(multiprocessing.Process):
    def __init__(self, group = None, target = None, name = None, args = ..., kwargs = ..., *, daemon = None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)

    