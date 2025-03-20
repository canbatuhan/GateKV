import random
from gatekv.user.service import GateKV_user_pb2
from gatekv.user.service.GateKV_user_pb2_grpc import GateKV_UserServicer

class InvalidNodeType(Exception):
    def __init__(self, *args):
        super().__init__(*args)

class GateKV_User(GateKV_UserServicer):
    def __init__(self, conf):
        self.__conf     = conf
        self.__servers  = []
        self.__response = None
        super().__init__()

    # Receiver-side

    def RecvSet(self, request, context):
        self.__response = request.success
        return GateKV_user_pb2.Empty()
    
    def RecvGet(self, request, context):
        self.__response = (request.success, request.value)
        return GateKV_user_pb2.Empty()
    
    def RecvRem(self, request, context):
        self.__response = request.success
        return GateKV_user_pb2.Empty()
    
    # Sender-side

    def __set(self, stub, request):
        _ = stub.Set(request)
        while self.__response == None:
            pass
        return self.__response
    
    def __get(self, stub, request):
        _ = stub.Get(request)
        while self.__response == None:
            pass
        return self.__response
    
    def __rem(self, stub, request):
        _ = stub.Rem(request)
        while self.__response == None:
            pass
        return self.__response
    
    # Benchmark wrappers

    def set_exec_time(self, key, value):
        stub, type = random.choice(self.__servers)
        if type == "gateway":
            request = ...
        elif type == "storage":
            request = ...
        else: # Invalid type
            raise InvalidNodeType()

    def get_exec_time(self, key):
        pass

    def rem_exec_time(self, key):
        pass

    # User-space wrappers 
    
    def set(self, key, value):
        pass

    def get(self, key):
        pass

    def rem(self, key):
        pass

