echo "---------------------------------"
echo "-- Code generator for protobuf --"
echo "---------------------------------"

RUNNER=python3
FLAGS=-m
PROGRAM=grpc_tools.protoc

echo "Creating source code for Gateway Node..."
PROTO_DIR_PATH=./gatekv/resources
PROTO_FILE_PATH=./gatekv/resources/GateKV-gateway.proto
PYTHON_OUT=./gatekv/gateway/service
GRPC_PYTHON_OUT=./gatekv/gateway/service

$RUNNER $FLAGS $PROGRAM --proto_path=$PROTO_DIR_PATH --python_out=$PYTHON_OUT --grpc_python_out=$GRPC_PYTHON_OUT $PROTO_FILE_PATH
echo "Done."

echo "Creating source code for Storage Node..."
PROTO_DIR_PATH=./gatekv/resources
PROTO_FILE_PATH=./gatekv/resources/GateKV-storage.proto
PYTHON_OUT=./gatekv/storage/service
GRPC_PYTHON_OUT=./gatekv/storage/service

$RUNNER $FLAGS $PROGRAM --proto_path=$PROTO_DIR_PATH --python_out=$PYTHON_OUT --grpc_python_out=$GRPC_PYTHON_OUT $PROTO_FILE_PATH
echo "Done."

echo "Creating source code for User..."
PROTO_DIR_PATH=./gatekv/resources
PROTO_FILE_PATH=./gatekv/resources/GateKV-user.proto
PYTHON_OUT=./gatekv/user/service
GRPC_PYTHON_OUT=./gatekv/user/service

$RUNNER $FLAGS $PROGRAM --proto_path=$PROTO_DIR_PATH --python_out=$PYTHON_OUT --grpc_python_out=$GRPC_PYTHON_OUT $PROTO_FILE_PATH
echo "Done."