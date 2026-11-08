#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
python3 -m grpc_tools.protoc -I proto --python_out=dfsha/pb --grpc_python_out=dfsha/pb proto/dfsha.proto
sed -i 's/^import dfsha_pb2 as/from . import dfsha_pb2 as/' dfsha/pb/dfsha_pb2_grpc.py
echo "proto OK"
