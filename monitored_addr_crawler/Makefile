PROTOC_PATH := /work/miningpool-base/protocol_def
.PYONY:all
all:
	go build

balanceInfo.pb.go:${PROTOC_PATH}/balanceInfo.proto
	protoc --go_out=. $^ -I ${PROTOC_PATH}
