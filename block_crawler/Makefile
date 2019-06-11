PROTOC_PATH := /work/miningpool-base/protocol_def
.PYONY:all
all:
	go build

orphanInfo.pb.go:${PROTOC_PATH}/orphanInfo.proto
	protoc --go_out=. $^ -I ${PROTOC_PATH}
