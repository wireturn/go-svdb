package main

import (
	"net/http"

	"github.com/gorilla/mux"
	"mempool.com/foundation/bitdb/common"
)

const (
	CONNECT_NODE_ErrorCode = -280
)

func NotifyMonitoredAddr(rsp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	addressType := vars["type"]
	appid := vars["appid"]
	owneridstr := vars["ownerid"]
	addr := vars["addr"]
	go gHandle.synchronizer.Notify(addressType, appid, owneridstr, addr)
	rsp.Write(common.MakeOkRspByData("ok"))
}
