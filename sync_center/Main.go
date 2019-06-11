package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"mempool.com/foundation/bitdb/common"
)

type GlobalHandle struct {
	comparatorManager *ComparatorManager
	synchronizer      *Synchronizer
}

var gHandle GlobalHandle

func (this *GlobalHandle) Init() {
	this.comparatorManager = new(ComparatorManager)
	this.comparatorManager.Init()
	this.synchronizer = new(Synchronizer)
	this.synchronizer.Init()
}

func main() {
	//1.读取配置文件
	configFilePath := flag.String("config", "./config.json", "Path of config file")
	flag.Parse()
	configJSON, err := ioutil.ReadFile(*configFilePath)
	configData := new(ConfigData)
	err = json.Unmarshal(configJSON, configData)
	if err != nil {
		glog.Fatal("parse config failed: ", err)
		return
	}
	//2.初始化mongodb，主要用来读取sync表，里面储存了ownerid
	gHandle.Init()
	gHandle.synchronizer.AddNodes(configData.Nodes)
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, mongoConfig := range configData.Mongo {
		pMongoManager.AddServiceHub(
			coinType,
			mongoConfig.AddrService,
			mongoConfig.BlockHeaderService,
			mongoConfig.AddrIndexService,
			mongoConfig.HeightAddrService,
			mongoConfig.HeightHashService,
			mongoConfig.MonitoredService,
			mongoConfig.SyncMonitoredAddrService)
		gHandle.comparatorManager.AddComparator(
			coinType,
			configData.Nodes,
			pMongoManager.Servicehub[coinType])
		go gHandle.comparatorManager.Comparators[coinType].ComparaLoop(pMongoManager.Servicehub[coinType])
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/monitored_address/add/type/{type}/appid/{appid}/ownerid/{ownerid}/addr/{addr}", common.Logging(NotifyMonitoredAddr))
	glog.Fatal(http.ListenAndServe(configData.Listen, r))
}
