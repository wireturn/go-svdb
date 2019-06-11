package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"time"

	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
	go_util "mempool.com/foundation/go-util"
)

var bitdbIp string

func main() {
	configFilePath := flag.String("config", "./config.json", "Path of config file")
	flag.Parse()

	configJSON, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		glog.Fatal("read config failed: ", err)
		return
	}

	configData := new(ConfigData)
	err = json.Unmarshal(configJSON, configData)
	if err != nil {
		glog.Fatal("parse config failed: ", err)
		return
	}
	common.GetStatisticInstance().Init(configData.EnableStatistics, configData.StatisticsFlushInterval)
	go_util.Init(configData.IsTestEnvironment)
	bitdbIp, err = common.GetIntranetIp()
	if err != nil {
		panic(err)
	}
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	pBlockManager := &BlockManager{}
	for coinType, fullnode := range configData.Fullnode {
		clients, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}
		mongo, ok := configData.Mongo[coinType]
		if !ok {
			glog.Fatal("parse mongo config failed: ", coinType)
			return
		}
		pMongoManager.AddServiceHub(
			coinType,
			mongo.AddrService,
			mongo.BlockHeaderService,
			mongo.AddrIndexService,
			mongo.HeightAddrService,
			mongo.HeightHashService,
			mongo.MonitoredService,
			mongo.SyncMonitoredAddrService)

		pBlockManager.init(configData.CrawlerInfos[coinType], clients, pMongoManager.Servicehub[coinType])
	}

	for {
		time.Sleep(time.Duration(5) * time.Second)
	}
}
