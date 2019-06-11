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

var glbBitdbIp string

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

	glog.Info("configData.EnableStatistics ", configData.EnableStatistics)
	glog.Flush()
	common.GetStatisticInstance().Init(configData.EnableStatistics, configData.StatisticsFlushInterval)
	go_util.Init(configData.IsTestEnvironment)
	glbBitdbIp, err = common.GetIntranetIp()
	if err != nil {
		panic(err)
	}
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	// pKafkaManager := &common.KafkaManager{}
	// pKafkaManager.Init()
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

		crawlerInfo := configData.CrawlerInfos[coinType]
		hub := pMongoManager.Servicehub[coinType]
		crawler := &MonitoredAddrCrawler{
			Hub:         hub,
			CrawlerInfo: crawlerInfo,
			Fullnodes:   clients}
		go CrawlLoop(crawler)
		go FixAddrTxTimestamp(crawler)
		go CheckAddrBalanceLoop(crawler)
		go SyncMonitoredAddr(hub)
		go UpdateOwnerBalance(hub)
		go NotifyLoop(hub)
	}

	for {
		time.Sleep(time.Duration(5) * time.Second)
	}
}
