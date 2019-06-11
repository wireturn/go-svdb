package main

import "mempool.com/foundation/bitdb/common"

type CrawlerInfo struct {
	CrawlerOffset int64
	CrawlerCount  int64
	StableCnt     int64
}

type CrawlInfoMap map[string]CrawlerInfo

type ConfigData struct {
	CrawlerInfos            CrawlInfoMap
	EnableStatistics        bool
	StatisticsFlushInterval int
	Fullnode                common.FullnodeConfig
	Mongo                   map[string]common.MongoConfig
	IsTestEnvironment       bool
}
