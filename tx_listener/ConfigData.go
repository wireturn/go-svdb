package main

import "mempool.com/foundation/bitdb/common"

type ConfigData struct {
	MonitoredAddrCallbackUrl string
	NewBlockCallbackUrl      string
	EnableStatistics         bool
	StatisticsFlushInterval  int
	Fullnode                 common.FullnodeConfig
	Mongo                    map[string]common.MongoConfig
	IsTestEnvironment        bool
}
