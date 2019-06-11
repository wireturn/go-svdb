package main

import "mempool.com/foundation/bitdb/common"

type ConfigData struct {
	Mongo  map[string]common.MongoConfig
	Nodes  []string
	Listen string
}
