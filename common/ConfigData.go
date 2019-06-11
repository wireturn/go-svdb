package common

type FullnodeInfo struct {
	URL    []string
	ZMQURL string
}
type FullnodeConfig map[string]FullnodeInfo

type MongoConfig struct {
	AddrService              []string
	BlockHeaderService       string
	AddrIndexService         string
	HeightAddrService        string
	HeightHashService        string
	MonitoredService         string
	SyncMonitoredAddrService string
}
