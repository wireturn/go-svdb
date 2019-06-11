package common

type SyncManager struct {
	SyncMonitoredAddrService map[string]*SyncMonitoredAddrService
}

func (this *SyncManager) Init() {
	this.SyncMonitoredAddrService = make(map[string]*SyncMonitoredAddrService)
}

type SyncMonitoredAddrService struct {
	hub *MongoServiceHub
}

func (this *SyncManager) AddSyncMonitoredAddrService(coinType string, hub *MongoServiceHub) {
	syncMonitoredAddrService := new(SyncMonitoredAddrService)
	syncMonitoredAddrService.hub = hub
	this.SyncMonitoredAddrService[coinType] = syncMonitoredAddrService
}

func (this *SyncMonitoredAddrService) SyncLoop(hub *MongoServiceHub) {
	//1.首先从同步中心获取全部
	//2.接着将其转换为map
	//3.对照本地的mongodb中有没有
}
