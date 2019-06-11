package main

import (
	"encoding/json"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
	go_util "mempool.com/foundation/go-util"
)

type MonitoredAddrCrawler struct {
	Hub         *common.MongoServiceHub
	CrawlerInfo CrawlerInfo
	Fullnodes   []*rpcclient.Client
}

type BalanceCheckResult struct {
	BitdbIp    string `json:"bitdbIp"`
	Addr       string `json:"addr"`
	Local      int64  `json:"local"`
	BlockChair int64  `json:"blockChair"`
}

func (this *MonitoredAddrCrawler) GetRecrawleCount() int64 {
	return this.CrawlerInfo.RecrawlerCount
}

func (this *MonitoredAddrCrawler) GetCrawlerCount() int64 {
	return this.CrawlerInfo.CrawlerCount
}

func (this *MonitoredAddrCrawler) GetConfigOffset() int64 {
	return this.CrawlerInfo.CrawlerOffset
}

func (this *MonitoredAddrCrawler) GetLastOffset() int64 {
	return this.Hub.GetLastInventoryOffset()
}

func (this *MonitoredAddrCrawler) GetHash(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashInventory(lastBlock, hashmap)
}

func (this *MonitoredAddrCrawler) GetHashPre(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashInventoryPre(lastBlock, hashmap)
}

func (this *MonitoredAddrCrawler) ClearAddrTx(height int64) error {
	return nil
}

func (this *MonitoredAddrCrawler) AddHashPre(height int64) error {
	return this.Hub.AddHeightHashInventoryPre(height, "")
}

func (this *MonitoredAddrCrawler) AddHash(height int64, hash string) error {
	return this.Hub.AddHeightHashInventory(height, hash)
}

func (this *MonitoredAddrCrawler) StartCrawl() bool {
	lastInputHeight := this.Hub.GetLastInputOffset()
	addrs, err := this.Hub.GetChangedMonitoredAddr(lastInputHeight)
	glog.Infof("StartCrawl lastInputHeight=%d len(addrs)=%d", lastInputHeight, len(addrs))
	if err != nil {
		panic(err)
	}

	for addrBson, targetheight := range addrs {
		addrtxs, err := this.Hub.GetAddressTxsByHeight(addrBson.Addr, addrBson.ScrawledAt)
		if err != nil {
			glog.Info("GetAddressTxsByHeight failed ", err)
			continue
		}

		for _, addrtx := range addrtxs {
			//glog.Info("MonitoredAddrCrawler StartCrawl 3 ", addrtx)
			timestamp := int64(-1)
			block, err := this.Hub.GetBlockInfoByHeight(addrtx.Height)
			if err == nil {
				timestamp = block.Time
			}
			err = this.Hub.AddMonitoredAddrTx(addrtx.Addr, addrtx.Height, addrtx.Txid, addrtx.Index, addrtx.Value,
				addrtx.Prehash, addrtx.Preindex, addrtx.SigScript, addrBson.Appid, addrBson.Ownerid, timestamp)
			if err != nil {
				glog.Info("AddMonitoredAddrTx failed ", err)
				panic(err)
			}
		}
		this.Hub.UpdateMonitoredAddrCrawledAt(addrBson.Addr, targetheight)
	}

	return true
}

func CrawlLoop(crawler *MonitoredAddrCrawler) {
	for {
		crawler.StartCrawl()
		time.Sleep(5 * time.Second)
	}
}

func FixAddrTxTimestamp(crawler *MonitoredAddrCrawler) {
	for {
		crawler.Hub.FixMonitoredAddrTxTimestamp()
		time.Sleep(60 * time.Second)
	}
}

func CheckAddrBalance(addr string, hub *common.MongoServiceHub) (*common.BalancePair, error) {
	balanceFromMongo, err := hub.GetAddrUnspentInfo(addr)
	if err != nil {
		glog.Infof("get %s balance Failed", addr)
		return nil, err
	}
	balanceFromBlockchair, err := hub.GetAddrBalanceFromBlockChair(addr)
	if err != nil {
		glog.Infof("get %s balance from Blockchair Failed", addr)
		return nil, err
	}
	if balanceFromBlockchair != balanceFromMongo.Balance {
		result := new(common.BalancePair)
		result.Addr = addr
		result.Local = balanceFromMongo.Balance
		result.BlockChair = balanceFromBlockchair
		return result, nil
	}
	return nil, nil
}

func CheckAddrBalanceLoop(crawler *MonitoredAddrCrawler) {
	for {
		glog.Infof("CheckAddrBalance start")
		monitoredAppidOwnerids := crawler.Hub.GetMonitoredAppidOwnerid()
		for _, monitoredAppidOwnerid := range monitoredAppidOwnerids {
			glog.Infof("CheckAddrBalance %d start", monitoredAppidOwnerid.Ownerid)
			balancePairs, total := crawler.Hub.CheckOwnerBalance(monitoredAppidOwnerid.Appid, monitoredAppidOwnerid.Ownerid)
			if len(balancePairs) == 0 {
				glog.Infof("CheckAddrBalance %d finish", monitoredAppidOwnerid.Ownerid)
				continue
			}
			//说明有错，再重新检查一遍
			ownerid := monitoredAppidOwnerid.Ownerid
			go func() {
				time.Sleep(time.Minute * 10)
				reCheckBalancePairs := make([]*common.BalancePair, 0, 10)
				for _, balancePair := range balancePairs {
					reCheckBalancePair, err := CheckAddrBalance(balancePair.Addr, crawler.Hub)
					if err != nil {
						glog.Infof("CheckAddrBalance fault %s", err)
						continue
					}
					if reCheckBalancePair != nil {
						//这个地址又出错
						reCheckBalancePairs = append(reCheckBalancePairs, reCheckBalancePair)
					}
				}
				if len(reCheckBalancePairs) == 0 {
					//重新检查后都已经是对的了
					return
				}
				result := make(map[string]interface{})
				result["bitdbIp"] = glbBitdbIp
				result["total"] = total
				result["different"] = len(reCheckBalancePairs)
				result["balancePairs"] = reCheckBalancePairs
				result["ownerid"] = ownerid
				contentByte, err := json.Marshal(result)
				if err != nil {
					glog.Infof("CheckBalance Marshal:%s", err)
				}
				err = go_util.SmsNotify(go_util.Sms_bitdb_incorrect, string(contentByte))
				if err != nil {
					glog.Infof("CheckBalance SmsNotify:%s", err)
				}
			}()
		}
		glog.Infof("CheckAddrBalance finish once")
		time.Sleep(50 * time.Minute)
	}
}

func UpdateOwnerBalance(hub *common.MongoServiceHub) {
	for {
		//1.首先获得appid和ownerid
		monitoredAppidOwnerids := hub.GetMonitoredAppidOwnerid()
		//2.逐个计算余额并写入
		for _, monitoredAppidOwnerid := range monitoredAppidOwnerids {
			//计算余额
			balance, err := hub.GetBalance(monitoredAppidOwnerid.Appid, monitoredAppidOwnerid.Ownerid)
			if err != nil {
				glog.Infof("CountBalance:%d", err)
				continue
			}
			//写入
			err = hub.UpsertBalance(monitoredAppidOwnerid.Appid, monitoredAppidOwnerid.Ownerid, balance)
			if err != nil {
				glog.Infof("CountBalance:%d", err)
			}
		}
		//先半个小时算一次
		time.Sleep(time.Minute * 30)
	}
}

func NotifyLoop(hub *common.MongoServiceHub) {
	//先设置时间为今天的7点40
	NextTime := time.Now()
	h, m, _ := NextTime.Clock()
	durationHour := 7 - h
	durationMin := 40 - m
	NextTime = NextTime.Add(time.Hour*time.Duration(durationHour) + time.Minute*time.Duration(durationMin))
	for {
		for i := 0; i < 10; i++ {
			//1.检查时间
			now := time.Now()
			if now.Before(NextTime) {
				glog.Infof("NotifyLoop 1")
				break
			}
			contentMap := make(map[string]interface{})
			contentMap["bitdbIp"] = glbBitdbIp
			//2.获取总余额
			totalBalance, err := hub.GetTotalBalance()
			if err != nil {
				glog.Infof("doNotify GetTotalBalance:%s", err)
				continue
			}
			contentMap["totalBalance"] = totalBalance
			//3.获取前十名
			monitoredAppidOwneridBalances, err := hub.GetBalanceTopTen()
			if err != nil {
				glog.Infof("doNotify GetBalanceTopTen:%s", err)
				continue
			}
			contentMap["balancesTopTen"] = monitoredAppidOwneridBalances
			//4.获取用户总数
			count, err := hub.GetOwnerCount()
			if err != nil {
				glog.Infof("doNotify monitoredAppidOwneridBalances:%s", err)
				time.Sleep(time.Second * 1)
				continue

			}
			contentMap["ownerCount"] = count
			//5.发送数据
			contentByte, err := json.Marshal(contentMap)
			if err != nil {
				glog.Infof("NotifyLoop:GetContext %s", err)
				continue
			}
			glog.Infof("NotifyLoop 2 %s", string(contentByte))
			err = go_util.SmsNotify(go_util.Sms_wechatwallet_daily, string(contentByte))
			if err != nil {
				glog.Infof("NotifyLoop:SmsNotify %s", err)
				continue
			}
			//6.发送成功修改时间
			NextTime = NextTime.Add(time.Hour * 24)
			break
		}
		time.Sleep(time.Minute * 5)
	}
}

func SyncMonitoredAddr(hub *common.MongoServiceHub) {
	tid := 1
	for {
		//1.首先两个Mongo获取数据
		glog.Infof("SyncMonitoredAddr:GetLocalMonitoredAddrMap start")
		fragment := common.StartTrace("SyncMonitoredAddr GetLocalMonitoredAddrMap")
		local := hub.GetLocalMonitoredAddrMap()
		fragment.StopTrace(tid)

		glog.Infof("SyncMonitoredAddr:GetSynCenterMonitoredAddrMap start")
		fragment = common.StartTrace("SyncMonitoredAddr GetSynCenterMonitoredAddrMap")
		syncCenter := hub.GetSynCenterMonitoredAddrMap()
		fragment.StopTrace(tid)

		glog.Infof("SyncMonitoredAddr:PutMonitoredAddrs start")
		//2.相互同步数据
		fragment = common.StartTrace("SyncMonitoredAddr SyncMonitoredAddrs")
		err := hub.SyncMonitoredAddrs(local, syncCenter)
		fragment.StopTrace(tid)
		if err != nil {
			glog.Infof("SyncMonitoredAddr:%s", err)

		}
		time.Sleep(time.Minute)
	}
}
