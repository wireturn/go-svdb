package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcutil"
	"github.com/golang/glog"
	zmq "github.com/pebbe/zmq4"
	"mempool.com/foundation/bitdb/common"
	go_util "mempool.com/foundation/go-util"
)

type AddrVal struct {
	Addr string `json:"addr"`
	Val  int64  `json:"val"`
}

type NewTxJson struct {
	Appid    string     `json:"appid"`
	Ownerid  int        `json:"ownerid"`
	Txid     string     `json:"txid"`
	AddrVals []*AddrVal `json:"addrval"`
	TotalVal int64      `json:"totalval"`
}

type SubscriptManager struct {
	lastBlockTxHashMapLock   sync.Mutex
	lastBlockTxHashMap       map[string]int
	monitoredAddrCallbackUrl string
	newBlockCallbackUrl      string
}

func getNotifyMapKey(appid string, ownerid int) string {
	return fmt.Sprintf("%s_%d", appid, ownerid)
}

func (this *SubscriptManager) CallUrl(originurl string) {
	resp, err := http.Get(originurl)
	if err != nil {
		glog.Infof("http.Get %s failed %s", originurl, err)
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Infof("http.Get ioutil.ReadAll %s failed", originurl)
	}
}
func (this *SubscriptManager) NotifyNewTx(newTxJson *NewTxJson) {
	content, err := json.Marshal(newTxJson)
	if err != nil {
		glog.Info(err)
		return
	}

	content64 := base64.StdEncoding.EncodeToString(content)
	resp, err := http.Post(this.monitoredAddrCallbackUrl, "application/x-www-form-urlencoded", strings.NewReader(content64))
	if err != nil {
		glog.Info(err)
		return
	}

	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Info(err)
		return
	}
}

func (this *SubscriptManager) NotifyLargeTrades(newTxJson *NewTxJson) {
	//通知消息中心
	if newTxJson.TotalVal >= 100000000 || newTxJson.TotalVal < -100000000 {
		contentMap := make(map[string]interface{})
		contentMap["bitdbIp"] = bitdbIp
		contentMap["appid"] = newTxJson.Appid
		contentMap["ownerid"] = newTxJson.Ownerid
		contentMap["totalVal"] = newTxJson.TotalVal
		contentByte, err := json.Marshal(contentMap)
		if err != nil {
			glog.Infof("NotifyNewTx:Marshal %s", err)
			return
		}
		err = go_util.SmsNotify(go_util.Sms_wechatwallet_large_mount_change, string(contentByte))
		if err != nil {
			glog.Infof("NotifyNewTx:SmsNotify %s", err)
		}
	}
}

func (this *SubscriptManager) NotifyNewBlock(block *btcutil.Block) {
	originurl := fmt.Sprintf("%s/height/%d/hash/%s", this.newBlockCallbackUrl, int64(block.Height()), block.Hash().String())
	this.CallUrl(originurl)
}

func (this *SubscriptManager) processTx(tx *btcutil.Tx, fullnodes []*rpcclient.Client, hub *common.MongoServiceHub) {
	fee := int64(0)
	vouts := make([]string, 0, 10)
	txhash := tx.Hash()
	txhashstr := txhash.String()
	height := int64(-1)
	tid := 0

	this.lastBlockTxHashMapLock.Lock()
	_, ok := this.lastBlockTxHashMap[txhashstr]
	this.lastBlockTxHashMapLock.Unlock()
	if ok {
		return
	}

	timestamp := time.Now().Unix()
	notifyMap := make(map[string]*NewTxJson)
	for index, txout := range tx.MsgTx().TxOut {
		addrstr, err := hub.DecodeOutput(txout)
		if err != nil {
			glog.Infof("decodeOutput %d failed txhash %s index %d %s", height, txhashstr, index, err)
			continue
		}

		err = hub.AddAddrTx(addrstr, height, txhashstr, index, txout.Value, "", 0, txout.PkScript)
		if err != nil {
			glog.Infof("addAddrTx %d failed addr=%s index=%d value=%d %s", height, addrstr, index, txout.Value, err)
			panic(err)
		}

		vout := fmt.Sprintf("%s@%d", addrstr, txout.Value)
		vouts = append(vouts, vout)
		fee -= txout.Value

		appid, ownerid, err := hub.GetMonitoredAddrOwner(addrstr)
		if err != nil {
			continue
		}

		err = hub.AddMonitoredAddrTx(addrstr, height, txhashstr, index, txout.Value, "", -1, txout.PkScript, appid, ownerid, timestamp)
		if err != nil {
			glog.Infof("AddMonitoredAddrTx %d failed addr=%s index=%d value=%d %s", height, addrstr, index, txout.Value, err)
			panic(err)
		}

		addrVal := &AddrVal{
			Addr: addrstr,
			Val:  txout.Value,
		}
		key := getNotifyMapKey(appid, ownerid)
		newTxJson, ok := notifyMap[key]
		if !ok {
			newTxJson = &NewTxJson{
				Appid:    appid,
				Ownerid:  ownerid,
				Txid:     txhashstr,
				TotalVal: 0,
				AddrVals: make([]*AddrVal, 0, 10),
			}
			notifyMap[key] = newTxJson
		}
		newTxJson.TotalVal += txout.Value
		newTxJson.AddrVals = append(newTxJson.AddrVals, addrVal)
	}

	vins := make([]string, 0, 10)
	coinDay := int64(0)
	for index, txin := range tx.MsgTx().TxIn {
		addrstr, vinvalue, _, err := hub.DecodeInput(fullnodes, txhashstr, index, txin, height, tid)
		if err != nil {
			glog.Infof("decodeInput %d %s %d failed %s", height, txhashstr, index, err)
			continue
		}

		previousOutPoint := txin.PreviousOutPoint
		err = hub.AddAddrTx(addrstr, height, txhashstr, index, vinvalue, previousOutPoint.Hash.String(),
			int(previousOutPoint.Index), txin.SignatureScript)
		if err != nil {
			glog.Infof("addAddrTx %d failed index=%d value=%d %s", height, index, vinvalue, err)
			panic(err)
		}

		vin := fmt.Sprintf("%s@%d", addrstr, vinvalue)
		vins = append(vins, vin)
		fee += vinvalue

		appid, ownerid, err := hub.GetMonitoredAddrOwner(addrstr)
		if err != nil {
			continue
		}

		err = hub.AddMonitoredAddrTx(addrstr, height, txhashstr, index, -vinvalue, previousOutPoint.Hash.String(),
			int(previousOutPoint.Index), txin.SignatureScript, appid, ownerid, timestamp)
		if err != nil {
			glog.Infof("AddMonitoredAddrTx %d failed addr=%s index=%d value=%d %s", height, addrstr, index, vinvalue, err)
			panic(err)
		}

		addrVal := &AddrVal{
			Addr: addrstr,
			Val:  -vinvalue,
		}
		key := getNotifyMapKey(appid, ownerid)
		newTxJson, ok := notifyMap[key]
		if !ok {
			newTxJson = &NewTxJson{
				Appid:    appid,
				Ownerid:  ownerid,
				Txid:     txhashstr,
				TotalVal: 0,
				AddrVals: make([]*AddrVal, 0, 10),
			}
			notifyMap[key] = newTxJson
		}
		newTxJson.TotalVal -= vinvalue
		newTxJson.AddrVals = append(newTxJson.AddrVals, addrVal)
	}

	for _, newTxJson := range notifyMap {
		go this.NotifyNewTx(newTxJson)
		go this.NotifyLargeTrades(newTxJson)
	}

	hub.AddTx(txhashstr, height, vins, vouts, fee, coinDay, tx.MsgTx().SerializeSize())
}

func (this *SubscriptManager) updateMonitoredAddrTime(
	fullnodes []*rpcclient.Client,
	hub *common.MongoServiceHub,
	block *btcutil.Block) {
	for _, tx := range block.Transactions() {
		txhash := tx.Hash()
		txhashstr := txhash.String()
		for _, txout := range tx.MsgTx().TxOut {
			addrstr, err := hub.DecodeOutput(txout)
			if err != nil {
				glog.Infof("updateMonitoredAddrTime DecodeOutput failed %s", err)
				continue
			}
			hub.UpdateMonitoredAddrUpdateAt(addrstr, int64(block.Height()), block.Hash().String())
		}
		for index, txin := range tx.MsgTx().TxIn {
			addrstr, _, _, err := hub.DecodeInput(fullnodes, txhashstr, index, txin, int64(block.Height()), 0)
			if err != nil {
				glog.Infof("updateMonitoredAddrTime DecodeInput failed %s", err)
				continue
			}
			hub.UpdateMonitoredAddrUpdateAt(addrstr, int64(block.Height()), block.Hash().String())
		}
	}
}

func (this *SubscriptManager) refillMempoolTx(fullnodes []*rpcclient.Client, hub *common.MongoServiceHub) {
	hub.RemoveAllUnconfirmedTx()

	mempoolTxHashes, err := common.GetFullnode(fullnodes).GetRawMempool()
	if nil != err {
		panic(err)
	}

	glog.Info("refillMempoolTx ", len(mempoolTxHashes))
	for _, hash := range mempoolTxHashes {
		tx, err := common.GetFullnode(fullnodes).GetRawTransaction(hash)
		if nil != err {
			panic(err)
		}
		this.processTx(tx, fullnodes, hub)
	}
}

func (this *SubscriptManager) refillLastBlockTxHash(block *btcutil.Block, hub *common.MongoServiceHub) {
	this.lastBlockTxHashMapLock.Lock()
	this.lastBlockTxHashMap = make(map[string]int)
	for _, tx := range block.Transactions() {
		txHashStr := tx.Hash().String()
		this.lastBlockTxHashMap[txHashStr] = 1
	}
	this.lastBlockTxHashMapLock.Unlock()

	for _, tx := range block.Transactions() {
		txHashStr := tx.Hash().String()
		hub.RemoveUnConfirmedTx(txHashStr)
		//hub.RemoveUnConfirmedMonitoredTx(txHashStr)
	}
}

func (this *SubscriptManager) subscriptTx(fullnodes []*rpcclient.Client, zmqurl string, hub *common.MongoServiceHub) {
	subscriber, err := zmq.NewSocket(zmq.SUB)
	if nil != err {
		panic(err)
	}

	defer subscriber.Close()
	err = subscriber.Connect(zmqurl)
	if nil != err {
		panic(err)
	}

	err = subscriber.SetSubscribe("rawtx")
	if nil != err {
		panic(err)
	}

	err = subscriber.SetRcvtimeo(300 * time.Minute)
	if nil != err {
		panic(err)
	}

	for {
		_, err := subscriber.Recv(0)
		if nil != err {
			panic(err)
		}

		txByte, err := subscriber.RecvBytes(0)
		if nil != err {
			panic(err)
		}
		subscriber.Recv(0)
		tx, err := btcutil.NewTxFromBytes(txByte)
		if err != nil {
			panic(err)
		}

		//glog.Info("subscriptTx ", tx.Hash().String())
		this.processTx(tx, fullnodes, hub)
	}
}

func (this *SubscriptManager) subscriptBlock(fullnodes []*rpcclient.Client, zmqurl string, hub *common.MongoServiceHub) {
	subscriber, err := zmq.NewSocket(zmq.SUB)
	if nil != err {
		panic(err)
	}
	defer subscriber.Close()
	err = subscriber.Connect(zmqurl)
	if nil != err {
		panic(err)
	}
	err = subscriber.SetSubscribe("rawblock")
	if nil != err {
		panic(err)
	}
	err = subscriber.SetRcvtimeo(300 * time.Minute)
	if nil != err {
		panic(err)
	}

	for {
		_, err := subscriber.Recv(0)
		if nil != err {
			panic(err)
		}
		blockbytes, err := subscriber.RecvBytes(0)
		if nil != err {
			panic(err)
		}
		subscriber.Recv(0)
		block, err := btcutil.NewBlockFromBytes(blockbytes)
		if err != nil {
			panic(err)
		}

		glog.Infof("subscriptBlock refillLastBlockTxHash %s %d", block.Hash().String(), block.Height())
		this.refillLastBlockTxHash(block, hub)
		this.updateMonitoredAddrTime(fullnodes, hub, block)
		//this.NotifyNewBlock(block)
	}
}

func (this *SubscriptManager) fixMonitoredAddrHeight(hub *common.MongoServiceHub) {
	for {
		addrs, err := hub.GetUnknownMonitoredAddr()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			block, err := hub.GetBlockInfoByHash(addr.UpdateAtHash)
			if err != nil {
				continue
			}
			hub.UpdateMonitoredAddrUpdateAt(addr.Addr, block.Height, addr.UpdateAtHash)
		}

		time.Sleep(5 * time.Second)
	}
}

func (this *SubscriptManager) removeDuplicateUnconfirmedTx(hub *common.MongoServiceHub) {
	for {
		hub.RemoveDuplicateUnconfirmedTx()
		time.Sleep(5 * time.Second)
	}
}

func (this *SubscriptManager) updateAllMonitoredAddr(hub *common.MongoServiceHub) {
	for {
		lastBlock := hub.GetLastInputOffset()
		hub.UpdateAllMonitoredAddr(lastBlock)
		time.Sleep(5 * time.Minute)
	}
}

func (this *SubscriptManager) cleanUnconfirmedTx(hub *common.MongoServiceHub) {
	for {
		hub.CleanUnconfirmTx()
		time.Sleep(time.Minute * 10)
	}

}

func (this *SubscriptManager) init(
	fullnodes []*rpcclient.Client,
	zmqurl string,
	monitoredAddrCallbackUrl string,
	newBlockCallbackUrl string,
	hub *common.MongoServiceHub) {
	glog.Info("init monitoredAddrCallbackUrl ", monitoredAddrCallbackUrl)
	glog.Info("init newBlockCallbackUrl ", newBlockCallbackUrl)
	glog.Flush()

	this.lastBlockTxHashMap = make(map[string]int)
	this.monitoredAddrCallbackUrl = monitoredAddrCallbackUrl
	this.newBlockCallbackUrl = newBlockCallbackUrl
	this.refillMempoolTx(fullnodes, hub)

	go this.subscriptTx(fullnodes, zmqurl, hub)
	go this.subscriptBlock(fullnodes, zmqurl, hub)
	go this.fixMonitoredAddrHeight(hub)
	go this.removeDuplicateUnconfirmedTx(hub)
	go this.updateAllMonitoredAddr(hub)
	go this.cleanUnconfirmedTx(hub)
}
