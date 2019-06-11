package common

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bitly/go-simplejson"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/golang/glog"
	"github.com/kataras/iris/core/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var glbServiceIndexLock sync.Mutex

var glbRequestLock sync.Mutex
var glbRequestMap map[string]*mgo.Bulk = make(map[string]*mgo.Bulk)

type AddrUnspentInfo struct {
	Address                 string `json:"address"`
	Balance                 int64  `json:"balance"`
	TotalReceived           int64  `json:"totalReceived"`
	TotalSent               int64  `json:"totalSent"`
	TxApperances            int    `json:"txApperances"`
	UnconfirmedBalance      int64  `json:"unconfirmedBalance"`
	UnconfirmedTxApperances int    `json:"unconfirmedTxApperances"`
}

type MongoService struct {
	mgoDb []*mgo.Database
}

type MongoServiceHub struct {
	blockHeaderService            *MongoService
	blockHeaderPreService         *MongoService
	addrIndexService              *MongoService
	heightAddrService             *MongoService
	heightHashService             *MongoService
	heightHashInputService        *MongoService
	heightHashOutputService       *MongoService
	heightHashInputPreService     *MongoService
	heightHashOutputPreService    *MongoService
	heightHashInventoryPreService *MongoService
	heightHashInventoryService    *MongoService
	heightHashTxService           *MongoService
	addrService                   *MongoService
	txService                     *MongoService
	txOutputService               *MongoService
	blockTxService                *MongoService
	monitoredAddrService          *MongoService
	monitoredAddrInventoryService *MongoService
	syncMonitoredAddrService      *MongoService
	mutex                         *sync.Mutex
	sessionsCount                 int
}

type MongoManager struct {
	Servicehub map[string]*MongoServiceHub
}

const (
	COL_ADDRTX                   = "addr_tx"
	COL_TX                       = "tx"
	COL_TX_OUTPUT                = "tx_output"
	COL_BLOCKHEADER              = "block_header"
	COL_BLOCKHEADER_PRE          = "block_header_pre"
	COL_ADDRINDEX                = "addr_index"
	COL_HEIGHTADDR               = "height_addr"
	COL_HEIGHTHASH               = "height_hash"
	COL_HEIGHTHASH_INPUT         = "height_hash_input"
	COL_HEIGHTHASH_OUTPUT        = "height_hash_output"
	COL_HEIGHTHASH_INVENTORY     = "height_hash_inventory"
	COL_HEIGHTHASH_INPUT_PRE     = "height_hash_input_pre"
	COL_HEIGHTHASH_OUTPUT_PRE    = "height_hash_output_pre"
	COL_HEIGHTHASH_INVENTORY_PRE = "height_hash_inventory_pre"
	COL_HEIGHTHASH_TX            = "height_hash_tx"
	COL_BLOCKTX                  = "block_tx"
	COL_MONITORED_ADDR           = "monitored_addr"
	COL_MONITORED_ADDR_INVENTORY = "monitored_addr_inventory"
	COL_SYNC_MONITORED_ADDR      = "sync_monitored_addr"
	COL_MONITORED_BALANCE        = "monitored_balance"
)

type TxBson struct {
	Hash          string   `bson:"hash"`
	Height        int64    `bson:"height"`
	VinAddrValue  []string `bson:"vin"`
	VoutAddrValue []string `bson:"vout"`
	Fee           int64    `bson:"fee"`
	CoinDay       int64    `bson:"coinday"`
	Confirmations int64    `bson:"confirmations"`
	Time          int64    `bson:"time"`
	Size          int      `bson:"size"`
}

type HeightAddrBson struct {
	Height int64 `bson:"height"`

	Addr string `bson:"addrs"`
}

type HeightHashBson struct {
	Height int64 `bson:"height"`

	Hash string `bson:"hash"`
}

type AddrIndex struct {
	Addr string `bson:"addr"`

	Index int `bson:"index"`
}

type AddrTxBson struct {
	Addr string `bson:"addr"`

	Height int64 `bson:"height"`

	Index int `bson:"index"`

	Value int64 `bson:"value"`

	Txid string `bson:"txid"`

	IsInput bool `bson:"isinput"`

	Prehash string `bson:"prehash"`

	Preindex int `bson:"preindex"`

	SigScript []byte `bson:"sigscript"`
}

type MonitoredAddrTxBson struct {
	Appid string `bson:"appid"`

	Ownerid int `bson:"ownerid"`

	Addr string `bson:"addr"`

	Height int64 `bson:"height"`

	Index int `bson:"index"`

	Value int64 `bson:"value"`

	Txid string `bson:"txid"`

	IsInput bool `bson:"isinput"`

	Prehash string `bson:"prehash"`

	Preindex int `bson:"preindex"`

	SigScript []byte `bson:"sigscript"`

	Timestamp int64 `bson:"timestamp"`
}

type InventoryID struct {
	Txid      string `bson:"txid"`
	Height    int64  `bson:"height"`
	Timestamp int64  `bson:"timestamp"`
}
type InventoryResult struct {
	Id    InventoryID `bson:"_id"`
	Value int64       `bson:"value"`
}
type InventoryResultReal struct {
	Value     int64  `bson:"value"`
	Txid      string `bson:"txid"`
	Height    int64  `bson:"height"`
	Timestamp int64  `bson:"timestamp"`
}
type InventoryResultRealWithCount struct {
	Data  []*InventoryResultReal `bson:"data"`
	Count int64                  `bson:"count"`
}

type BlockVerboseBson struct {
	Hash          string                `bson:"hash"`
	Confirmations int64                 `bson:"confirmations"`
	StrippedSize  int32                 `bson:"strippedsize"`
	Size          int32                 `bson:"size"`
	Weight        int32                 `bson:"weight"`
	Height        int64                 `bson:"height"`
	Version       int32                 `bson:"version"`
	VersionHex    string                `bson:"versionHex"`
	MerkleRoot    string                `bson:"merkleroot"`
	Tx            []string              `bson:"tx,omitempty"`
	RawTx         []btcjson.TxRawResult `bson:"rawtx,omitempty"`
	Time          int64                 `bson:"time"`
	Nonce         uint32                `bson:"nonce"`
	Bits          string                `bson:"bits"`
	Difficulty    float64               `bson:"difficulty"`
	PreviousHash  string                `bson:"previousblockhash"`
	NextHash      string                `bson:"nextblockhash,omitempty"`
	TxCnt         int                   `bson:"txcnt"`
	CoinBaseInfo  []byte                `bson:"coinbaseinfo"`
	CoinBaseTxid  string                `bson:"coinbasetxid"`
}

type TxError struct {
	Hash     string `bson:"hash"`
	Index    int    `bson:"index"`
	PreHash  string `bson:"prehash"`
	PreIndex int    `bson:"preindex"`
}

type BlockTxhash struct {
	Blockhash string `bson:"blockhash"`

	Txhash string `bson:"txhash"`
}

type MonitoredAddrBson struct {
	Appid        string `bson:"appid"`
	Ownerid      int    `bson:"ownerid"`
	Addr         string `bson:"addr"`
	ScrawledAt   int64  `bson:"scrawledat"`
	UpdateAt     int64  `bson:"updateat"`
	UpdateAtHash string `bson:"updateathash"`
}

type CrawlCountInfo struct {
	AddrTxCount                 int64
	BlockHeaderCount            int64
	BlockHeaderPreCount         int64
	BlockTxCount                int64
	HeightAddrCount             int64
	HeightHashInputCount        int64
	HeightHashInputPreCount     int64
	HeightHashOutputCount       int64
	HeightHashOutputPreCount    int64
	MonitoredAddrCount          int64
	MonitoredAddrInventoryCount int64
	TxCount                     int64
	TxOutputCount               int64
}

// type MonitoredAddrCount struct {
// 	Count int
// }

type MonitoredAppidOwnerid struct {
	Appid   string `bson:"appid"`
	Ownerid int    `bson:"ownerid"`
}
type MonitoredAppidOwneridBalance struct {
	Appid   string `bson:"appid"`
	Ownerid int    `bson:"ownerid"`
	Balance int64  `bson:"balance"`
}

type MonitoredDailyInfo struct {
	TotalBalance                  int64                           `json:"totalBalance"`
	MonitoredAppidOwneridBalances []*MonitoredAppidOwneridBalance `json:"topTen"`
	OwnerCount                    int                             `json:"ownerCount"`
}

func (this *MongoManager) initMongoService(coinType string, addr string) *MongoService {
	mongoService := &MongoService{}
	session, err := mgo.Dial(addr)
	if err != nil {
		panic(addr)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSocketTimeout(24 * time.Hour)
	session.SetPoolLimit(300)
	mongoService.mgoDb = append(mongoService.mgoDb, session.DB("bitdb_"+coinType))
	return mongoService
}

func (this *MongoManager) Init() {
	this.Servicehub = make(map[string]*MongoServiceHub)
}

func (this *MongoServiceHub) GetRequestKey(index int, collectionName string) string {
	return fmt.Sprintf("%p@@%d@@%s", this, index, collectionName)
}

func (this *MongoServiceHub) ConsumeRequest() {
	tmpMap := make(map[string]*mgo.Bulk)
	glbRequestLock.Lock()
	tmpMap = glbRequestMap
	glbRequestMap = make(map[string]*mgo.Bulk)
	glbRequestLock.Unlock()

	for _, v := range tmpMap {
		_, err := v.Run()
		if err != nil {
			glog.Flush()
			panic(err.Error())
		}
	}
}

func (this *MongoManager) AddServiceHub(k string, addrs []string, blockheader string,
	addrindex string, heightaddr string, heighthash string, monitored string, syncCenter string) {
	mongoServiceHub := &MongoServiceHub{}
	mongoServiceHub.mutex = &sync.Mutex{}
	mongoAddrService := &MongoService{}
	mongoServiceHub.addrService = mongoAddrService
	mongoTxService := &MongoService{}
	mongoServiceHub.txService = mongoTxService
	mongoTxOutputService := &MongoService{}
	mongoServiceHub.txOutputService = mongoTxOutputService
	mongoBlockTxService := &MongoService{}
	mongoServiceHub.blockTxService = mongoBlockTxService
	// monitoredAddrInventoryService := &MongoService{}
	// mongoServiceHub.monitoredAddrInventoryService = monitoredAddrInventoryService
	for _, addr := range addrs {
		session, err := mgo.Dial(addr)
		if err != nil {
			glog.Info("addr ", addr)
			glog.Flush()
			panic(err)
		}

		session.SetMode(mgo.Monotonic, true)
		session.SetSocketTimeout(24 * time.Hour)
		session.SetPoolLimit(300)
		mongoAddrService.mgoDb = append(mongoAddrService.mgoDb, session.DB("bitdb_"+k))
		mongoTxService.mgoDb = append(mongoTxService.mgoDb, session.DB("bitdb_"+k))
		mongoTxOutputService.mgoDb = append(mongoTxOutputService.mgoDb, session.DB("bitdb_"+k))
		mongoBlockTxService.mgoDb = append(mongoBlockTxService.mgoDb, session.DB("bitdb_"+k))
	}

	mongoServiceHub.blockHeaderService = this.initMongoService(k, blockheader)
	mongoServiceHub.blockHeaderPreService = this.initMongoService(k, blockheader)
	mongoServiceHub.addrIndexService = this.initMongoService(k, addrindex)
	mongoServiceHub.heightAddrService = this.initMongoService(k, heightaddr)
	mongoServiceHub.heightHashService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashInputService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashOutputService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashInventoryService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashInputPreService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashOutputPreService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashInventoryPreService = this.initMongoService(k, heighthash)
	mongoServiceHub.heightHashTxService = this.initMongoService(k, heighthash)
	mongoServiceHub.monitoredAddrService = this.initMongoService(k, monitored)
	mongoServiceHub.monitoredAddrInventoryService = this.initMongoService(k, monitored)
	mongoServiceHub.syncMonitoredAddrService = this.initMongoService(k, syncCenter)
	this.Servicehub[k] = mongoServiceHub
}

func (this *MongoServiceHub) AddMonitoredAddr(appid string, ownerid int, addr string) error {
	monitoredAddrBson := &MonitoredAddrBson{
		Appid:        appid,
		Ownerid:      ownerid,
		Addr:         addr,
		ScrawledAt:   -1,
		UpdateAt:     this.GetLastInputOffset(),
		UpdateAtHash: "",
	}
	//先向本地插入
	err := this.getMongoService(0, COL_MONITORED_ADDR).Insert(monitoredAddrBson)
	if err != nil {
		return err
	}
	//在接着向同步中心插入
	err = this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Insert(bson.M{
		"appid":   monitoredAddrBson.Appid,
		"ownerid": monitoredAddrBson.Ownerid,
		"addr":    monitoredAddrBson.Addr})
	return err
}

func (this *MongoServiceHub) GetMonitoredAddr(addr string) int {
	monitoredAddrBson := &MonitoredAddrBson{}
	err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"addr": addr}).One(monitoredAddrBson)
	if err != nil {
		return -1
	}

	return monitoredAddrBson.Ownerid
}

func (this *MongoServiceHub) UpdateMonitoredAddrCrawledAt(addr string, crawledat int64) error {
	monitoredAddrBson := &MonitoredAddrBson{}
	err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"addr": addr}).One(monitoredAddrBson)
	if err != nil {
		return nil
	}

	monitoredAddrBson.ScrawledAt = crawledat
	err = this.getMongoService(0, COL_MONITORED_ADDR).Update(bson.M{"addr": addr}, monitoredAddrBson)
	return err
}

func (this *MongoServiceHub) UpdateMonitoredAddrUpdateAt(addr string, updateat int64, updateathash string) error {
	monitoredAddrBson := &MonitoredAddrBson{}
	err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"addr": addr}).One(monitoredAddrBson)
	if err != nil {
		return nil
	}

	monitoredAddrBson.UpdateAt = updateat
	monitoredAddrBson.UpdateAtHash = updateathash
	err = this.getMongoService(0, COL_MONITORED_ADDR).Update(bson.M{"addr": addr}, monitoredAddrBson)
	return err
}

func (this *MongoServiceHub) UpdateAllMonitoredAddr(updateat int64) {
	monitoredAddrBson := &MonitoredAddrBson{}
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Iter()
	for iter.Next(monitoredAddrBson) {
		this.UpdateMonitoredAddrUpdateAt(monitoredAddrBson.Addr, updateat, "")
	}
}

func (this *MongoServiceHub) GetChangedMonitoredAddr(height int64) (map[*MonitoredAddrBson]int64, error) {
	addrs := make(map[*MonitoredAddrBson]int64)
	monitored_addr := &MonitoredAddrBson{}
	iter := this.getMongoService(0, COL_MONITORED_ADDR).
		Find(nil).Iter()
	for iter.Next(monitored_addr) {
		if monitored_addr.UpdateAt <= monitored_addr.ScrawledAt {
			continue
		}
		targetHeight := monitored_addr.UpdateAt
		if targetHeight > height {
			targetHeight = height
		}
		tmpAddrBson := &MonitoredAddrBson{
			Appid:        monitored_addr.Appid,
			Ownerid:      monitored_addr.Ownerid,
			Addr:         monitored_addr.Addr,
			ScrawledAt:   monitored_addr.ScrawledAt,
			UpdateAt:     monitored_addr.UpdateAt,
			UpdateAtHash: monitored_addr.UpdateAtHash,
		}
		addrs[tmpAddrBson] = targetHeight
	}

	return addrs, nil
}

func (this *MongoServiceHub) GetBalance(appid string, ownerid int) (int64, error) {
	balance := int64(0)

	job := &mgo.MapReduce{
		Map:    "function() { emit(1, this.value) }",
		Reduce: "function(key, values) { return Array.sum(values) }",
	}

	var result []struct {
		One   int64
		Value int
	}

	_, err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(bson.M{
		"appid":   appid,
		"ownerid": ownerid,
	}).MapReduce(job, &result)
	if err != nil {
		return balance, err
	}

	if len(result) > 0 {
		balance = int64(result[0].Value)
	}

	return balance, nil
}

func (this *MongoServiceHub) GetInventory(appid string, ownerid int, start int, end int) (*InventoryResultRealWithCount, error) {
	m := []bson.M{
		{"$match": bson.M{"appid": appid, "ownerid": ownerid}},
		{"$group": bson.M{"_id": bson.M{"txid": "$txid", "height": "$height", "timestamp": "$timestamp"}, "value": bson.M{"$sum": "$value"}}},
		{"$sort": bson.M{"_id.timestamp": -1}},
		{"$skip": start},
		{"$limit": end - start},
	}

	ret := &InventoryResultRealWithCount{}
	resultWithTime := make([]*InventoryResultReal, 0, 10)
	var result []InventoryResult
	err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Pipe(m).All(&result)
	if err != nil {
		return ret, err
	}

	var txStrs []string
	err = this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).
		Find(bson.M{"appid": appid, "ownerid": ownerid}).
		Distinct("txid", &txStrs)
	if err != nil {
		return ret, err
	}

	for _, inventoryResult := range result {
		tmpInventoryResult := &InventoryResultReal{
			Value:     inventoryResult.Value,
			Txid:      inventoryResult.Id.Txid,
			Height:    inventoryResult.Id.Height,
			Timestamp: inventoryResult.Id.Timestamp,
		}
		resultWithTime = append(resultWithTime, tmpInventoryResult)
	}

	ret.Count = int64(len(txStrs))
	ret.Data = resultWithTime
	return ret, err
}

func (this *MongoServiceHub) GetMonitoredAddrUtxo(appid string, ownerid int, value int64) (int64, []*MonitoredAddrTxBson, error) {
	vins := make([]*MonitoredAddrTxBson, 0, 10)
	vouts := make([]*MonitoredAddrTxBson, 0, 10)
	rets := make([]*MonitoredAddrTxBson, 0, 10)
	totalValue := int64(0)

	err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(bson.M{
		"appid":   appid,
		"ownerid": ownerid,
		"isinput": false,
	}).Sort("height").All(&vouts)
	if err != nil {
		return totalValue, rets, err
	}
	err = this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(bson.M{
		"appid":   appid,
		"ownerid": ownerid,
		"isinput": true,
	}).Sort("height").All(&vins)
	if err != nil {
		return totalValue, rets, err
	}

	vinMap := make(map[string]bool)
	for _, vin := range vins {
		key := fmt.Sprintf("%s-%d", vin.Prehash, vin.Preindex)
		vinMap[key] = true
	}

	for _, vout := range vouts {
		key := fmt.Sprintf("%s-%d", vout.Txid, vout.Index)
		_, ok := vinMap[key]
		if ok {
			continue
		}

		rets = append(rets, vout)
		totalValue += vout.Value
	}

	return totalValue, rets, nil
}

func (this *MongoServiceHub) GetUnknownMonitoredAddr() ([]*MonitoredAddrBson, error) {
	addrs := make([]*MonitoredAddrBson, 0, 10)
	err := this.getMongoService(0, COL_MONITORED_ADDR).
		Find(bson.M{"updateat": -1}).All(&addrs)
	return addrs, err
}

func (this *MongoServiceHub) GetMonitoredAddrOwner(addr string) (string, int, error) {
	monitoredAddrBson := &MonitoredAddrBson{}
	err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"addr": addr}).One(monitoredAddrBson)
	if err != nil {
		return "", -1, err
	}
	return monitoredAddrBson.Appid, monitoredAddrBson.Ownerid, nil
}

func (this *MongoServiceHub) AddMonitoredAddrTx(addr string, height int64, txid string, index int, value int64,
	prehash string, preindex int, sigscript []byte, appid string, ownerid int, timestamp int64) error {
	realvalue := value

	isinput := true
	if prehash == "" {
		isinput = false
	}

	addrTxBson := &MonitoredAddrTxBson{
		Appid:     appid,
		Ownerid:   ownerid,
		Addr:      addr,
		Height:    height,
		Txid:      txid,
		Index:     index,
		Value:     realvalue,
		IsInput:   isinput,
		Prehash:   prehash,
		Preindex:  preindex,
		SigScript: sigscript,
		Timestamp: timestamp,
	}

	_, err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Upsert(bson.M{
		"addr":    addr,
		"txid":    txid,
		"index":   index,
		"isinput": isinput,
	}, addrTxBson)
	return err
}

func (this *MongoServiceHub) FixMonitoredAddrTxTimestamp() {
	addrTxBson := &MonitoredAddrTxBson{}
	iter := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(bson.M{
		"timestamp": -1}).Iter()
	for iter.Next(addrTxBson) {
		block, err := this.GetBlockInfoByHeight(addrTxBson.Height)
		if err != nil {
			continue
		}

		addrTxBson.Timestamp = block.Time
		this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Upsert(bson.M{
			"addr":    addrTxBson.Addr,
			"txid":    addrTxBson.Txid,
			"index":   addrTxBson.Index,
			"isinput": addrTxBson.IsInput,
		}, addrTxBson)
	}
}

func (this *MongoServiceHub) FixMonitoredAddrTxTimestampTmp() {
	addrTxBson := &MonitoredAddrTxBson{}
	iter := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(nil).Iter()
	for iter.Next(addrTxBson) {
		block, err := this.GetBlockInfoByHeight(addrTxBson.Height)
		if err != nil {
			continue
		}

		addrTxBson.Timestamp = block.Time
		this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Upsert(bson.M{
			"addr":    addrTxBson.Addr,
			"txid":    addrTxBson.Txid,
			"index":   addrTxBson.Index,
			"isinput": addrTxBson.IsInput,
		}, addrTxBson)
	}
}

func (p *MongoManager) GetMogAddrService(cointype string, collection string) (*mgo.Collection, error) {
	return p.Servicehub[cointype].getMongoService(0, collection), nil
}

func (p *MongoServiceHub) GetBestBlockHeader(start int, limit int) ([]BlockVerboseBson, error) {
	col := p.getMongoService(0, COL_BLOCKHEADER)
	var items []BlockVerboseBson
	if col == nil {
		glog.Error("get handle fail,")
		return items, nil
	}
	err := col.Find(nil).Select(bson.M{"tx": 0}).Sort("-height").Limit(limit).All(&items)
	return items, err
}

func (p *MongoServiceHub) GetBlockInfoByHeight(height int64) (BlockVerboseBson, error) {
	col := p.getMongoService(0, COL_BLOCKHEADER)
	var items []BlockVerboseBson
	err := col.Find(bson.M{"height": height}).All(&items)
	if len(items) == 0 {
		return BlockVerboseBson{}, nil
	}
	return items[0], err
}

func (p *MongoServiceHub) GetBlockInfoByHash(hash string) (BlockVerboseBson, error) {
	col := p.getMongoService(0, COL_BLOCKHEADER)
	var items []BlockVerboseBson
	err := col.Find(bson.M{"hash": hash}).All(&items)
	if len(items) == 0 {
		return BlockVerboseBson{}, nil
	}
	return items[0], err
}

func (p *MongoServiceHub) GetBlockTxs(blockhash string, start int, end int) ([]*TxBson, error) {
	serviceIndex := p.getServiceIndex(blockhash)
	iter := p.getMongoService(serviceIndex, COL_BLOCKTX).
		Find(bson.M{"blockhash": blockhash}).Skip(start).Limit(end - start).Iter()
	blocktx := &BlockTxhash{}
	txs := make([]*TxBson, 0, 10)
	for iter.Next(blocktx) {
		txServiceIndex := p.getServiceIndex(blocktx.Txhash)
		tx := &TxBson{}
		err := p.getMongoService(txServiceIndex, COL_TX).Find(bson.M{"hash": blocktx.Txhash}).One(tx)
		if err != nil {
			return txs, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
	//if err != nil {
	//	return result, err
	//}
	//
	//var result []TxBson
	//for _, mgo := range p.txService.mgoDb {
	//	var items []TxBson
	//
	//	result = append(result, items...)
	//}
	//if end > len(result) {
	//	end = len(result)
	//}
	//if start < 0 || start > len(result) {
	//	return make([]TxBson, 0), nil
	//}
	//return result[start:end], nil
}

func (p *MongoServiceHub) GetDetailTx(txBson *TxBson) error {
	blockHeader := &BlockVerboseBson{}
	err := p.getMongoService(0, COL_BLOCKHEADER).Find(bson.M{"height": txBson.Height}).One(blockHeader)
	if err != nil {
		return err
	}
	lastblock := p.GetLastBlock()
	txBson.Confirmations = lastblock - txBson.Height
	txBson.Time = blockHeader.Time
	return nil
}

func (p *MongoServiceHub) GetAddressTxs(addr string, start int, end int) ([]*TxBson, error) {
	serviceIndex := p.getServiceIndex(addr)
	txs := make([]*TxBson, 0, 10)
	iter := p.getMongoService(serviceIndex, COL_ADDRTX).
		Find(bson.M{"addr": addr}).Skip(start).Limit(end - start).Iter()
	addrtx := &AddrTxBson{}

	for iter.Next(addrtx) {
		txServiceIndex := p.getServiceIndex(addrtx.Txid)
		tx := &TxBson{}
		err := p.getMongoService(txServiceIndex, COL_TX).Find(bson.M{"hash": addrtx.Txid}).One(tx)
		if err != nil {
			return txs, err
		}
		err = p.GetDetailTx(tx)
		if err != nil {
			return txs, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (this *MongoServiceHub) GetAddressTxsByHeight(addr string, height int64) ([]*AddrTxBson, error) {
	serviceIndex := this.getServiceIndex(addr)
	addrtxs := make([]*AddrTxBson, 0, 10)
	err := this.getMongoService(serviceIndex, COL_ADDRTX).
		Find(bson.M{"addr": addr, "height": bson.M{"$gt": height}}).All(&addrtxs)

	return addrtxs, err
}

func (p *MongoServiceHub) GetTx(txid string) (TxBson, error) {
	if len(txid) < 64 {
		return TxBson{}, errors.New("txid illeagal:" + txid)
	}
	var result []TxBson
	col := p.getMongoService(p.getServiceIndex(txid), COL_TX)
	err := col.Find(bson.M{"hash": txid}).All(&result)
	if len(result) == 0 {
		return TxBson{}, nil
	}
	return result[0], err
}

func (this *MongoServiceHub) getMongoService(index int, collectionName string) *mgo.Collection {
	var mongoService *MongoService
	switch collectionName {
	case COL_BLOCKHEADER:
		mongoService = this.blockHeaderService
	case COL_BLOCKHEADER_PRE:
		mongoService = this.blockHeaderPreService
	case COL_ADDRINDEX:
		mongoService = this.addrIndexService
	case COL_HEIGHTADDR:
		mongoService = this.heightAddrService
	case COL_HEIGHTHASH:
		mongoService = this.heightHashService
	case COL_HEIGHTHASH_INPUT:
		mongoService = this.heightHashInputService
	case COL_HEIGHTHASH_OUTPUT:
		mongoService = this.heightHashOutputService
	case COL_HEIGHTHASH_INVENTORY:
		mongoService = this.heightHashInventoryService
	case COL_HEIGHTHASH_INPUT_PRE:
		mongoService = this.heightHashInputPreService
	case COL_HEIGHTHASH_OUTPUT_PRE:
		mongoService = this.heightHashOutputPreService
	case COL_HEIGHTHASH_INVENTORY_PRE:
		mongoService = this.heightHashInventoryPreService
	case COL_HEIGHTHASH_TX:
		mongoService = this.heightHashTxService
	case COL_ADDRTX:
		mongoService = this.addrService
	case COL_TX:
		mongoService = this.txService
	case COL_TX_OUTPUT:
		mongoService = this.txOutputService
	case COL_BLOCKTX:
		mongoService = this.blockTxService
	case COL_MONITORED_ADDR:
		mongoService = this.monitoredAddrService
	case COL_MONITORED_ADDR_INVENTORY:
		mongoService = this.monitoredAddrInventoryService
	case COL_SYNC_MONITORED_ADDR:
		mongoService = this.syncMonitoredAddrService
	case COL_MONITORED_BALANCE:
		mongoService = this.monitoredAddrService
	default:
		panic(nil)
	}
	//先gc再生成新的
	this.mutex.Lock()
	var needGC bool
	if this.sessionsCount > 200 {
		needGC = true
	}
	this.sessionsCount++
	this.mutex.Unlock()
	if needGC {
		runtime.GC()
	}
	db := mongoService.mgoDb[index]
	session := db.Session.New()
	runtime.SetFinalizer(session, func(session *mgo.Session) {
		this.mutex.Lock()
		this.sessionsCount--
		this.mutex.Unlock()
		session.Close()
	})
	return session.DB(db.Name).C(collectionName)
}

func (p *MongoServiceHub) AddBlockTxhash(blockhash string, txhashes []string) error {
	serviceIndex := p.getServiceIndex(blockhash)
	bulk := p.getMongoService(serviceIndex, COL_BLOCKTX).Bulk()
	for i, txhash := range txhashes {
		blockTxhash := &BlockTxhash{
			Blockhash: blockhash,
			Txhash:    txhash,
		}
		bulk.Insert(blockTxhash)

		if i%1000 == 0 {
			_, err := bulk.Run()
			if err != nil {
				return err
			}
			bulk = p.getMongoService(serviceIndex, COL_BLOCKTX).Bulk()
		}
	}
	_, err := bulk.Run()
	return err
}

func (p *MongoServiceHub) GetBlockTxhash(blockhash string) (txhashes []string, err error) {
	serviceIndex := p.getServiceIndex(blockhash)
	iter := p.getMongoService(serviceIndex, COL_BLOCKTX).Find(bson.M{
		"blockhash": blockhash}).Iter()
	blockTx := &BlockTxhash{}
	txhashes = make([]string, 0, 10)
	for iter.Next(blockTx) {
		txhashes = append(txhashes, blockTx.Txhash)
	}
	return txhashes, nil
}

func (this *MongoServiceHub) AddBulk(serviceIndexKey string, collection string, height int64, docs ...interface{}) error {
	serviceIndex := this.getServiceIndex(serviceIndexKey)

	if height == -1 {
		err := this.getMongoService(serviceIndex, collection).Insert(docs...)
		if err != nil {
			return err
		}
		return nil
	}

	shouldRun := false

	glbRequestLock.Lock()
	key := this.GetRequestKey(serviceIndex, collection)
	bulk, ok := glbRequestMap[key]
	if !ok {
		glbRequestMap[key] = this.getMongoService(serviceIndex, collection).Bulk()
	}
	glbRequestMap[key].Insert(docs...)
	if glbRequestMap[key].Opcount() > 100 {
		shouldRun = true
		glbRequestMap[key] = this.getMongoService(serviceIndex, collection).Bulk()
	}
	glbRequestLock.Unlock()

	if shouldRun {
		_, err := bulk.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *MongoServiceHub) AddTx(hash string, height int64,
	vins []string, vouts []string, fee int64, coinDay int64, size int) error {
	tx := &TxBson{
		Hash:          hash,
		Height:        height,
		VinAddrValue:  vins,
		VoutAddrValue: vouts,
		Fee:           fee,
		CoinDay:       coinDay,
		Size:          size,
	}

	return this.AddBulk(hash, COL_TX, height, tx)
}

func (this *MongoServiceHub) AddTxOutput(hash string, height int64,
	vouts []string, fee int64) error {
	tx := &TxBson{
		Hash:          hash,
		Height:        height,
		VoutAddrValue: vouts,
		Fee:           fee,
	}

	return this.AddBulk(hash, COL_TX_OUTPUT, height, tx)
}

func (this *MongoServiceHub) RemoveTx(hash string) {
	serviceIndex := this.getServiceIndex(hash)
	_, err := this.getMongoService(serviceIndex, COL_TX).RemoveAll(bson.M{
		"hash": hash})
	if err != nil {
		panic(err)
	}

	addrServiceCnt := len(this.addrService.mgoDb)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		_, err = this.getMongoService(serviceIndex, COL_ADDRTX).RemoveAll(bson.M{
			"txid": hash})
		if err != nil {
			panic(err)
		}
	}
}

func (this *MongoServiceHub) RemoveMonitoredTx(txhash string) {
	_, err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).RemoveAll(bson.M{
		"txid": txhash})
	if err != nil {
		panic(err)
	}
}

func (this *MongoServiceHub) RemoveUnConfirmedTx(hash string) {
	serviceIndex := this.getServiceIndex(hash)
	_, err := this.getMongoService(serviceIndex, COL_TX).RemoveAll(bson.M{
		"height": -1, "hash": hash})
	if err != nil {
		panic(err)
	}

	addrServiceCnt := len(this.addrService.mgoDb)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		_, err = this.getMongoService(serviceIndex, COL_ADDRTX).RemoveAll(bson.M{
			"height": -1, "txid": hash})
		if err != nil {
			panic(err)
		}
	}
}

func (this *MongoServiceHub) RemoveUnConfirmedMonitoredTx(txhash string) {
	_, err := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).RemoveAll(bson.M{
		"height": -1, "txid": txhash})
	if err != nil {
		panic(err)
	}
}

func (this *MongoServiceHub) RemoveAllUnconfirmedTx() {
	addrServiceCnt := len(this.addrService.mgoDb)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		_, err := this.getMongoService(serviceIndex, COL_TX).RemoveAll(bson.M{
			"height": -1})
		if err != nil {
			panic(err)
		}

		_, err = this.getMongoService(serviceIndex, COL_ADDRTX).RemoveAll(bson.M{
			"height": -1})
		if err != nil {
			panic(err)
		}
	}
}

func (this *MongoServiceHub) RemoveDuplicateUnconfirmedTx() {
	addrServiceCnt := len(this.addrService.mgoDb)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		txs := make([]*TxBson, 1, 10)
		err := this.getMongoService(serviceIndex, COL_TX).Find(bson.M{
			"height": -1}).All(&txs)
		if err != nil {
			panic(err)
		}

		for _, tx := range txs {
			cnt, err := this.getMongoService(serviceIndex, COL_TX).Find(bson.M{"hash": tx.Hash}).Count()
			if err != nil {
				panic(err)
			}
			if cnt > 1 {
				this.getMongoService(serviceIndex, COL_TX).RemoveAll(bson.M{
					"hash": tx.Hash, "height": -1})
			}
		}
	}
}

func (this *MongoServiceHub) Get24hTxCount() (int64, int64, error) {
	lastHeight := this.GetLastBlock()
	lastHeightYestoday := lastHeight - 144
	job := &mgo.MapReduce{
		Map:    "function() { emit(1, this.txcnt) }",
		Reduce: "function(key, values) { return Array.sum(values) }",
	}

	var result []struct {
		One   int64
		Value int
	}

	_, err := this.getMongoService(0, COL_BLOCKHEADER).Find(bson.M{"height": bson.M{"$gte": lastHeightYestoday}}).MapReduce(job, &result)
	if err != nil {
		return 0, 0, err
	}
	txCnt := int64(0)
	if len(result) > 0 {
		txCnt = int64(result[0].Value)
	}

	block, err := this.GetBlockInfoByHeight(lastHeightYestoday)
	if err != nil {
		return 0, 0, err
	}

	now := time.Now()
	interval := now.Unix() - block.Time
	return txCnt, interval, nil
}

func (this *MongoServiceHub) Get24hHashRate() (float64, int64, error) {
	lastHeight := this.GetLastBlock()
	lastHeightYestoday := lastHeight - 144
	job := &mgo.MapReduce{
		Map:    "function() { emit(1, this.difficulty) }",
		Reduce: "function(key, values) { return Array.sum(values) }",
	}

	var result []struct {
		One   float64
		Value int
	}

	_, err := this.getMongoService(0, COL_BLOCKHEADER).Find(bson.M{"height": bson.M{"$gte": lastHeightYestoday}}).MapReduce(job, &result)
	if err != nil {
		return 0, 0, err
	}
	totalDiff := float64(0)
	if len(result) > 0 {
		totalDiff = float64(result[0].Value)
	}

	block, err := this.GetBlockInfoByHeight(lastHeightYestoday)
	if err != nil {
		return 0, 0, err
	}

	now := time.Now()
	interval := now.Unix() - block.Time
	return totalDiff, interval, nil
}

func (this *MongoServiceHub) GetDifficulty() (float64, error) {
	lastHeight := this.GetLastBlock()
	block, err := this.GetBlockInfoByHeight(lastHeight)
	if err != nil {
		return 0, err
	}
	return block.Difficulty, nil
}

func (this *MongoServiceHub) GetUnconfirmedTxInfo() (count uint64, size uint64, err error) {
	addrServiceCnt := len(this.addrService.mgoDb)
	count = 0
	size = 0
	err = nil
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		txCount, err := this.getMongoService(serviceIndex, COL_TX).Find(bson.M{
			"height": -1}).Count()
		if err != nil {
			return count, size, err
		}

		job := &mgo.MapReduce{
			Map:    "function() { emit(1, this.size) }",
			Reduce: "function(key, values) { return Array.sum(values) }",
		}

		var result []struct {
			One   int64
			Value int
		}
		_, err = this.getMongoService(serviceIndex, COL_TX).Find(bson.M{"height": -1}).MapReduce(job, &result)
		if err != nil {
			return count, size, err
		}
		txSize := 0
		if len(result) > 0 {
			txSize = result[0].Value
		}

		count += uint64(txCount)
		size += uint64(txSize)
	}

	return count, size, err
}

func (this *MongoServiceHub) GetAddrValue(hash string, voutIndex uint32) (addr string, value int64, err error) {
	tx := &TxBson{}
	index := this.getServiceIndex(hash)
	err = this.getMongoService(index, COL_TX_OUTPUT).Find(bson.M{
		"hash": hash}).One(tx)
	if err != nil {
		return "", 0, errors.New("tx not found")
	}

	if int(voutIndex) >= len(tx.VoutAddrValue) {
		return "", 0, errors.New("voutindex out of range")
	}

	addrvalue := tx.VoutAddrValue[voutIndex]
	arr := strings.Split(addrvalue, "@")
	addr = arr[0]
	fmt.Sscanf(arr[1], "%d", &value)
	return addr, value, nil
}

func (this *MongoServiceHub) GetBlockTime(height int64) (blockTime int64, err error) {
	block := &BlockVerboseBson{}
	err = this.getMongoService(0, COL_BLOCKHEADER).Find(bson.M{
		"height": height}).One(block)
	if err != nil {
		return 0, errors.New("block not found")
	}

	return block.Time, nil
}

func (this *MongoServiceHub) AddBlockHeader(verboseBlock *BlockVerboseBson) error {
	return this.getMongoService(0, COL_BLOCKHEADER).Insert(verboseBlock)
}

func (this *MongoServiceHub) RemoveBlockHeader(height int64) error {
	_, err := this.getMongoService(0, COL_BLOCKHEADER).RemoveAll(bson.M{
		"height": height})
	if err != nil {
		panic(err)
	}

	info, err := this.GetBlockInfoByHeight(height)
	if err != nil {
		panic(err)
	}

	addrServiceCnt := len(this.addrService.mgoDb)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		_, err = this.getMongoService(serviceIndex, COL_BLOCKTX).RemoveAll(bson.M{
			"blockhash": info.Hash})
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (this *MongoServiceHub) GetBlockHeaders(lastblock int64, headersMap map[int64]string) error {
	headers := make([]BlockVerboseBson, 0, 10)
	err := this.getMongoService(0, COL_BLOCKHEADER).
		Find(bson.M{"height": bson.M{"$gte": lastblock}}).
		Select(bson.M{"height": 1, "hash": 1}).
		Sort("height").
		All(&headers)
	if err != nil {
		return err
	}

	glog.Infof("GetBlockHeaders header size %d", len(headers))
	lastHeight := int64(0)
	removedHeights := make([]int64, 0, 10)
	for _, blockHeaderBson := range headers {
		if blockHeaderBson.Height < lastHeight {
			panic("sort failed")
		}
		if lastHeight != blockHeaderBson.Height-1 {
			glog.Infof("GetBlockHeaders header %d -> %d", lastHeight, blockHeaderBson.Height)
		}
		lastHeight = blockHeaderBson.Height

		_, ok := headersMap[blockHeaderBson.Height]
		if ok {
			err := this.RemoveBlockHeader(blockHeaderBson.Height)
			if err != nil {
				panic(err)
			}
			glog.Infof("GetBlockHeaders height %d already exists", blockHeaderBson.Height)
			removedHeights = append(removedHeights, blockHeaderBson.Height)
			continue
		}
		headersMap[blockHeaderBson.Height] = blockHeaderBson.Hash
	}

	for _, removedHeight := range removedHeights {
		delete(headersMap, removedHeight)
	}

	return nil
}

func (this *MongoServiceHub) AddHeightAddr(height int64, addr string) error {
	heightAddrBson := &HeightAddrBson{
		Height: height,
		Addr:   addr,
	}
	return this.getMongoService(0, COL_HEIGHTADDR).Insert(heightAddrBson)
}

func (p *MongoServiceHub) GetAddrTx(addr string) ([]AddrTxBson, error) {
	col := p.getMongoService(p.getServiceIndex(addr), COL_ADDRTX)
	var items []AddrTxBson
	err := col.Find(bson.M{"addr": addr}).All(&items)
	return items, err
}

func (this *MongoServiceHub) AddHeightHash(height int64, hash string, collection string) error {
	heightHashBson := &HeightHashBson{
		Height: height,
		Hash:   hash,
	}

	this.ConsumeRequest()
	return this.getMongoService(0, collection).Insert(heightHashBson)
}

func (this *MongoServiceHub) AddBlockHeaderPre(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_BLOCKHEADER_PRE)
}

func (this *MongoServiceHub) AddHeightHashInput(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_INPUT)
}

func (this *MongoServiceHub) AddHeightHashOutput(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_OUTPUT)
}

func (this *MongoServiceHub) AddHeightHashInventory(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_INVENTORY)
}

func (this *MongoServiceHub) AddHeightHashInputPre(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_INPUT_PRE)
}

func (this *MongoServiceHub) AddHeightHashOutputPre(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_OUTPUT_PRE)
}

func (this *MongoServiceHub) AddHeightHashInventoryPre(height int64, hash string) error {
	return this.AddHeightHash(height, hash, COL_HEIGHTHASH_INVENTORY_PRE)
}

func (this *MongoServiceHub) GetLastBlock() int64 {
	blockHeaderBson := &BlockVerboseBson{}
	err := this.getMongoService(0, COL_BLOCKHEADER).
		Find(nil).Sort("-height").Limit(1).One(blockHeaderBson)
	if err != nil {
		return 0
	}
	return blockHeaderBson.Height
}

func (this *MongoServiceHub) GetLastAddrBlock(collection string) int64 {
	heightHashBson := &HeightHashBson{}
	err := this.getMongoService(0, collection).
		Find(nil).Sort("-height").Limit(1).One(heightHashBson)
	if err != nil {
		return 0
	}
	return heightHashBson.Height
}

func (this *MongoServiceHub) GetLastBlockHeaderOffset() int64 {
	return this.GetLastAddrBlock(COL_BLOCKHEADER)
}

func (this *MongoServiceHub) GetLastBlockHeaderPreOffset() int64 {
	return this.GetLastAddrBlock(COL_BLOCKHEADER_PRE)
}

func (this *MongoServiceHub) GetLastInputOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_INPUT)
}

func (this *MongoServiceHub) GetLastOutputOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_OUTPUT)
}

func (this *MongoServiceHub) GetLastInventoryOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_INVENTORY)
}

func (this *MongoServiceHub) GetLastInputPreOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_INPUT_PRE)
}

func (this *MongoServiceHub) GetLastOutputPreOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_OUTPUT_PRE)
}

func (this *MongoServiceHub) GetLastInventoryPreOffset() int64 {
	return this.GetLastAddrBlock(COL_HEIGHTHASH_INVENTORY_PRE)
}

func (this *MongoServiceHub) GetHeightHashs(lastblock int64, hashsMap map[int64]string, collection string) error {
	hashs := make([]HeightHashBson, 0, 10)
	err := this.getMongoService(0, collection).
		Find(bson.M{"height": bson.M{"$gte": lastblock}}).
		Sort("height").
		All(&hashs)
	if err != nil {
		return err
	}

	lastHeight := int64(0)
	for _, heightHashBson := range hashs {
		if heightHashBson.Height < lastHeight {
			panic("sort failed")
		}

		if lastHeight != heightHashBson.Height-1 && lastHeight != 0 {
			glog.Infof("GetHeightHashs header %s %d -> %d", collection, lastHeight, heightHashBson.Height)
		}

		lastHeight = heightHashBson.Height
		_, ok := hashsMap[heightHashBson.Height]
		if ok {
			glog.Infof("GetHeightHashs height %s %d already exists", collection, heightHashBson.Height)
			continue
		}

		hashsMap[heightHashBson.Height] = heightHashBson.Hash
	}

	return nil
}

func (this *MongoServiceHub) GetBlockHeaderPre(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_BLOCKHEADER_PRE)
}

func (this *MongoServiceHub) GetHeightHashInput(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_INPUT)
}

func (this *MongoServiceHub) GetHeightHashInventory(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_INVENTORY)
}

func (this *MongoServiceHub) GetHeightHashOutput(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_OUTPUT)
}

func (this *MongoServiceHub) GetHeightHashInputPre(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_INPUT_PRE)
}

func (this *MongoServiceHub) GetHeightHashInventoryPre(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_INVENTORY_PRE)
}

func (this *MongoServiceHub) GetHeightHashOutputPre(lastblock int64, hashsMap map[int64]string) error {
	return this.GetHeightHashs(lastblock, hashsMap, COL_HEIGHTHASH_OUTPUT_PRE)
}

func (this *MongoServiceHub) getServiceIndex(addr string) int {
	//addrIndex := AddrIndex{}
	//glbServiceIndexLock.Lock()
	//defer glbServiceIndexLock.Unlock()
	//err := this.getMongoService(0, COL_ADDRINDEX).Find(bson.M{
	//	"addr": addr,
	//}).One(&addrIndex)
	//if err != nil {
	//	i := int(addr[len(addr)-1])
	//	serverCnt := len(this.addrService.mgoDb)
	//	i = i % serverCnt
	//	addrIndexNew := AddrIndex{
	//		Addr:  addr,
	//		Index: i,
	//	}
	//	err = this.getMongoService(0, COL_ADDRINDEX).Insert(addrIndexNew)
	//	if err != nil {
	//		panic(err)
	//	}
	//	return i
	//}
	//
	//return addrIndex.Index
	serverCnt := len(this.addrService.mgoDb)
	i := 0
	if len(addr) > 0 {
		i = int(addr[len(addr)-1])
	}

	i = i % serverCnt
	return i
}

func (this *MongoServiceHub) AddAddrTx(addr string, height int64, txid string, index int, value int64,
	prehash string, preindex int, sigscript []byte) error {
	realvalue := value

	isinput := true
	if prehash == "" {
		isinput = false
	}

	if isinput {
		realvalue = -realvalue
	}
	addrTxBson := &AddrTxBson{
		Addr:      addr,
		Height:    height,
		Txid:      txid,
		Index:     index,
		Value:     realvalue,
		IsInput:   isinput,
		Prehash:   prehash,
		Preindex:  preindex,
		SigScript: sigscript,
	}
	return this.AddBulk(addr, COL_ADDRTX, height, addrTxBson)
}

func (this *MongoServiceHub) ClearAddrTx(height int64, isinput bool) error {
	addrServiceCnt := len(this.addrService.mgoDb)
	txCollection := COL_TX
	if !isinput {
		txCollection = COL_TX_OUTPUT
	}
	glog.Info("ClearAddrTx ", height, " ", isinput, " ", addrServiceCnt)
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		if isinput {
			this.getMongoService(serviceIndex, COL_ADDRTX).RemoveAll(bson.M{
				"height": height})
		}

		this.getMongoService(serviceIndex, txCollection).RemoveAll(bson.M{
			"height": height})
	}
	return nil
}

func (this *MongoServiceHub) AddErrorTx(hash string, index int, prehash string, preindex int) {
	txError := &TxError{
		Hash:     hash,
		Index:    index,
		PreHash:  prehash,
		PreIndex: preindex,
	}
	this.getMongoService(0, COL_HEIGHTADDR).Insert(txError)
}

func (this *MongoServiceHub) CheckEmptyVout(height int64) {
	addrServiceCnt := len(this.addrService.mgoDb)
	found := false
	for serviceIndex := 0; serviceIndex < addrServiceCnt; serviceIndex++ {
		cnt, err := this.getMongoService(serviceIndex, COL_ADDRTX).Find(bson.M{
			"height": height, "isinput": false}).Count()
		if err != nil {
			glog.Infof("height %d count failed %s", height, err.Error())
			glog.Flush()
			continue
		}

		if cnt != 0 {
			found = true
			break
		}
	}

	if !found {
		glog.Infof("height %d not found", height)
		glog.Flush()
	}
}

func (this *MongoServiceHub) CheckOwnerid(ownerid int) {
	glog.Info("CheckOwnerid 1")
	monitoredAddrBson := &MonitoredAddrBson{}
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{
		"ownerid": ownerid}).Iter()
	cnt, err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{
		"ownerid": ownerid}).Count()
	if err != nil {
		glog.Infof("GetAddrUnspentInfo %d failed", ownerid)
		return
	}
	glog.Info("CheckOwnerid 2 ", cnt)
	for iter.Next(monitoredAddrBson) {
		glog.Info("CheckOwnerid 2a ", monitoredAddrBson.Addr)
		glog.Flush()
		rst, err := this.GetAddrUnspentInfo(monitoredAddrBson.Addr)
		if err != nil {
			glog.Infof("GetAddrUnspentInfo %s failed", monitoredAddrBson.Addr)
			continue
		}

		balance := int64(9988776655544)
		for j := 0; j < 10; j++ {
			cmdstr := "curl"
			argstr := "https://api.blockchair.com/bitcoin-sv/dashboards/address/" + monitoredAddrBson.Addr
			var buf []byte
			for i := 0; i < 10; i++ {
				cmd := exec.Command(cmdstr, argstr)
				buf, err = cmd.Output()
				if err == nil {
					break
				}
				glog.Infof("GetAddrUnspentInfo i=%d %s failed %s", i, monitoredAddrBson.Addr, err.Error())
			}

			js, err := simplejson.NewJson(buf)
			if err != nil {
				glog.Infof("GetAddrUnspentInfo %s failed %s",
					monitoredAddrBson.Addr, err.Error())
				break
			}

			balance, err = js.
				Get("data").
				Get(monitoredAddrBson.Addr).
				Get("address").
				Get("balance").Int64()
			if err != nil {
				balanceint, err := js.
					Get("data").
					Get(monitoredAddrBson.Addr).
					Get("address").
					Get("balance").Int()
				if err != nil {
					//glog.Infof("GetAddrUnspentInfo %s failed %s %d",
					//	monitoredAddrBson.Addr, err.Error(), rst.Balance)
					time.Sleep(1 * time.Second)
					continue
				}
				balance = int64(balanceint)
			}
			break
		}

		if balance != rst.Balance {
			glog.Infof("CheckOwnerid %s %d %d", monitoredAddrBson.Addr, balance, rst.Balance)
		}
	}
	glog.Flush()
}

func (this *MongoServiceHub) GetAddrUnspentInfo(addr string) (*AddrUnspentInfo, error) {
	rst := &AddrUnspentInfo{}
	items, err := this.GetAddrTx(addr)
	if err != nil {
		return rst, err
	}

	rst.Address = addr
	rst.TotalReceived = 0
	rst.TotalSent = 0
	txMap := make(map[string]int)
	for _, item := range items {
		if item.IsInput {
			rst.TotalSent -= item.Value
		} else {
			rst.TotalReceived += item.Value
		}
		txMap[item.Txid] = 0
	}

	rst.Balance = rst.TotalReceived - rst.TotalSent
	rst.TxApperances = len(txMap)
	return rst, nil
}

func (this *MongoServiceHub) GetCrawlCountInfo() *CrawlCountInfo {
	crawlCountInfo := &CrawlCountInfo{}
	n, err := this.getMongoService(0, COL_ADDRTX).Count()
	if err == nil {
		crawlCountInfo.AddrTxCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_BLOCKHEADER).Count()
	if err == nil {
		crawlCountInfo.BlockHeaderCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_BLOCKHEADER_PRE).Count()
	if err == nil {
		crawlCountInfo.BlockHeaderPreCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_BLOCKTX).Count()
	if err == nil {
		crawlCountInfo.BlockTxCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_HEIGHTADDR).Count()
	if err == nil {
		crawlCountInfo.HeightAddrCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_HEIGHTHASH_INPUT).Count()
	if err == nil {
		crawlCountInfo.HeightHashInputCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_HEIGHTHASH_INPUT_PRE).Count()
	if err == nil {
		crawlCountInfo.HeightHashInputPreCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_HEIGHTHASH_OUTPUT).Count()
	if err == nil {
		crawlCountInfo.HeightHashOutputCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_HEIGHTHASH_OUTPUT_PRE).Count()
	if err == nil {
		crawlCountInfo.HeightHashOutputPreCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_MONITORED_ADDR).Count()
	if err == nil {
		crawlCountInfo.MonitoredAddrCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Count()
	if err == nil {
		crawlCountInfo.MonitoredAddrInventoryCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_TX).Count()
	if err == nil {
		crawlCountInfo.TxCount = int64(n)
	}
	n, err = this.getMongoService(0, COL_TX_OUTPUT).Count()
	if err == nil {
		crawlCountInfo.TxOutputCount = int64(n)
	}

	return crawlCountInfo
}

func (this *MongoServiceHub) removeBlockInfo(height int64, col string) {
	_, err := this.getMongoService(0, col).RemoveAll(bson.M{"height": bson.M{"$gte": height}})
	if err != nil {
		glog.Infof("clear "+col+" %d false ", height)
		panic(err)
	}
}

func (this *MongoServiceHub) RemoveBlockHeaderInfo(height int64) {
	this.removeBlockInfo(height, COL_BLOCKHEADER)
}
func (this *MongoServiceHub) RemoveOutputInfo(height int64) {
	this.removeBlockInfo(height, COL_HEIGHTHASH_OUTPUT)
}
func (this *MongoServiceHub) RemoveInputInfo(height int64) {
	this.removeBlockInfo(height, COL_HEIGHTHASH_INPUT)
}
func (this *MongoServiceHub) GetHashFromBlockHeader(height int64) (string, error) {
	heightHashBson := HeightHashBson{}
	err := this.getMongoService(0, COL_BLOCKHEADER).
		Find(bson.M{"height": height}).
		Select(bson.M{"hash": 1, "height": 1}).
		One(&heightHashBson)
	return heightHashBson.Hash, err
}
func (this *MongoServiceHub) GetHashFromOutput(height int64) (string, error) {
	heightHashBson := HeightHashBson{}
	err := this.getMongoService(0, COL_HEIGHTHASH_OUTPUT).
		Find(bson.M{"height": height}).
		Select(bson.M{"hash": 1, "height": 1}).
		One(&heightHashBson)
	return heightHashBson.Hash, err
}

func (this *MongoServiceHub) GetMonitoredAddrIter() *mgo.Iter {
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Iter()
	return iter
}

func (this *MongoServiceHub) GetAllMonitoredAddr(start int, end int) []*MonitoredAddrBson {
	monitoredAddrBsons := make([]*MonitoredAddrBson, 0, 10)
	err := this.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Skip(start).Limit(end - start).All(&monitoredAddrBsons)
	if err != nil {
		glog.Infof("GetAllMonitoredAddr False:%s", err)
		panic(err)
	}
	return monitoredAddrBsons
}

func (this *MongoServiceHub) GetAllMonitoredAddrCount() int {
	count, err := this.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Count()
	if err != nil {
		glog.Infof("GetAllMonitoredAddr False:%s", err)
		panic(err)
	}
	return count
}

func (this *MongoServiceHub) AddMonitoredAddrs(monitoredAddrBsons []*MonitoredAddrBson) error {
	// _, err := this.getMongoService(0, COL_SYN_MONITORED_ADDR).Upsert(bson.M{"addr": addr}, bson.M{"appid": appid, "ownerid": ownerid, "addr": addr, bdid: true})
	bulk := this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
	//保险起见，先用UpSert
	for _, monitoredAddrBson := range monitoredAddrBsons {
		monitoredAddrBson.ScrawledAt = -1
		bulk.Upsert(bson.M{"addr": monitoredAddrBson.Addr},
			monitoredAddrBson)
	}
	_, err := bulk.Run()
	if err != nil {
		return err
	}
	return nil
}

func (this *MongoServiceHub) GetAddrBalanceFromBlockChair(addr string) (int64, error) {
	for {
		cmdstr := "curl"
		argstr := "https://api.blockchair.com/bitcoin-sv/dashboards/address/" + addr
		cmd := exec.Command(cmdstr, argstr)
		buf, err := cmd.Output()
		if err != nil {
			//休息一秒钟，重新运行一次
			time.Sleep(time.Second)
			continue
		}
		js, err := simplejson.NewJson(buf)
		if err != nil {
			//休息一秒钟，重新运行一次
			time.Sleep(time.Second)
			continue
		}
		balance, err := js.
			Get("data").
			Get(addr).
			Get("address").
			Get("balance").Int64()
		if err != nil {
			balanceint, err := js.
				Get("data").
				Get(addr).
				Get("address").
				Get("balance").Int()
			if err != nil {
				//休息一秒钟，重新运行一次
				time.Sleep(time.Second)
				continue
			}
			balance = int64(balanceint)
		}
		return balance, nil
	}
}

func (this *MongoServiceHub) GetLocalMonitoredAddrMap() map[string]*MonitoredAddrBson {
	return this.getMonitoredAddrMap(COL_MONITORED_ADDR)
}

func (this *MongoServiceHub) GetSynCenterMonitoredAddrMap() map[string]*MonitoredAddrBson {
	return this.getMonitoredAddrMap(COL_SYNC_MONITORED_ADDR)
}

func (this *MongoServiceHub) getMonitoredAddrMap(collection string) map[string]*MonitoredAddrBson {
	//获取用于同步的Mongodb的内容
	iter := this.getMongoService(0, collection).Find(nil).Iter()
	monitoredAddrBson := new(MonitoredAddrBson)
	monitoredAddrBsons := make(map[string]*MonitoredAddrBson)
	for iter.Next(monitoredAddrBson) {
		monitoredAddrBsons[monitoredAddrBson.Addr] = monitoredAddrBson
		//更新MonitoredAddr指针
		monitoredAddrBson = new(MonitoredAddrBson)
	}
	return monitoredAddrBsons
}

func (this *MongoServiceHub) GetLackMonitoredAddr(bitdbID string, step int) *[]*MonitoredAddrBson {
	monitoredAddrBsons := new([]*MonitoredAddrBson)
	err := this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Find(bson.M{bitdbID: bson.M{"$ne": true}}).Limit(step).All(monitoredAddrBsons)
	if err != nil {
		panic(err)
	}
	return monitoredAddrBsons
}

func (this *MongoServiceHub) GetMonitoredAddrCount(bitdbID string) (int, error) {
	count, err := this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Find(bson.M{bitdbID: bson.M{"$ne": true}}).Count()
	return count, err
}

func (this *MongoServiceHub) PutMonitoredAddr(appid string, ownerid int, addr string, bdid string) error {
	_, err := this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Upsert(bson.M{"addr": addr}, bson.M{"appid": appid, "ownerid": ownerid, "addr": addr, bdid: true})
	return err
}

func (this *MongoServiceHub) SyncMonitoredAddrs(local map[string]*MonitoredAddrBson, syncCenter map[string]*MonitoredAddrBson) error {
	//检查中心的内容本地有没有
	for addr, monitoredAddrBson := range syncCenter {
		_, ok := local[addr]
		if !ok {
			//本地没有,向本地插入
			monitoredAddrBson.ScrawledAt = -1
			monitoredAddrBson.UpdateAt = this.GetLastInputOffset()
			err := this.getMongoService(0, COL_MONITORED_ADDR).Insert(monitoredAddrBson)
			if err != nil {
				return err
			}
		}
	}
	//检查本地的内容中心有没有
	for addr, monitoredAddrBson := range local {
		_, ok := syncCenter[addr]
		if !ok {
			//中心没有,向中心插入
			err := this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Insert(
				bson.M{"appid": monitoredAddrBson.Appid, "ownerid": monitoredAddrBson.Ownerid, "addr": monitoredAddrBson.Addr})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (this *MongoServiceHub) FillMonitoredAddrs(monitoredAddrBsons map[string]*MonitoredAddrBson) error {
	if len(monitoredAddrBsons) == 0 {
		return nil
	}
	bulk := this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
	//此处的monitoredAddrBson不需要更新
	for _, monitoredAddrBson := range monitoredAddrBsons {
		monitoredAddrBson.ScrawledAt = -1
		monitoredAddrBson.UpdateAt = this.GetLastInputOffset()
		//保险起见，先用Upsert
		bulk.Upsert(bson.M{"addr": monitoredAddrBson.Addr}, monitoredAddrBson)
		if bulk.Opcount() > 4000 {
			_, err := bulk.Run()
			if err != nil {
				return err
			}
			bulk = this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
		}
	}
	_, err := bulk.Run()
	return err
}

func (this *MongoServiceHub) CopyMonitoredAddr(source *MongoServiceHub) {
	//1.首先从原数据库获取
	monitoredAddrBson := &MonitoredAddrBson{}
	iter := source.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Iter()
	//2.先生成一个bulk
	bulk := this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
	for iter.Next(monitoredAddrBson) {
		bulk.Insert(monitoredAddrBson)
		//更新指针
		monitoredAddrBson = &MonitoredAddrBson{}
		if bulk.Opcount() > 300 {
			//3.每300个插入一次
			bulk.Run()
			//4.重新生成一个bulk
			bulk = this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
		}
	}
	//5.最后将不足300的消费掉
	bulk.Run()
}

func (this *MongoServiceHub) CopyMonitoredInvertory(source *MongoServiceHub) {
	//1.首先从原数据库获取
	addrTxBson := &MonitoredAddrTxBson{}
	iter := source.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Find(nil).Iter()
	//2.先生成一个bulk
	bulk := this.getMongoService(0, COL_MONITORED_ADDR_INVENTORY).Bulk()
	for iter.Next(addrTxBson) {
		bulk.Insert(addrTxBson)
		//更新指针
		addrTxBson = &MonitoredAddrTxBson{}
		if bulk.Opcount() > 300 {
			//3.每300个插入一次
			bulk.Run()
			//4.重新生成一个bulk
			bulk = this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
		}
	}
	//5.最后将不足300的消费掉
	bulk.Run()
}

func (this *MongoServiceHub) CompareMonitoredAddrAndInsert(source *MongoServiceHub) {
	//1.首先从原数据库获取
	monitoredAddrBson := &MonitoredAddrBson{}
	iter := source.getMongoService(0, COL_MONITORED_ADDR).Find(nil).Iter()
	//2.先生成一个bulk
	bulk := this.getMongoService(0, COL_MONITORED_ADDR).Bulk()
	tatalCount := 0
	for iter.Next(monitoredAddrBson) {
		//3.检查新的库中是否有这个内容
		count, err := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"addr": monitoredAddrBson.Addr}).Count()
		if err != nil {
			glog.Infof("CompareMonitoredAddrAndInsert:%s", err)
			panic(err)
		}
		tatalCount = count + tatalCount
		//4.如果count>1，则再复制的过程中出错
		if count > 1 {
			glog.Infof("有地址有重复记录：%s", monitoredAddrBson.Addr)
			continue
		}
		if count == 0 {
			bulk.Insert(monitoredAddrBson)
			//更新指针
			monitoredAddrBson = &MonitoredAddrBson{}
		}

	}
	//5.执行插入操作
	glog.Infof("总共缺少%d条", bulk.Opcount())
	bulk.Run()
}

func (this *MongoServiceHub) GetMonitoredAppidOwnerid() []*MonitoredAppidOwnerid {
	m := []bson.M{
		{"$group": bson.M{"_id": bson.M{"appid": "$appid", "ownerid": "$ownerid"}}},
	}
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Pipe(m).Iter()
	resultMap := make(map[string]*MonitoredAppidOwnerid)
	monitoredAppidOwnerids := make([]*MonitoredAppidOwnerid, 0, 10)
	for iter.Next(resultMap) {
		monitoredAppidOwnerid, ok := resultMap["_id"]
		if !ok {
			glog.Infof("GetMonitoredAppidOwnerid resultMap does not contain _id")
			continue
		}
		monitoredAppidOwnerids = append(monitoredAppidOwnerids, monitoredAppidOwnerid)
	}
	return monitoredAppidOwnerids
}

func (this *MongoServiceHub) UpsertBalance(Appid string, Ownerid int, balance int64) error {
	_, err := this.getMongoService(0, COL_MONITORED_BALANCE).Upsert(bson.M{"ownerid": Ownerid},
		bson.M{"appid": Appid, "ownerid": Ownerid, "balance": balance})
	return err
}

func (this *MongoServiceHub) GetBalanceTopTen() ([]*MonitoredAppidOwneridBalance, error) {
	monitoredAppidOwneridBalance := make([]*MonitoredAppidOwneridBalance, 0, 10)
	err := this.getMongoService(0, COL_MONITORED_BALANCE).Find(nil).Sort("-balance").Limit(10).All(&monitoredAppidOwneridBalance)
	return monitoredAppidOwneridBalance, err
}

func (this *MongoServiceHub) GetOwnerCount() (int, error) {
	count, err := this.getMongoService(0, COL_MONITORED_BALANCE).Count()
	return count, err
}

func (this *MongoServiceHub) GetTotalBalance() (int64, error) {
	m := []bson.M{
		{"$group": bson.M{"_id": nil, "balance": bson.M{"$sum": "$balance"}}},
	}
	resultMap := make(map[string]int64)
	err := this.getMongoService(0, COL_MONITORED_BALANCE).Pipe(m).One(resultMap)
	if err != nil {
		return 0, err
	}
	balance, ok := resultMap["balance"]
	if !ok {
		return 0, errors.New("Result Map does not contain balance")
	}
	return balance, nil
}

func (this *MongoServiceHub) GetMonitoredDailyInfo() (*MonitoredDailyInfo, error) {
	messageMap := make(map[string]interface{}, 3)
	//1.获取总余额
	totalBalance, err := this.GetTotalBalance()
	if err != nil {
		glog.Infof("doNotify GetTotalBalance:%s", err)
		return nil, err
	}
	messageMap["totalBalance"] = totalBalance
	//2.获取前十名
	monitoredAppidOwneridBalances, err := this.GetBalanceTopTen()
	if err != nil {
		glog.Infof("doNotify GetBalanceTopTen:%s", err)
		return nil, err
	}
	messageMap["balancesTopTen"] = monitoredAppidOwneridBalances
	//3.获取用户总数
	count, err := this.GetOwnerCount()
	if err != nil {
		glog.Infof("doNotify monitoredAppidOwneridBalances:%s", err)
		return nil, err
	}
	messageMap["ownerCount"] = count
	monitoredDailyInfo := new(MonitoredDailyInfo)
	monitoredDailyInfo.TotalBalance = totalBalance
	monitoredDailyInfo.MonitoredAppidOwneridBalances = monitoredAppidOwneridBalances
	monitoredDailyInfo.OwnerCount = count
	return monitoredDailyInfo, nil
}

func (this *MongoServiceHub) CleanUnconfirmTx() {
	serverCnt := len(this.addrService.mgoDb)
	tx := &TxBson{}
	//对每一个数据库遍历过去
	for serviceIndex := 0; serviceIndex < serverCnt; serviceIndex++ {
		iter := this.getMongoService(serviceIndex, COL_TX).Find(bson.M{"height": -1}).Iter()
		for iter.Next(tx) {
			//如果至少两条，证明有重复的
			count, err := this.getMongoService(serviceIndex, COL_TX).Find(bson.M{"hash": tx.Hash}).Count()
			if err != nil {
				glog.Infof("CleanUnconfirmTx %s", err)
			}
			if count > 1 {
				//开始删除两张表，一张tx，一张addr_tx
				_, err := this.getMongoService(serviceIndex, COL_TX).RemoveAll(bson.M{
					"height": -1, "hash": tx.Hash})
				if err != nil {
					panic(err)
				}
				_, err = this.getMongoService(serviceIndex, COL_ADDRTX).RemoveAll(bson.M{
					"height": -1, "txid": tx.Hash})
				if err != nil {
					panic(err)
				}
			}
		}
	}
}

func (this *MongoServiceHub) GetOwnerAddrIter(ownerid int) *mgo.Iter {
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{
		"ownerid": ownerid}).Iter()
	return iter
}

type AppOwner interface {
	Run(string, int)
}

func (this *MongoServiceHub) ForeachAppOwner(appOwner AppOwner) {
	m := []bson.M{
		{"$group": bson.M{"_id": bson.M{"appid": "$appid", "ownerid": "$ownerid"}}},
	}
	//用来计算数量的
	count := 0
	iter := this.getMongoService(0, COL_SYNC_MONITORED_ADDR).Pipe(m).Iter()
	resultMap := make(map[string]*MonitoredAppidOwnerid)
	for iter.Next(resultMap) {
		monitoredAppidOwnerid, ok := resultMap["_id"]
		if !ok {
			glog.Infof("GetMonitoredAppidOwnerid resultMap does not contain _id")
			continue
		}
		appOwner.Run(monitoredAppidOwnerid.Appid, monitoredAppidOwnerid.Ownerid)
		count++
		if count%1000 == 0 {
			glog.Infof("ForeachAppOwner %d", count)
		}
	}
}

func (this *MongoServiceHub) CountOwnerNubmer(appid string) int {
	m := []bson.M{
		{"$match": bson.M{"appid": appid}},
		{"$group": bson.M{"_id": bson.M{"ownerid": "$ownerid"}}},
		{"$group": bson.M{"_id": nil, "count": bson.M{"$sum": 1}}}}

	test := make(map[string]int)
	err := this.getMongoService(0, COL_MONITORED_ADDR).Pipe(m).One(&test)
	if err != nil {
		glog.Infof("CountOwnerNubmer %s", err)
		return -1
	}
	count, ok := test["count"]
	if !ok {
		return -1
	}
	return count
}
