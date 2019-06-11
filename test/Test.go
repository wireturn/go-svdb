package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	_ "github.com/btcsuite/btcutil"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"mempool.com/foundation/bitdb/common"
)

//zmq "github.com/pebbe/zmq4"
type person struct {
	Name string
}

func (this person) say() {
	fmt.Printf("i am %s\n", this.Name)
}

//“继承”person
type teacher struct {
	person //这个叫匿名字段,其实就是个名字，不用在意
}

//我不用又去写一个say方法

type TestMap map[int]int

func test1() {
	p := person{"trump"}
	p.say()
	t := teacher{person: person{"trump's teacher"}}
	t.say()

	addr := "1EzhNyzKNAA2QdYy5NcjxS9KUrs8ak6ULo"
	c := addr[len(addr)-1]
	fmt.Println(c)
	fmt.Println(int(c))

	testmap := make(TestMap)
	testmap[1] = 1
	val1, _ := testmap[1]
	val1 += 3
	val2, _ := testmap[1]
	fmt.Println(val2)

	session, err := mgo.Dial("192.168.1.17:27013")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("height_hash").Count()
	fmt.Println("cnt:", cnt)

	it := db.C("height_hash").Find(nil).Sort("height").Iter()
	heightHashBson := &common.HeightHashBson{}
	last := int64(-1)
	for it.Next(heightHashBson) {
		if heightHashBson.Height != last+1 {
			fmt.Printf("%d -> %d\n", last, heightHashBson.Height)
		}
		last = heightHashBson.Height
	}

	//arr := []HeightHashBson{}
	//db.C("height_hash").Find(nil).Sort("height").All(&arr)
	//for _, cursor := range arr {
	//	fmt.Printf("%d\n", cursor.Height)
	//}
}

func test2() {
	connCfg := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	hash, err := client.GetBlockHash(556030)
	if err != nil {
		panic(err)
	}

	verboseBlock, err := client.GetBlockVerbose(hash)
	if err != nil {
		panic(err)
	}

	if len(verboseBlock.Tx) > 20 {
		verboseBlock.Tx = verboseBlock.Tx[:20]
	}
	if len(verboseBlock.RawTx) > 20 {
		verboseBlock.RawTx = verboseBlock.RawTx[:20]
	}
	session, err := mgo.Dial("192.168.1.17:27013")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	db := session.DB("bitdb_" + "BSV")
	err = db.C("verbose_header").Insert(verboseBlock)
	if err != nil {
		panic(err)
	}
}

func test3() {
	connCfg := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	hash, err := client.GetBlockHash(556030)
	if err != nil {
		panic(err)
	}

	block, err := client.GetBlock(hash)
	if err != nil {
		panic(err)
	}

	tx := block.Transactions[0].TxIn[0]
	bytes := tx.SignatureScript
	fmt.Println(string(bytes))

	txhash, err := chainhash.NewHashFromStr("75809efad4f1b79f0acbdd8ad2a3ad09166171a6038e04d0062162db1b16dbe4")
	if err != nil {
		panic(err)
	}
	txverbose, err := client.GetRawTransactionVerbose(txhash)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", txverbose.Blocktime)
	fmt.Printf("%d\n", txverbose.Time)

}

func test4() {
	session, err := mgo.Dial("192.168.1.17:27010")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("block_header").Count()
	fmt.Println("cnt:", cnt)

	it := db.C("block_header").Find(nil).Sort("height").Iter()
	blockVerboseBson := &common.BlockVerboseBson{}
	last := int64(-1)
	for it.Next(blockVerboseBson) {
		if blockVerboseBson.Height != last+1 {
			fmt.Printf("%d -> %d\n", last, blockVerboseBson.Height)
		}
		last = blockVerboseBson.Height
	}
}

func test5() {
	connCfg := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	hash, _ := chainhash.NewHashFromStr("0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9")
	time1 := time.Now()
	for i := 0; i < 100; i++ {
		_, err := client.GetDifficulty()
		if err != nil {
			panic(err)
		}
	}
	d1 := time.Since(time1)

	time2 := time.Now()
	for i := 0; i < 100; i++ {
		_, err := client.GetRawTransaction(hash)
		if err != nil {
			panic(err)
		}
	}
	d2 := time.Since(time2)
	fmt.Println(d1)
	fmt.Println(d2)
}

func test6() {
	var addr string
	var value int64
	s := "0@12345678"
	arr := strings.Split(s, "@")
	addr = arr[0]
	fmt.Sscanf(arr[1], "%d", &value)
	fmt.Println(addr)
	fmt.Println(value)
}

func test7() {
	session, err := mgo.Dial("192.168.1.17:27012")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(nil)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("testaddrtx3").Count()
	fmt.Println("cnt:", cnt)

	time1 := time.Now()
	tmpdb := db.C("testaddrtx3")
	for i := 0; i < 10000000; i++ {
		addr := fmt.Sprintf("%d8db9408d5ccf8c349019493ef8ad45a9ca47475841aa9527de005a905e616633", i)
		addrTx := &common.AddrTxBson{
			Addr:     addr,
			Height:   0,
			Index:    i,
			Value:    50,
			Txid:     "txid1",
			IsInput:  false,
			Prehash:  "prehash1",
			Preindex: 1,
		}
		err := tmpdb.Insert(addrTx, addrTx, addrTx, addrTx, addrTx, addrTx, addrTx, addrTx, addrTx, addrTx)
		if err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			d := time.Since(time1)
			time1 = time.Now()
			cnt, _ := db.C("testaddrtx3").Count()
			fmt.Printf("cnt:%d d:%f\n", cnt, d.Seconds())
		}
	}
}

func test8() {
	time1 := time.Now()
	for i := 0; i < 1000; i++ {
		time.Now()
	}
	//time.Sleep(time.Duration(1) * time.Second)
	d := time.Since(time1)
	fmt.Println("d ", d.Nanoseconds())
}

func test9() {
	session, err := mgo.Dial("192.168.1.17:27012")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("testaddrtx3").Count()
	fmt.Println("cnt:", cnt)

	time1 := time.Now()
	for j := 0; j < 10; j++ {
		func() {
			for i := 0; i < 1000000; i++ {
				addr := fmt.Sprintf("%d8db9408d5ccf8c349019493ef8ad45a9ca47475841aa9527de005a905e616633", i)
				addrTx := &common.AddrTxBson{}
				db.C("testaddrtx3").Find(bson.M{
					"addr": addr}).One(addrTx)
				if i%1000 == 0 {
					d := time.Since(time1)
					time1 = time.Now()
					cnt, _ := db.C("testaddrtx3").Count()
					fmt.Printf("read cnt:%d d:%f\n", cnt, d.Seconds())
				}
			}
		}()
	}
	for {
		time.Sleep(time.Duration(5) * time.Second)
	}
}

func test10() {
	session, err := mgo.Dial("192.168.1.17:27012")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(nil)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("testaddrtx3").Count()
	fmt.Println("cnt:", cnt)

	time1 := time.Now()
	tmpdb := db.C("testaddrtx3")
	buck := tmpdb.Bulk()
	for i := 0; i < 1000; i++ {
		addr := fmt.Sprintf("%d8db9408d5ccf8c349019493ef8ad45a9ca47475841aa9527de005a905e616633", i)
		addrTx := &common.AddrTxBson{
			Addr:     addr,
			Height:   0,
			Index:    i,
			Value:    50,
			Txid:     "txid1",
			IsInput:  false,
			Prehash:  "prehash1",
			Preindex: 1,
		}
		buck.Insert(addrTx)
	}
	for j := 0; j < 10000; j++ {
		_, err = buck.Run()
		buck.RemoveAll()
		if err != nil {
			panic(err)
		}
		d := time.Since(time1)
		time1 = time.Now()
		fmt.Printf("read cnt:%d d:%f\n", 0, d.Seconds())
		cnt, _ = db.C("testaddrtx3").Count()
		fmt.Println("cnt:", cnt)
	}
}

func test11() {
	fmt.Println("test11 1")
	connCfg1 := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client1, err := rpcclient.New(connCfg1, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("test11 2")
	//connCfg2 := &rpcclient.ConnConfig{
	//	User:         "toorq2000",
	//	Pass:         "helloworldthisisafuckingtestingforbch2000",
	//	Host:         "192.168.1.13:8332",
	//	HTTPPostMode: true,
	//	DisableTLS:   true,
	//}
	//client2, err := rpcclient.New(connCfg2, nil)
	//if err != nil {
	//	panic(err)
	//}
	fmt.Println("test11 3")

	for j := 0; j < 10; j++ {
		go func() {
			time1 := time.Now()
			for i := 200000; i < 200100; i++ {
				client := client1
				if i%2 == 0 {
					client = client1
				}

				hash, err := client.GetBlockHash(int64(i))
				if err != nil {
					panic(err)
				}

				block, err := client.GetBlock(hash)
				if err != nil {
					panic(err)
				}

				txhash := block.Transactions[0].TxHash()
				tx, err := client.GetRawTransaction(&txhash)
				if err != nil {
					panic(err)
				}

				fmt.Printf("i:%d tx:%s\n", i, tx.Hash().String())
			}
			d1 := time.Since(time1)
			fmt.Println(d1)
		}()
	}

	time.Sleep(time.Duration(100000) * time.Second)
}

func xx() {
	fragment := common.StartTrace("xx")
	time.Sleep(time.Duration(2) * time.Second)
	fragment.StopTrace(0)
}
func test12() {
	common.GetStatisticInstance().Init(true, 5)
	for i := 0; i < 30; i++ {
		fragment := common.StartTrace("test 12 a")
		xx()
		fragment.StopTrace(0)

		fragment = common.StartTrace("test 12 b")
		fragment.StopTrace(0)
	}
}

func test13() {
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

	connCfg1 := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.13:8332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client1, err := rpcclient.New(connCfg1, nil)
	if err != nil {
		panic(err)
	}

	txcnt := 278804081
	inputcnt := 679773457
	outputcnt := 735834519
	for i := 562250; i < 570000; i++ {
		hash, err := client1.GetBlockHash(int64(i))
		if err != nil {
			panic(err)
		}
		block, err := client1.GetBlock(hash)
		if err != nil {
			panic(err)
		}

		txcnt += len(block.Transactions)
		for _, tx := range block.Transactions {
			inputcnt += len(tx.TxIn)
			outputcnt += len(tx.TxOut)
		}

		//if i % 10 == 0 {
		glog.Infof("height %d %d %d %d", i, txcnt, inputcnt, outputcnt)
		//}
	}
}

// func testZmq() {
// 	subscriber, err := zmq.NewSocket(zmq.SUB)
// 	if nil != err {
// 		panic(err)
// 	}
// 	defer subscriber.Close()
// 	err = subscriber.Connect("tcp://192.168.1.9:18331")
// 	if nil != err {
// 		panic(err)
// 	}
// 	err = subscriber.SetSubscribe("rawtx")
// 	if nil != err {
// 		panic(err)
// 	}
// 	err = subscriber.SetRcvtimeo(300 * time.Minute)
// 	if nil != err {
// 		panic(err)
// 	}

// 	for {
// 		_, err := subscriber.Recv(0)
// 		if nil != err {
// 			panic(err)
// 		}
// 		txByte, err := subscriber.RecvBytes(0)
// 		if nil != err {
// 			panic(err)
// 		}
// 		subscriber.Recv(0)
// 		tx, err := btcutil.NewTxFromBytes(txByte)
// 		if err != nil {
// 			panic(err)
// 		}
// 		fmt.Println("txhash ", tx.Hash().String())
// 	}

// 	fmt.Println("all")
// }

func testMapReduce() {
	session, err := mgo.Dial("192.168.1.13:27002")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(nil)
	db := session.DB("bitdb_" + "BSV")
	cnt, _ := db.C("tx").Count()
	fmt.Println("cnt ", cnt)

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.height, this.fee) }",
		Reduce: "function(key, values) { return Array.count(values) }",
	}

	var result []struct {
		Height int64 "height"
		Value  int
	}
	_, err = db.C("tx").Find(bson.M{"height": 140002}).MapReduce(job, &result)
	if err != nil {
		panic(err)
	}
	fmt.Println(result[0].Value)
}

func getTxPerDay() {
	fmt.Println("getTxPerDay 1 ")
	configFilePath := flag.String("config", "./config.json", "Path of config file")
	flag.Parse()

	configJSON, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		glog.Fatal("read config failed: ", err)
		return
	}
	fmt.Println("getTxPerDay 2 ")
	configData := new(ConfigData)
	err = json.Unmarshal(configJSON, configData)
	if err != nil {
		glog.Fatal("parse config failed: ", err)
		return
	}

	glog.Info("configData.EnableStatistics ", configData.EnableStatistics)
	glog.Flush()
	common.GetStatisticInstance().Init(configData.EnableStatistics, configData.StatisticsFlushInterval)
	fmt.Println("getTxPerDay 3 ")
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}
	fmt.Println("getTxPerDay 4 ")
	now := time.Now()

	//highCursor := 150000
	highCursor := pMongoManager.Servicehub["BSV"].GetLastBlock()
	lowCursor := highCursor - 300
	if lowCursor < 0 {
		lowCursor = 0
	}
	block, err := pMongoManager.Servicehub["BSV"].GetBlockInfoByHeight(highCursor)
	if err != nil {
		panic(err)
	}
	if block.Time < now.Unix()-common.SEC_PER_DAY {
		panic("no tx")
	}

	cursor := (highCursor + lowCursor) / 2
	glog.Info("cursor ", cursor)
	glog.Flush()
	for {
		if highCursor == lowCursor || highCursor == lowCursor-1 {
			break
		}
		block, err = pMongoManager.Servicehub["BSV"].GetBlockInfoByHeight(cursor)
		glog.Infof("cursor %d %d %d %d", cursor, lowCursor, highCursor, block.Time)
		glog.Flush()
		if err != nil {
			panic(err)
		}
		if block.Time < now.Unix()-common.SEC_PER_DAY {
			lowCursor = cursor
		} else {
			highCursor = cursor
		}
		cursor = (highCursor + lowCursor) / 2
	}

	glog.Info("finish cursor ", cursor, " time ", block.Time)
	glog.Flush()
}

func get24hTx() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	//highCursor := 150000
	cnt, interval, err := pMongoManager.Servicehub["BSV"].Get24hTxCount()
	if err != nil {
		panic(err)
	}

	glog.Infof("cnt %d %d", cnt, interval)
	glog.Flush()
}

func testRemoveUnconfirmedTx() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	pMongoManager.Servicehub["BSV"].RemoveAllUnconfirmedTx()
}

func testHttp() {
	originurl := "http://192.168.1.16:4001/api/blockchain/state/BSV"
	encodeurl := url.QueryEscape(originurl)
	resp, err := http.Get(originurl)
	if err != nil {
		fmt.Println("http.Get %s failed %s", encodeurl, err)
		return
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("http.Get ioutil.ReadAll %s failed", encodeurl)
	}
	fmt.Println(resp.Body)
}

func testCoinbase() {
	connCfg := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	blockhash, err := chainhash.NewHashFromStr("0000000000000000009e43472a79f6154c61bfaaf309bec7ff4fa421753e533c")
	if err != nil {
		panic(err)
	}

	block, err := client.GetBlock(blockhash)
	if err != nil {
		panic(err)
	}

	tx := block.Transactions[0]
	txin := tx.TxIn[0]
	fmt.Println(string(txin.SignatureScript))

	txhash, err := chainhash.NewHashFromStr("7ce19db2517f4f17352e94977b73354d94bb899579c24cd69e1537de279fde97")
	if err != nil {
		panic(err)
	}
	txopreturn, err := client.GetRawTransaction(txhash)
	if err != nil {
		panic(err)
	}
	txout := txopreturn.MsgTx().TxOut[0]
	fmt.Println(string(txout.PkScript))
	scriptClass, addrs, _, err := txscript.ExtractPkScriptAddrs(txout.PkScript, &chaincfg.MainNetParams)
	fmt.Println("scriptClass ", scriptClass, " value ", txout.Value)
	if err != nil {
		panic(err)
	}
	fmt.Println("addr len ", len(addrs))
}

func testBlockHeader() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	headers := make(map[int64]string)
	pMongoManager.Servicehub["BSV"].GetBlockHeaders(0, headers)
	fmt.Println("headers size ", len(headers))
	headersPre := make(map[int64]string)
	pMongoManager.Servicehub["BSV"].GetBlockHeaderPre(0, headersPre)
	fmt.Println("headersPre size ", len(headersPre))

	for height, _ := range headers {
		_, ok := headersPre[height]
		if !ok {
			fmt.Println(" ", height, " not existed 1")
		}
	}

	for heightPre, _ := range headersPre {
		_, ok := headersPre[heightPre]
		if !ok {
			fmt.Println(" ", heightPre, " not existed 2")
		}
	}
}

func testMinusOne() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	off1 := pMongoManager.Servicehub["BSV"].GetLastInputOffset()
	off2 := pMongoManager.Servicehub["BSV"].GetLastOutputOffset()
	fmt.Println(" ", off1, " ", off2)
}

func testMap(name string, m map[int64]string) {
	for i := 0; i < len(m); i++ {
		_, ok := m[int64(i)]
		if !ok {
			glog.Info("name ", name, " ", i, " is missing")
		}
	}
	glog.Info("name ", name, " len ", len(m))
}

func testCount() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	crawlCountInfo := pMongoManager.Servicehub["BSV"].GetCrawlCountInfo()
	glog.Info("AddrTxCount ", crawlCountInfo.AddrTxCount)
	glog.Info("BlockHeaderCount ", crawlCountInfo.BlockHeaderCount)
	glog.Info("BlockHeaderPreCount ", crawlCountInfo.BlockHeaderPreCount)
	glog.Info("BlockTxCount ", crawlCountInfo.BlockTxCount)
	glog.Info("HeightAddrCount ", crawlCountInfo.HeightAddrCount)
	glog.Info("HeightHashInputCount ", crawlCountInfo.HeightHashInputCount)
	glog.Info("HeightHashInputPreCount ", crawlCountInfo.HeightHashInputPreCount)
	glog.Info("HeightHashOutputCount ", crawlCountInfo.HeightHashOutputCount)
	glog.Info("HeightHashOutputPreCount ", crawlCountInfo.HeightHashOutputPreCount)
	glog.Info("MonitoredAddrCount ", crawlCountInfo.MonitoredAddrCount)
	glog.Info("MonitoredAddrInventoryCount ", crawlCountInfo.MonitoredAddrInventoryCount)
	glog.Info("TxCount ", crawlCountInfo.TxCount)
	glog.Info("TxOutputCount ", crawlCountInfo.TxOutputCount)

	m1 := make(map[int64]string)
	m2 := make(map[int64]string)
	m3 := make(map[int64]string)
	m4 := make(map[int64]string)
	m5 := make(map[int64]string)
	m6 := make(map[int64]string)
	m7 := make(map[int64]string)
	pMongoManager.Servicehub["BSV"].GetBlockHeaderPre(0, m1)
	testMap("m1", m1)
	pMongoManager.Servicehub["BSV"].GetHeightHashInput(0, m2)
	testMap("m2", m2)
	pMongoManager.Servicehub["BSV"].GetHeightHashInventory(0, m3)
	testMap("m3", m3)
	pMongoManager.Servicehub["BSV"].GetHeightHashOutput(0, m4)
	testMap("m4", m4)
	pMongoManager.Servicehub["BSV"].GetHeightHashInputPre(0, m5)
	testMap("m5", m5)
	pMongoManager.Servicehub["BSV"].GetHeightHashInventoryPre(0, m6)
	testMap("m6", m6)
	pMongoManager.Servicehub["BSV"].GetHeightHashOutputPre(0, m7)
	testMap("m7", m7)
	glog.Flush()
}

func testCoinbaseInfo() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}
	glog.Info("coinbase1 ")
	glog.Flush()
	block, err := pMongoManager.Servicehub["BSV"].GetBlockInfoByHeight(int64(574456))
	if err != nil {
		panic(err)
	}

	glog.Info("coinbase ", string(block.CoinBaseInfo))
	glog.Flush()
}

func fixMonitoredAddrTxTimestamp() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	pMongoManager.Servicehub["BSV"].FixMonitoredAddrTxTimestampTmp()
}

func checkMissingTx() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	connCfg := &rpcclient.ConnConfig{
		User:         "fullnodetesting",
		Pass:         "fullnodetesting9902kcylap0sdsdbldfasdpfuoupouewrh1111jknvllad",
		Host:         "192.168.17.218:8332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	str := `{"data":{"1Gbo8zKpqnsrdmkgUpUW6SkDUKL5atbh2K":{"address":{"type":"pubkeyhash","script_hex":"76a914ab1e8b15646c215dc720e2646db6cfddba1028ef88ac","balance":65362932330,"balance_usd":41752.5339137574,"received":10189520949739,"received_usd":8151263.962,"spent":10124158017409,"spent_usd":8095128.3295,"output_count":266,"unspent_output_count":2,"first_seen_receiving":"2018-11-29 07:38:54","last_seen_receiving":"2019-03-29 09:29:38","first_seen_spending":"2018-11-29 07:54:43","last_seen_spending":"2019-03-29 09:29:38","transaction_count":266,"formats":{"legacy":"1Gbo8zKpqnsrdmkgUpUW6SkDUKL5atbh2K","cashaddr":"qz43azc4v3kzzhw8yr3xgmdkelwm5ypgauhp4gkwuy"}},"transactions":["72293db85fa2f5efe2aaab6bd373b638bd8d0c8180a13a8ae6349adb4d6483ce","1e800802e7b95e12a357dc039325351a71d9d023356d8a3c824d00e23071624e","10c60fd8fa8ae122908704a6c3d72057e659ce081a0645689bac87f54a71f47b","d7b3fad55d730fe8f797f48466bc22135ef0476c7614af9098b0bf3f89841922","7b0a55097f4319abaa96d8efe0cf84c960f543691d0e6d387d8e0e4d9772c1d6","942d30c52a3d965ec746950e63d73b35e9185bb6078c64be0ca46151987bff0d","c88004d5ec7db47652b65bd23aec6efbcb2ed5d470906463302eda468728e12d","4bb9d9d99beea32dc43c76022e4a968dd01445a8ec825b2b52fe449f87b8d171","d7a88132ce38dfa60f3c0324919034a8fe14cda42aeb727608b0bb993104d843","14a4ebc6673b0a79b28ab48ff4d68fae1853eb00e722562825f60f09a5f96c56","f384a214bc77d3ef889df89d9ccc0780a627eac1128dd9def05a56b75fcb69d2","7922cb79280a1042a5dc2b75757df9954d353465cb20769a4798a6cbd8d6518e","f336057eaf7a1b6b17f7e28232560730d5e6755f067255657289340413743f14","ea2cbe9e29442dbcb5ded03f465e38d0cf78d8d3eaa09525c4712865bb3b59c5","c0e0273a06102306a34dcbec31b829e4aa8215f6d2b009d108b6ba0b7913ac0f","a61c2b68b78c3eca11e2e4b97dd4387fc679e9005346be21904ac3577be3827a","79d8ced5399878df87a7e4f3a56c77f8d7f66562af5e9831dde3e30abe01de4e","341194a5144cd8a91de4dc8f13ce8649e77219ca456231529e3f181288ec9bd9","5590aceb5c9cb0f3bc7de348f08d1a4e1c4e546ec91d091890046b17dd03ce52","83b7c1b066422bae250615d722ad4e51cb68f7397a721e6f13ee5d276764791f","955c339ff7f033fbae1ed80957c11563321a5e5db545d01a739a44c54e8b0f5f","b4e0f3dd897cacd75bbe09272f97a5e2c2b3f86e02f7a1541d7bfb981743235a","db53c160bbb353e6c564567e8b74a1ea8e89f10c94916ef7ca80f450a8a4ef25","94300cc588c71d7aaaa24df7f5cc0e2cb6cc815e5843bcbaa1031ecae52d33f8","8229df32ebc0f81f2384a7d7deae266f1d516c6895843290e11ee1f4628318dd","94a363670f9a39cc97bc372f8654776446a9c40efb3d44cce6fae6fd28f0d24a","cdfd7e4f052c8f69fa7e80e993a0e406d246e54fda3742451dfc1384a0da06b1","ac2e6dbb86cca455f0df2ec9143de74c13a03aca0ffb051de85b0eb0caf773d4","82a3f8082ba04f963ff25f32ea45540c00339bae59ba003b587f5ab8b546fdf0","0ed25d2994a3d310841476262a1f8b30ffeb5107b7c70c3c59f51ee9984ef893","b860ebc89f480aa204b5bbb0c6eca642bd283bd0e6606c6e23b5f43662567c54","cc7b7c48fb45580dc6520092b30eb9124046d2782ad102a672cff6cf3e622be8","641fac78707bf69e14e2962eb6b954b44f93e024f9128edc94eb0e0af213dec7","e1185c72f32cd2612b945264e6c39a560553c8540f4ce49fde11a28be42050a6","a353ba33ae80cac51a3874608dffdf3d764211d7041906bd77dbd1eaf5a19e38","d60b7641cc9281d5a2b037eb492c246150db23af7dadf118fc9792be38a1f379","80e691ca43875fb66b38d4ffa70bc125959ffe0c52c137e6be0f9e77b2a94f66","fa3a5f95f5a0f900bbc10dc6251448ac1ce344ac4203b85f002930527d99e9ca","882b8724705b5aae86400897594b48b21763adc9c48d70320ca06944aae0fc2e","396580d1f0ea2f186c5b875374ec44ed3fc3030acce6ad7e6dcd0ab51af15e1b","0e9353148a8eb51499abcd093c235b87f5ae71d3db6c2c542f5030526481cca6","e2e25dbdc417941cc5a9018b5d72b729bf6ce5b568f8251ce9c5211ab8096f2c","dbd008d8a342b1d02be00cefd41b0cacea457ff565d6fd8c473ca783462d34e3","5762c00d244c85dc7ad451378347c2431e9892a235d514a7f76f6603a0874b66","5fe75960ce8cbfcbf4fed353e3c1fce97aef2837fcad986ee704d00cb3e8df29","e458656d779d4bc823649689794192c5ef15f410ed04178a350826f326406df8","96186e91bc55f7c568037e4f2322f223438cbd39a5ee52de8cb3b3f9da30a836","ad693bee665562d48676f1d6c8d86231fbd5310cc778e74f306f8d6c5f1b2a1a","c32e116ae8a0a1cc2eb98c1c01e44b8fb4ebbdb3d5142630e82b1e9a26b1302e","8f34b733578fab1fbd69e048de06a753615cbccef0b531ca70778d738ebbbc98","71d276a3008704122e049d7465c1c1a69f7bf4f0909e32197a086a50c81de12c","106767e90eae5fb5c5dd530de7b28ad7cf4efe17c1e7e063174f1f1673245215","a9937058fa057fe957851bb8de1d25f18debe9ceaf6806bab574edca51dd31ee","c192cef8c8c7ec12231dc41358824e511eaab9fdbe5fe6d87d6192720f029b37","aacec6d2bbe4940fe2899f524da3cef4db0407cb2262b2bdd527131c4e41045c","aa7074b3832321ddb0ecf1f9f387ad8fe7969c57c7503f3a15b2e6f66352528c","b84450b21c2d2a20ebcfdd326eba1b275c01fea50aead0461525bc334dabdbfc","f6612dde8688838d7bdd3d13f288fd3661d921e81cca64862acf869f8f15f227","86f2c1a35f4125657bf37648ced2c1a48672d45386936bac17fe66603ed33674","e2f73a133c60bd95ec7219e87f21b38d10df93ccf799a7f4c5ec5fc89951df11","12896effa1b68b64a3851523a312074929bbc07f8975b4b3a7bd25456932804f","9983a99d7bb8357c44725b4089fa32363cf1523b53fe5fe72e86954963cf8d2b","b539e279f810a09880b20a89d0da36fda954b28d3deb5ccc2cfdfc810147f33a","5cb8ec8cf844056c7b8112278fc80e0418b30297ca9884a3e8321c9c24da26c7","b5486f7c3a85d35915568c50ad6e030d0517e1d3a1065bbb9f25dde9dea79e4a","905a8a8bf32eeff11324f5a29b58d2898204ed27d0a55d860d5fcb70739b6b27","2f501d96f63ad6f513a954c64b3103524d840f3bacf144f24fd355d379e5a3b8","8936f00138782b18bdb45a3740543cdde89135723809ab1b5fbb9be7b19adce5","c7064fc08b0a56652472bc23cc13ba09995a727e4d2fea13ecbfc538b56abdcc","5859d45013e0f58e3edc89943232fa3627b31187b70016e34fa76ca74b309c0a","6cde64e141c8fbed3aa8064a438bacaeb09fbd44ec7bf50a41c2c9bcda8260b7","07612439f1f75775377829d8394c7bb4264b3fe51712ddc65f81f28bdf4a392c","111a1f6a0e1775093bc7402e14218d76f185bd7369e5f72675eeb6340585fbd1","9bdcadebdea16195e2fe1a82dba1ea7443838409059f2bec32bd2ff23cbd063d","f01418ddf5ac6b9cb63d0c9cd5495c58f0da5bd49abb813cdd8cd480f34c5d14","1476135a3d12c709ba52383107059b8f7effdff7896be81a26258e8f123e3558","427f1cb5b11e9e28ab9398b12fa4cf8bd25c4b556b4d6ef457f721372ed054d8","d3a94a3dac112fedb732731e2a40209393f06a631211096c30254ef9eca491ea","40bdc7faa60d3a43094c3f3ecc843763ac21bd43940b6fd5f39cd8a7a89da7f1","b4b03e0ec44945e1ff20ba86f993822b83c472fff995f5bfe8dffa91b47655bf","602836f6e2823f35e511aa817481fed67824daea70dded4011b6dfd6fd78c1ca","625b9be29c82800896678e46769f5fe2d5df28b932ff3c74951aa142ae0f9057","908495c383f034442bc336b4db1e28385fc875bad6d603fc3e9c17c34c8755a4","2047679f9415437b22cb77a44b2dbfeba582b807ab040d23e2a86911a8c4241b","a73eb27d4927d54b9819d97dd215de9254c47f4e0d8ed75aa14b2c96c3124323","f262fe99e3d5aad3c33349abe874adb5c4332aab184ad8917a1d724c536a0416","76917dcc13c680ce5f9c0cc273f82039a672f0229e143bfbd550ef9d84de7341","b83dd3f54ca22101e981ab888b27b74bb147bb18aa93d3d6e5616d94decd2532","42f29ee9f398bc4ebd937d21da85a2616b3e417e46058a5fa4c8b83f4d2327a9","3fd48e243c18fd55151d9129267b7117b403d07ffb09cd9cd6258aa87a82132b","0588d768b72aad2fd53160ebbb904652a7d80272c04437e099ecf15827af6928","7531db623a617e28812d0cdfcdf8dd520cf1728602224b99a529bceb8a25e2bb","5757a33131ff49aacdbd5dd378a310ea9376ef948923cef4cc5183df7f0ac3fb","896120906a3869bef22266e2eb39df3976fd70ff4b62abf4051e186509ea9d2a","42906939205896100f1d6ef09a8096b585d42d6c8fcf9fe3b699fe42ccdad694","1b87aafc52af6edc8291f0e564a667957d38acea29c54715ebf405415ea7afe0","73dcb4989f98892c684433741848d9ef4ce11044ced032a53ada837e6c2e2f33","c9562f096ca9363508e74333391740a76c755608626958574689ef1708a4a5c7","7c870bbb894c8967096f685baf5cd6d663b45fa71fd3777099e98e7a40ca2d19","2276ff659520bf84e45cd4c85b4a0dfe701b000c7efb79e623b66f6b727bc4ba"]}},"context":{"code":200,"source":"D","time":0.11289501190185547,"limit":100,"offset":0,"results":1,"state":575775,"cache":{"live":true,"duration":30,"since":"2019-03-29 12:31:15","until":"2019-03-29 12:31:45","time":null},"api":{"version":"2.0.16","last_major_update":"2018-07-19 18:07:19","next_major_update":null,"tested_features":"omni-v.a1,whc-v.a1,aggregate-v.b3,ipfs-nodes-v.a1,xpub-v.b2,dash-v.rc1","documentation":"https:\/\/github.com\/Blockchair\/Blockchair.Support\/blob\/master\/API.md"}}}`
	js, err := simplejson.NewJson([]byte(str))
	if err != nil {
		panic(err)
	}
	//fmt.Println(js)

	//fmt.Println("========================")
	txArr, err := js.Get("data").Get("1Gbo8zKpqnsrdmkgUpUW6SkDUKL5atbh2K").Get("transactions").Array()
	feeMap := make(map[string]int64)
	for _, txstrtmp := range txArr {
		txstr := txstrtmp.(string)
		//fmt.Println("tx ", txstr)
		hash, _ := chainhash.NewHashFromStr(txstr)
		txOrigin, err := client.GetRawTransaction(hash)
		if err != nil {
			panic(err)
		}
		tx := txOrigin.MsgTx()
		txhashstr := txstr

		fee := int64(0)
		for index, txout := range tx.TxOut {
			addrstr, err := pMongoManager.Servicehub["BSV"].DecodeOutput(txout)
			if err != nil {
				pMongoManager.Servicehub["BSV"].AddErrorTx(tx.TxHash().String(), index, "", 0)
				continue
			}

			if addrstr == "1Gbo8zKpqnsrdmkgUpUW6SkDUKL5atbh2K" {
				fee += txout.Value
			}
		}

		feeMap[txhashstr] = fee
	}

	// for k, v := range feeMap {
	// 	fmt.Println(k, " ", v)
	// }

	items, err := pMongoManager.Servicehub["BSV"].GetAddrTx("1Gbo8zKpqnsrdmkgUpUW6SkDUKL5atbh2K")
	if err != nil {
		return
	}

	txMap := make(map[string]int64)
	for _, item := range items {
		txMap[item.Txid] = 0
	}

	for _, item := range items {
		if item.Txid == "14a4ebc6673b0a79b28ab48ff4d68fae1853eb00e722562825f60f09a5f96c56" ||
			item.Txid == "d7a88132ce38dfa60f3c0324919034a8fe14cda42aeb727608b0bb993104d843" {
			fmt.Println(item.Txid, " ", item.Value)
		}
		if !item.IsInput {
			txMap[item.Txid] += int64(item.Value)
		}
	}

	fmt.Println("map size ", len(feeMap), " ", len(txMap))
	// for k, v := range feeMap {
	// 	if v == 0 {
	// 		continue
	// 	}
	// 	if v != int64(txMap[k]) {
	// 		fmt.Println("diff1 ", k, " ", v, " ", txMap[k])
	// 	}
	// }

	// for k, v := range txMap {
	// 	if v == 0 {
	// 		continue
	// 	}
	// 	if v != int64(feeMap[k]) {
	// 		fmt.Println("diff2 ", k, " ", v, " ", feeMap[k])
	// 	}
	// }

	blockhash, err := chainhash.NewHashFromStr("0000000000000000074422241cc1cd7b4d9379832e018f4db2df0830f690f1d7")
	if err != nil {
		panic(err)
	}

	block, err := client.GetBlock(blockhash)
	if err != nil {
		panic(err)
	}
	for _, tx := range block.Transactions {
		for _, txout := range tx.TxOut {
			txhash := tx.TxHash()
			txhashstr := txhash.String()
			addrstr, err := pMongoManager.Servicehub["BSV"].DecodeOutput(txout)
			if err != nil {
				continue
			}

			fmt.Println(txhashstr, " ", addrstr, " ", txout.Value)
		}
	}
}

func checkEmptyVout() {
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
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	for i := int64(0); i < 600000; i++ {
		if i%10000 == 0 {
			fmt.Println("i:", i)
		}
		pMongoManager.Servicehub["BSV"].CheckEmptyVout(i)
	}
}

func testOwnerid() {
	owneridstr := "10756"
	ownerid, err := strconv.Atoi(owneridstr)
	if err != nil {
		panic(err)
	}
	println("testOwnerid", ownerid)
}

func checkOwnerid() {
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

	glog.Info("configData.EnableStatisticsss ", configData.EnableStatistics)
	glog.Flush()
	common.GetStatisticInstance().Init(configData.EnableStatistics, configData.StatisticsFlushInterval)
	pFullnodeManager := &common.FullnodeManager{}
	pFullnodeManager.Init()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, fullnode := range configData.Fullnode {
		_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
		if err != nil {
			panic(err)
		}

		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}

	pMongoManager.Servicehub["BSV"].CheckOwnerid(151)
}

func testLog() {
	flag.Parse()
	for i := 0; i < 10000; i++ {
		//fmt.Println("i", i)
		glog.Info("testLog")
	}
}

func testGetBlockInfo() {
	connCfg := &rpcclient.ConnConfig{
		User:         "toorq2000",
		Pass:         "helloworldthisisafuckingtestingforbch2000",
		Host:         "192.168.1.9:18332",
		HTTPPostMode: true,
		DisableTLS:   true,
	}
	client, err := rpcclient.New(connCfg, nil)
	if err != nil {
		panic(err)
	}

	info, err := client.GetBlockChainInfo()
	if err != nil {
		panic(err)
	}

	fmt.Println("height ", info.Blocks)
}

func connectMongo(configFilePath *string) (*common.MongoManager, error) {
	configJSON, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		glog.Fatal("read config failed: ", err)
		return nil, err
	}

	configData := new(ConfigData)
	err = json.Unmarshal(configJSON, configData)
	if err != nil {
		glog.Fatal("parse config failed: ", err)
		return nil, err
	}

	glog.Info("configData.EnableStatistics ", configData.EnableStatistics)
	glog.Flush()
	pMongoManager := &common.MongoManager{}
	pMongoManager.Init()
	for coinType, _ := range configData.Fullnode {
		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
	}
	return pMongoManager, nil
}

func copyMonitored() {
	flag.Parse()
	//1.先创建到新的数据库的连接
	configFilePath := "./config.json"
	newMongoManger, err := connectMongo(&configFilePath)
	if err != nil {
		panic(err)
	}
	//2.再创建到原始数据库的连接
	configFilePath = "./source.json"
	oldMongoManger, err := connectMongo(&configFilePath)
	//3.准备同步通道，用来等待所有的表复制完成
	count := len(oldMongoManger.Servicehub)
	ci := make(chan int)
	//4.开始复制
	for coinType, des := range newMongoManger.Servicehub {
		soruce, ok := oldMongoManger.Servicehub[coinType]
		if !ok {
			glog.Infof(coinType + "类型的monitored在原来的数据库中不存在")
			continue
		}
		desLocal := des
		go func() {
			desLocal.CopyMonitoredAddr(soruce)
			ci <- 1
		}()
		go func() {
			desLocal.CopyMonitoredInvertory(soruce)
			ci <- 1
		}()
	}
	for count = 2 * count; count > 0; count-- {
		<-ci
	}
}

func compara() {
	flag.Parse()
	//1.先创建到新的数据库的连接
	configFilePath := "./config.json"
	newMongoManger, err := connectMongo(&configFilePath)
	if err != nil {
		panic(err)
	}
	//2.再创建到原始数据库的连接
	configFilePath = "./source.json"
	oldMongoManger, err := connectMongo(&configFilePath)
	//4.开始复制
	for coinType, des := range newMongoManger.Servicehub {
		soruce, ok := oldMongoManger.Servicehub[coinType]
		if !ok {
			glog.Infof(coinType + "类型的monitored在原来的数据库中不存在")
			continue
		}
		desLocal := des
		desLocal.CompareMonitoredAddrAndInsert(soruce)
	}
}

func FindDifferent(local map[string]*common.MonitoredAddrBson, syncCenter map[string]*common.MonitoredAddrBson) {
	//检查中心的内容本地有没有
	glog.Infof("local count %d", len(local))
	glog.Infof("local syncCenter %d", len(syncCenter))
	glog.Flush()
	for addr, _ := range syncCenter {
		_, ok := local[addr]
		if !ok {
			//本地没有,向本地插入
			glog.Infof("local lack %s", addr)
		}
	}
	//检查本地的内容中心有没有
	for addr, _ := range local {
		_, ok := syncCenter[addr]
		if !ok {
			//中心没有,向中心插入
			glog.Infof("syncCenter lack %s", addr)
		}
	}
	glog.Flush()
}

func SyncMonitoredAddr(hub *common.MongoServiceHub) {
	//1.首先两个Mongo获取数据
	glog.Infof("SyncMonitoredAddr:GetLocalMonitoredAddrMap start")
	local := hub.GetLocalMonitoredAddrMap()
	glog.Infof("SyncMonitoredAddr:GetSynCenterMonitoredAddrMap start")
	syncCenter := hub.GetSynCenterMonitoredAddrMap()
	glog.Infof("SyncMonitoredAddr:PutMonitoredAddrs start")
	//2.相互同步数据
	// err := hub.SyncMonitoredAddrs(local, syncCenter)
	// if err != nil {
	// 	glog.Infof("SyncMonitoredAddr:%s", err)

	// }

	//找到不同的数据
	FindDifferent(local, syncCenter)
}

func syncMonitoredData() {
	//1.读取配置mongo的配置文件
	configFilePath := flag.String("config", "./config.json", "配置文件地址")
	flag.Parse()
	configJson, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		panic(err)
	}
	configData := new(ConfigData)
	err = json.Unmarshal(configJson, configData)
	pMongoManager := new(common.MongoManager)
	pMongoManager.Init()
	pSyncManager := new(common.SyncManager)
	pSyncManager.Init()
	for coinType, _ := range configData.Fullnode {
		pMongoManager.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			configData.Mongo[coinType].MonitoredService,
			configData.Mongo[coinType].SyncMonitoredAddrService)
		SyncMonitoredAddr(pMongoManager.Servicehub[coinType])
	}
}

func checkTwoOwnerBalance(hub1 *common.MongoServiceHub, hub2 *common.MongoServiceHub) {
	local1 := hub1.GetLocalMonitoredAddrMap()
	local2 := hub2.GetLocalMonitoredAddrMap()
	count := 0
	for addr, _ := range local1 {
		_, ok := local2[addr]
		if !ok {
			glog.Infof("local2 lack %s", addr)
			continue
		}
		balanceFromMongo1, err := hub1.GetAddrUnspentInfo(addr)
		if err != nil {
			glog.Infof("check addr %s false", addr)
		}
		balanceFromMongo2, err := hub2.GetAddrUnspentInfo(addr)
		if err != nil {
			glog.Infof("check addr %s false", addr)

		}
		if balanceFromMongo1.Balance != balanceFromMongo2.Balance {
			glog.Infof("check addr %s local1 %d local2 %d", addr, balanceFromMongo1.Balance, balanceFromMongo2.Balance)
		}
		count++
		if count%10000 == 0 {
			glog.Infof("already compara %d", count)
		}
	}
	for addr, _ := range local2 {
		_, ok := local1[addr]
		if !ok {
			glog.Infof("local1 lack %s", addr)
			continue
		}

	}

}

func InitStartcheckBalance() {
	//1.读取配置mongo的配置文件
	configFilePath := flag.String("config", "./config.json", "配置文件地址")
	flag.Parse()
	configJson, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		panic(err)
	}
	configData := new(ConfigData)
	err = json.Unmarshal(configJson, configData)
	pMongoManager1 := new(common.MongoManager)
	pMongoManager1.Init()
	pMongoManager2 := new(common.MongoManager)
	pMongoManager2.Init()
	for coinType, _ := range configData.Fullnode {
		pMongoManager1.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			"localhost:27017",
			configData.Mongo[coinType].SyncMonitoredAddrService)
		pMongoManager2.AddServiceHub(
			coinType,
			configData.Mongo[coinType].AddrService,
			configData.Mongo[coinType].BlockHeaderService,
			configData.Mongo[coinType].AddrIndexService,
			configData.Mongo[coinType].HeightAddrService,
			configData.Mongo[coinType].HeightHashService,
			"localhost:27017",
			configData.Mongo[coinType].SyncMonitoredAddrService)
		checkTwoOwnerBalance(pMongoManager1.Servicehub[coinType], pMongoManager2.Servicehub[coinType])
	}
}

func newPool() *redis.Pool {
	return &redis.Pool{
		Wait:        true,
		MaxIdle:     300,
		MaxActive:   1000,
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "192.168.1.9:6379")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func testMultiBalance() {
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

	glog.Info("configData.EnableStatisticsss ", configData.EnableStatistics)
	glog.Flush()
	common.GetStatisticInstance().Init(configData.EnableStatistics, configData.StatisticsFlushInterval)
	for i:=0; i < 100; i++ {
		go func(index int) {
			pFullnodeManager := &common.FullnodeManager{}
			pFullnodeManager.Init()
			pMongoManager := &common.MongoManager{}
			pMongoManager.Init()
			for coinType, fullnode := range configData.Fullnode {
				_, err := pFullnodeManager.AddFullnodeInfo(coinType, fullnode)
				if err != nil {
					panic(err)
				}

				pMongoManager.AddServiceHub(
					coinType,
					configData.Mongo[coinType].AddrService,
					configData.Mongo[coinType].BlockHeaderService,
					configData.Mongo[coinType].AddrIndexService,
					configData.Mongo[coinType].HeightAddrService,
					configData.Mongo[coinType].HeightHashService,
					configData.Mongo[coinType].MonitoredService,
					configData.Mongo[coinType].SyncMonitoredAddrService)
			}

			for j:=0; j < 100; j++ {
				start := time.Now()
				balance, err := pMongoManager.Servicehub["BSV"].GetBalance("wechat-wallet", 100 * j + index)
				if err != nil {
					panic(err)
				}
				timeused := time.Since(start).Nanoseconds()
				glog.Infof("i:%d j:%d timeused:%d balance:%d", index, j, timeused, balance)
				time.Sleep(time.Second)
			}
		}(i)
	}

	for {
		time.Sleep(time.Second)
	}
}

//func testRedis() {
//	pool := newPool()
//	for i:=0; i < 10000; i++ {
//		go func() {
//			for {
//				connTmp := pool.Get()
//				defer connTmp.Close()
//				_, err := connTmp.Do("HGET", miningWorkerKey, "Accept_1M", in.Share)
//				if err != nil {
//					glog.Info(err, " ", miningWorkerKey)
//					return err
//				}
//			}
//		}()
//	}
//}

func main() {
	testMultiBalance()
}
