package main

import (
	"encoding/json"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
	go_util "mempool.com/foundation/go-util"
)

type BlockManager struct {
	// KafkaUrl []string
	// Topic    string
}

func getBlockVerbose(jsonBlockVerbose *btcjson.GetBlockVerboseResult, msgtx *wire.MsgTx) *common.BlockVerboseBson {
	blockVerboseBson := &common.BlockVerboseBson{
		Hash:          jsonBlockVerbose.Hash,
		Confirmations: jsonBlockVerbose.Confirmations,
		StrippedSize:  jsonBlockVerbose.StrippedSize,
		Size:          jsonBlockVerbose.Size,
		Weight:        jsonBlockVerbose.Weight,
		Height:        jsonBlockVerbose.Height,
		Version:       jsonBlockVerbose.Version,
		VersionHex:    jsonBlockVerbose.VersionHex,
		MerkleRoot:    jsonBlockVerbose.MerkleRoot,
		Tx:            jsonBlockVerbose.Tx,
		RawTx:         jsonBlockVerbose.RawTx,
		TxCnt:         len(jsonBlockVerbose.Tx),
		Time:          jsonBlockVerbose.Time,
		Nonce:         jsonBlockVerbose.Nonce,
		Bits:          jsonBlockVerbose.Bits,
		Difficulty:    jsonBlockVerbose.Difficulty,
		PreviousHash:  jsonBlockVerbose.PreviousHash,
		NextHash:      jsonBlockVerbose.NextHash,
		CoinBaseInfo:  msgtx.TxIn[0].SignatureScript,
		CoinBaseTxid:  msgtx.TxHash().String(),
	}

	if len(blockVerboseBson.Tx) > 20 {
		blockVerboseBson.Tx = blockVerboseBson.Tx[:20]
	}
	if len(blockVerboseBson.RawTx) > 20 {
		blockVerboseBson.RawTx = blockVerboseBson.RawTx[:20]
	}

	return blockVerboseBson
}

func (blockManager *BlockManager) crawlBlockHeader(
	index int64,
	fullnodes []*rpcclient.Client,
	hub *common.MongoServiceHub,
	crawlerInfo CrawlerInfo) {
	tid := int(index % crawlerInfo.CrawlerCount)

	for {
		info, err := common.GetFullnode(fullnodes).GetBlockChainInfo()
		if err != nil {
			panic(err)
		}

		crawlerHeight := int64(info.Blocks)
		lastBlock := crawlerInfo.CrawlerOffset
		if lastBlock == -1 {
			lastBlock = hub.GetLastBlock()
		}
		if lastBlock > 100 {
			lastBlock -= 100
		} else {
			lastBlock = 0
		}
		glog.Infof("info.Blocks %d lastBlock %d",
			info.Blocks, lastBlock)

		headers := make(map[int64]string)
		err = hub.GetBlockHeaders(lastBlock, headers)
		if err != nil {
			panic(err)
		}

		prehashs := make(map[int64]string)
		err = hub.GetBlockHeaderPre(lastBlock, prehashs)
		if err != nil {
			panic(err)
		}

		glog.Infof("headers size %d prehashs size %d", len(headers), len(prehashs))
		glog.Flush()

		for height := index + lastBlock; height <= crawlerHeight; height += crawlerInfo.CrawlerCount {
			if height%100 == 0 {
				glog.Infof("block header %d %d", height, crawlerHeight)
			}

			_, ok := headers[height]
			if ok {
				//glog.Infof("block height %d already exists ", height)
				continue
			}

			_, ok = prehashs[height]
			if ok {
				//glog.Infof("addr height %d already exists ", height)
				hub.RemoveBlockHeader(height)
			} else {
				hub.AddBlockHeaderPre(height, "")
			}

			fragment := common.StartTrace("BlockManager 1")
			blockhash, err := common.GetFullnode(fullnodes).GetBlockHash(height)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("getblockheader %d failed ", height)
				continue
			}

			fragment = common.StartTrace("BlockManager 2")
			verboseBlock, err := common.GetFullnode(fullnodes).GetBlockVerbose(blockhash)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("GetBlockVerbose %d %s failed ", height, blockhash.String())
				continue
			}

			fragment = common.StartTrace("BlockManager 3")
			block, err := common.GetFullnode(fullnodes).GetBlock(blockhash)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("getblock %d failed ", height)
				continue
			}

			fragment = common.StartTrace("BlockManager 4")
			err = hub.AddBlockTxhash(verboseBlock.Hash, verboseBlock.Tx)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("AddBlockTxhash %d failed ", height)
				continue
			}

			fragment = common.StartTrace("BlockManager 5")
			blockVerboseBson := getBlockVerbose(verboseBlock, block.Transactions[0])
			fragment.StopTrace(tid)

			fragment = common.StartTrace("BlockManager 6")
			err = hub.AddBlockHeader(blockVerboseBson)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("insert block %d failed ", height)
				continue
			}
		}

		time.Sleep(time.Duration(10) * time.Second)
	}

}

func (blockManager *BlockManager) cleanOrphanBlock(
	// index int64,
	fullnodes []*rpcclient.Client,
	hub *common.MongoServiceHub,
	crawlerInfo CrawlerInfo) {
	//一开始先从550000开始爬
	lastBlock := int64(550000)
	// kafkaProductor := &common.KafkaProductor{KafkaUrl: blockManager.KafkaUrl, Topic: blockManager.Topic}
	for {
		//不应该检查到区块的最大高度，而是应该检查到已经爬取的最大高度
		crawlerHeight := hub.GetLastBlock()
		// lastBlock := crawlerHeight - 100
		headers := make(map[int64]string)
		err := hub.GetBlockHeaders(lastBlock, headers)
		if err != nil {
			panic(err)
		}
		for height := lastBlock; height <= crawlerHeight; height++ {
			hash, ok := headers[height]
			if !ok {
				//如果不存在，证明这个区块还没有爬，直接就不用检查了
				continue
			}
			//代码运行到这里，证明这个区块被爬过，所以才有检查的必要
			blockhash, err := common.GetFullnode(fullnodes).GetBlockHash(height)
			if err != nil {
				glog.Infof("getblockheader %d failed ", height)
				continue
			}
			//比较数据库中的hash和全节点中得到的hash是否一样，如果一样就继续循环，不一样就要删除
			if blockhash.String() == hash {
				continue
			}
			glog.Infof("block %d is orphan ", height)
			//接着要清理属于orphan的区块
			hub.RemoveBlockHeaderInfo(height)
			//此处发起通知
			// content := fmt.Sprintf("block height %d is orphan", height)
			contentMap := make(map[string]interface{})
			contentMap["bitdbIp"] = bitdbIp
			contentMap["height"] = height
			contentByte, err := json.Marshal(contentMap)
			if err != nil {
				glog.Infof("cleanOrphanBlock Marshal:%s", err)
				break
			}
			err = go_util.SmsNotify(go_util.Sms_bitdb_orphan, string(contentByte))
			if err != nil {
				glog.Infof("cleanOrphanBlock SmsNotify:%s", err)
			}
			// orphanMessage := &OrphanMessage{Height: height, HashInDatabase: hash, HashFromFullNode: blockhash.String()}
			// kafkaProductor.Notify(orphanMessage)
			break
		}
		//之后从倒数10个开始爬
		lastBlock = crawlerHeight - 10
		time.Sleep(time.Duration(10) * time.Second)
	}
}

func (blockManager *BlockManager) init(crawlerInfo CrawlerInfo, fullnodes []*rpcclient.Client, hub *common.MongoServiceHub) {
	for i := int64(0); i < crawlerInfo.CrawlerCount; i++ {
		go blockManager.crawlBlockHeader(
			i,
			fullnodes,
			hub,
			crawlerInfo)
	}
	go blockManager.cleanOrphanBlock(
		fullnodes,
		hub,
		crawlerInfo)
}
