package common

import (
	"time"

	"github.com/btcsuite/btcd/wire"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/golang/glog"
)

type ICrawler interface {
	GetRecrawleCount() int64
	GetCrawlerCount() int64
	GetConfigOffset() int64
	GetLastOffset() int64
	GetHash(int64, map[int64]string) error
	GetHashPre(int64, map[int64]string) error
	ClearAddrTx(height int64) error
	AddHash(height int64, hash string) error
	AddHashPre(height int64) error
	StartCrawl(block *wire.MsgBlock,
		height int64,
		tid int)
	RemoveBlockInfo(height int64)
	GetHashFromDependence(height int64) (string, error)
	GetHeightFromDependence() int64
}

func CrawlLoop(index int64, crawler ICrawler,
	fullnodes []*rpcclient.Client, hub *MongoServiceHub) {
	go cleanOrphanBlock(crawler, fullnodes, hub)
	for {
		endHeight := crawler.GetHeightFromDependence()

		lastBlock := crawler.GetConfigOffset()
		if lastBlock == -1 {
			lastBlock = crawler.GetLastOffset()
		}
		if lastBlock > crawler.GetRecrawleCount() {
			lastBlock -= crawler.GetRecrawleCount()
		} else {
			lastBlock = 0
		}
		startHeight := index + lastBlock
		// endHeight := int64(info.Blocks)
		glog.Info("lastBlock ", lastBlock)
		glog.Flush()

		hashs := make(map[int64]string)
		err := crawler.GetHash(lastBlock, hashs)
		if err != nil {
			panic(err)
		}

		prehashs := make(map[int64]string)
		err = crawler.GetHashPre(lastBlock, prehashs)
		if err != nil {
			panic(err)
		}
		tid := int(startHeight % crawler.GetCrawlerCount())
		for height := startHeight; height <= endHeight; height += crawler.GetCrawlerCount() {
			if height%100 == 0 {
				glog.Infof("crawlLoop height %d crawlerHeight %d ", height, endHeight)
			}

			_, ok := hashs[height]
			if ok {
				//glog.Infof("addr height %d already exists ", height)
				continue
			}

			_, ok = prehashs[height]
			if ok {
				//glog.Infof("addr height %d already exists ", height)
				crawler.ClearAddrTx(height)
			} else {
				crawler.AddHashPre(height)
			}

			var tryCount int
			for tryCount < 200 {
				tryCount++
				time.Sleep(time.Millisecond * 500)
				blockinfo, err := hub.GetBlockInfoByHeight(height)
				if err != nil {
					glog.Infof("chainhash.GetBlockInfoByHeight %d failed ", height)
					continue
				}

				blockhash, err := chainhash.NewHashFromStr(blockinfo.Hash)
				if err != nil {
					glog.Infof("chainhash.NewHashFromStr %s failed ", blockinfo.Hash)
					continue
				}

				fragment := StartTrace("crawlLoop 1")
				block, err := GetFullnode(fullnodes).GetBlock(blockhash)
				fragment.StopTrace(tid)
				if err != nil {
					glog.Infof("getblock %d failed ", height)
					continue
				}

				crawler.StartCrawl(block, height, tid)

				fragment = StartTrace("crawlLoop 2")
				err = crawler.AddHash(height, blockhash.String())
				fragment.StopTrace(tid)
				if err != nil {
					panic(err)
				}
				break
			}
			if tryCount >= 200 {
				panic("crawlLoop get block 200 times fault")
			}
		}
		time.Sleep(5 * time.Second)
	}
}
func cleanOrphanBlock(crawler ICrawler, fullnodes []*rpcclient.Client, hub *MongoServiceHub) {
	startHeight := int64(550000)
	for {
		//不应该检查到区块的最大高度，而是应该检查到已经爬取的最大高度
		endHeight := crawler.GetLastOffset()
		hashs := make(map[int64]string)
		err := crawler.GetHash(startHeight, hashs)
		if err != nil {
			glog.Infof("cleanOrphanBlock GetHash %s", err)
			time.Sleep(time.Second)
			continue
		}
		for height := startHeight; height <= endHeight; height += 1 {
			//如果不存在，则不需要检查
			hash, ok := hashs[height]
			if !ok {
				//glog.Infof("addr height %d already exists ", height)
				continue
			}
			//从依赖处获得hash
			blockhash, err := crawler.GetHashFromDependence(height)
			if err != nil {
				glog.Infof("chainhash GetBlockInfoFromDependence %d failed %s", height, err)
				glog.Flush()
				panic(err)
			}
			if blockhash == hash {
				//不是孤块，则继续
				continue
			}
			//运行到这里,证明是孤块
			glog.Infof("block %d is orphan ", height)
			crawler.RemoveBlockInfo(height)
			break
		}
		startHeight = endHeight - 10
		time.Sleep(time.Duration(60) * time.Second)
	}
}
