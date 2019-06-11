package main

import (
	"fmt"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
)

type OutputCrawler struct {
	Hub         *common.MongoServiceHub
	CrawlerInfo CrawlerInfo
	Fullnodes   []*rpcclient.Client
}

func (this *OutputCrawler) GetRecrawleCount() int64 {
	return this.CrawlerInfo.RecrawlerCount
}

func (this *OutputCrawler) GetCrawlerCount() int64 {
	return this.CrawlerInfo.CrawlerCount
}

func (this *OutputCrawler) GetConfigOffset() int64 {
	return this.CrawlerInfo.CrawlerOffset
}

func (this *OutputCrawler) GetLastOffset() int64 {
	return this.Hub.GetLastOutputOffset()
}

func (this *OutputCrawler) GetHash(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashOutput(lastBlock, hashmap)
}

func (this *OutputCrawler) GetHashPre(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashOutputPre(lastBlock, hashmap)
}

func (this *OutputCrawler) ClearAddrTx(height int64) error {
	return this.Hub.ClearAddrTx(height, false)
}

func (this *OutputCrawler) AddHashPre(height int64) error {
	return this.Hub.AddHeightHashOutputPre(height, "")
}

func (this *OutputCrawler) AddHash(height int64, hash string) error {
	return this.Hub.AddHeightHashOutput(height, hash)
}

func (this *OutputCrawler) StartCrawl(block *wire.MsgBlock,
	height int64,
	tid int) {
	for _, tx := range block.Transactions {
		txhash := tx.TxHash()
		txhashstr := txhash.String()

		vouts := make([]string, 0, 10)
		for index, txout := range tx.TxOut {
			fragment := common.StartTrace("OutputCrawler startCrawl 1")
			addrstr, err := this.Hub.DecodeOutput(txout)
			fragment.StopTrace(tid)
			if err != nil {
				this.Hub.AddErrorTx(tx.TxHash().String(), index, "", 0)
				glog.Infof("decodeOutput %d failed %s", height, err)
				continue
			}

			vout := fmt.Sprintf("%s@%d", addrstr, txout.Value)
			vouts = append(vouts, vout)
		}

		fragment := common.StartTrace("OutputCrawler startCrawl 2")
		this.Hub.AddTxOutput(txhashstr, height, vouts, 0)
		fragment.StopTrace(tid)
		//if txhashstr == "999e1c837c76a1b7fbb7e57baf87b309960f5ffefbf2a9b95dd890602272f644" {
		//	panic(nil)
		//}
	}
}
func (this *OutputCrawler) RemoveBlockInfo(height int64) {
	this.Hub.RemoveOutputInfo(height)
}
func (this *OutputCrawler) GetHashFromDependence(height int64) (string, error) {
	hash, err := this.Hub.GetHashFromBlockHeader(height)
	return hash, err
}

func (this *OutputCrawler) GetHeightFromDependence() int64 {
	height := this.Hub.GetLastBlockHeaderOffset()
	return height
}
