package main

import (
	"fmt"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/wire"
	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
)

type InputCrawler struct {
	Hub         *common.MongoServiceHub
	CrawlerInfo CrawlerInfo
	Fullnodes   []*rpcclient.Client
}

func (this *InputCrawler) GetRecrawleCount() int64 {
	return this.CrawlerInfo.RecrawlerCount
}

func (this *InputCrawler) GetCrawlerCount() int64 {
	return this.CrawlerInfo.CrawlerCount
}

func (this *InputCrawler) GetConfigOffset() int64 {
	return this.CrawlerInfo.CrawlerOffset
}

func (this *InputCrawler) GetLastOffset() int64 {
	return this.Hub.GetLastInputOffset()
}

func (this *InputCrawler) GetHash(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashInput(lastBlock, hashmap)
}

func (this *InputCrawler) GetHashPre(lastBlock int64, hashmap map[int64]string) error {
	return this.Hub.GetHeightHashInputPre(lastBlock, hashmap)
}

func (this *InputCrawler) ClearAddrTx(height int64) error {
	return this.Hub.ClearAddrTx(height, true)
}

func (this *InputCrawler) AddHashPre(height int64) error {
	return this.Hub.AddHeightHashInputPre(height, "")
}

func (this *InputCrawler) AddHash(height int64, hash string) error {
	return this.Hub.AddHeightHashInput(height, hash)
}

func (this *InputCrawler) StartCrawl(
	block *wire.MsgBlock,
	height int64,
	tid int) {

	fragment := common.StartTrace("InputCrawler startCrawl 1")
	thisBlockTime, err := this.Hub.GetBlockTime(height)
	fragment.StopTrace(tid)
	if err != nil {
		panic(err)
	}
	for _, tx := range block.Transactions {
		txhash := tx.TxHash()
		txhashstr := txhash.String()

		fee := int64(0)
		vins := make([]string, 0, 10)
		coinDay := int64(0)
		for index, txin := range tx.TxIn {
			fragment = common.StartTrace("InputCrawler startCrawl 2")
			vinaddr, vinvalue, preBlockTime, err := this.Hub.DecodeInput(
				this.Fullnodes, txhashstr, index, txin, height, tid)
			fragment.StopTrace(tid)
			if err != nil {
				this.Hub.AddErrorTx(tx.TxHash().String(), index, "", 0)
				glog.Infof("decodeInput %d %s %d failed %s", height, txhashstr, index, err)
				continue
			}
			previousOutPoint := txin.PreviousOutPoint
			err = this.Hub.AddAddrTx(vinaddr, height, txhashstr, index, vinvalue, previousOutPoint.Hash.String(), int(previousOutPoint.Index), txin.SignatureScript)
			if err != nil {
				glog.Infof("addAddrTx %d failed index=%d value=%d %s", height, index, vinvalue, err)
				panic(err)
			}
			vin := fmt.Sprintf("%s@%d", vinaddr, vinvalue)
			vins = append(vins, vin)
			fee += vinvalue
			coinDay += (thisBlockTime - preBlockTime) * vinvalue / common.SEC_PER_DAY
		}

		vouts := make([]string, 0, 10)
		for index, txout := range tx.TxOut {
			fragment = common.StartTrace("InputCrawler startCrawl 3")
			addrstr, err := this.Hub.DecodeOutput(txout)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("decodeOutput %d failed %s", height, err)
				continue
			}

			fragment := common.StartTrace("InputCrawler startCrawl 4")
			err = this.Hub.AddAddrTx(addrstr, height, txhashstr, index, txout.Value, "", 0, txout.PkScript)
			fragment.StopTrace(tid)
			if err != nil {
				glog.Infof("addAddrTx %d failed addr=%s index=%d value=%d %s", height, addrstr, index, txout.Value, err)
				panic(err)
			}

			vout := fmt.Sprintf("%s@%d", addrstr, txout.Value)
			vouts = append(vouts, vout)
			fee -= txout.Value
		}
		fragment = common.StartTrace("InputCrawler startCrawl 5")
		this.Hub.AddTx(txhashstr, height, vins, vouts, fee, coinDay, tx.SerializeSize())
		fragment.StopTrace(tid)

		//if txhashstr == "999e1c837c76a1b7fbb7e57baf87b309960f5ffefbf2a9b95dd890602272f644" {
		//	panic(nil)
		//}
	}
}

func (this *InputCrawler) RemoveBlockInfo(height int64) {
	this.Hub.RemoveInputInfo(height)
}
func (this *InputCrawler) GetHashFromDependence(height int64) (string, error) {
	hash, err := this.Hub.GetHashFromOutput(height)
	return hash, err
}

func (this *InputCrawler) GetHeightFromDependence() int64 {
	height := this.Hub.GetLastOutputOffset()
	return height
}
