package common

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/golang/glog"
	"github.com/kataras/iris/core/errors"
	"gopkg.in/mgo.v2/bson"
)

var SEC_PER_DAY int64 = 24 * 3600
var COIN_PER_BLOCK float64 = 12.5

type ChainState struct {
	HashesPerSec      float64 `bson:"hashesPerSec"`
	Difficulty        float64 `bson:"difficulty"`
	ProfitPerT        float64 `bson:"profitpert"`
	UnconfirmedTxCnt  uint64  `bson:"unconfirmedTxcnt"`
	UnconfirmedTxSize uint64  `bson:"unconfirmedtxsize"`
	TxCntPerDay       int64   `bson:"txcntperday"`
	TxRatePerDay      float64 `bson:"txrateperday"`
}

func (this *MongoServiceHub) GetChainState() (*ChainState, error) {
	hashrateTDay := float64(1000000000000 * SEC_PER_DAY)
	totalDiff, interval, err := this.Get24hHashRate()
	if err != nil {
		glog.Info("GetChainState 1 ", err)
		return nil, err
	}
	networkHashTPS := totalDiff * (1 << 32) / hashrateTDay

	count, size, err := this.GetUnconfirmedTxInfo()
	if err != nil {
		glog.Info("GetChainState 2 ", err)
		return nil, err
	}
	txCnt, interval, err := this.Get24hTxCount()
	if err != nil {
		glog.Info("GetChainState 3 ", err)
		return nil, err
	}

	diff, err := this.GetDifficulty()
	if err != nil {
		glog.Info("GetChainState 4 ", err)
		return nil, err
	}
	chainState := &ChainState{
		UnconfirmedTxCnt:  count,
		UnconfirmedTxSize: size,
		HashesPerSec:      networkHashTPS,
		Difficulty:        diff,
		ProfitPerT:        hashrateTDay * COIN_PER_BLOCK / (1 << 32) / diff,
		TxCntPerDay:       txCnt,
		TxRatePerDay:      float64(txCnt) / float64(interval),
	}

	return chainState, nil
}

func (this *MongoServiceHub) GetTxTime(fullnodes []*rpcclient.Client,
	prehash *chainhash.Hash, height int64) (blockTime int64, err error) {
	txTime, err := this.GetBlockTime(height)
	if err == nil {
		return txTime, nil
	}

	txverbose, err := GetFullnode(fullnodes).GetRawTransactionVerbose(prehash)
	if err == nil {
		return txverbose.Time, nil
	}

	return 0, err
}

func (this *MongoServiceHub) GetAddrTxWrapper(fullnodes []*rpcclient.Client,
	prehash *chainhash.Hash, preindex uint32, tid int) (string, int64, error) {
	fragment := StartTrace("GetAddrTxWrapper 1")
	addr, value, err := this.GetAddrValue(prehash.String(), preindex)
	fragment.StopTrace(tid)
	return addr, value, err

	// //glog.Infof("getAddrTx1 %s %d not found", prehash.String(), preindex)
	// fragment = StartTrace("GetAddrTxWrapper 2")
	// btcutilTx, err := GetFullnode(fullnodes).GetRawTransaction(prehash)
	// fragment.StopTrace(tid)
	// if err != nil {
	// 	glog.Infof("GetRawTransaction failed preindex=%d %s", preindex, prehash)
	// 	return "", 0, err
	// }

	// out := btcutilTx.MsgTx().TxOut[preindex]
	// fragment = StartTrace("GetAddrTxWrapper 3")
	// _, addrs, _, err := txscript.ExtractPkScriptAddrs(out.PkScript, &chaincfg.MainNetParams)
	// fragment.StopTrace(tid)
	// if err != nil {
	// 	glog.Info("NewAddressScriptHashFromHash failed ", err)
	// 	return "", 0, err
	// }

	// if len(addrs) < 1 {
	// 	//errstr := fmt.Sprintf("no address prehash %s index %d", prehash.String(), preindex)
	// 	return "", 0, nil
	// }
	// //glog.Infof("getAddrTx2 %s %d", addrs[0].EncodeAddress(), out.Value)
	// return addrs[0].EncodeAddress(), out.Value, nil
}

func (this *MongoServiceHub) IsCoinBase(txin *wire.TxIn) bool {
	previousOutPoint := txin.PreviousOutPoint
	if previousOutPoint.Hash.String() == "0000000000000000000000000000000000000000000000000000000000000000" {
		//glog.Infof("coinbase %s %d", hex.EncodeToString(txin.SignatureScript), txin.Sequence)
		return true
	}

	return false
}

func (this *MongoServiceHub) DecodeInput(fullnodes []*rpcclient.Client,
	txhash string, index int, txin *wire.TxIn, height int64, tid int) (vinaddr string, vinvalue int64, preBlockTime int64, err error) {
	previousOutPoint := txin.PreviousOutPoint
	preBlockTime, err = this.GetTxTime(fullnodes, &previousOutPoint.Hash, height)
	if err != nil {
		return "", 0, 0, err
	}

	if this.IsCoinBase(txin) {
		return "", 0, preBlockTime, nil
	}

	vinaddr, vinvalue, err = this.GetAddrTxWrapper(fullnodes, &previousOutPoint.Hash, previousOutPoint.Index, tid)
	if err != nil {
		return "", 0, 0, err
	}

	if len(vinaddr) == 0 {
		return "", 0, 0, errors.New("DecodeInput len(vinaddr) == 0")
	}

	return vinaddr, vinvalue, preBlockTime, nil
}

func (this *MongoServiceHub) DecodeOutput(txout *wire.TxOut) (voutaddr string, err error) {
	_, addrs, _, err := txscript.ExtractPkScriptAddrs(txout.PkScript, &chaincfg.MainNetParams)
	if err != nil {
		glog.Info("NewAddressScriptHashFromHash failed ", err)
		return "", err
	}

	voutaddr = ""
	if len(addrs) == 0 {
		//scriptstr := string(txout.PkScript[:])
		//errstr := fmt.Sprintf("DecodeOutput len(vinaddr) == 0 %s", scriptstr)
		return voutaddr, nil
	}

	addr := addrs[0]
	voutaddr = addr.EncodeAddress()
	return voutaddr, nil
}

type BalancePair struct {
	Addr       string `json:"addr"`
	Local      int64  `json:"local"`
	BlockChair int64  `json:"blockChair"`
}

func (this *MongoServiceHub) CheckOwnerBalance(appid string, ownerid int) ([]*BalancePair, int) {
	iter := this.getMongoService(0, COL_MONITORED_ADDR).Find(bson.M{"ownerid": ownerid, "appid": appid}).Iter()
	monitoredAddrBson := &MonitoredAddrBson{}
	balancePairs := make([]*BalancePair, 0, 10)
	count := 0
	glog.Infof("CheckOwnerBalance %d start", ownerid)
	for iter.Next(monitoredAddrBson) {
		count++
		balanceFromMongo, err := this.GetAddrUnspentInfo(monitoredAddrBson.Addr)
		if err != nil {
			glog.Infof("get %s balance Failed", monitoredAddrBson.Addr)
		}
		fragment := StartTrace("checkOwneridBalance 1")
		balanceFromBlockchair, err := this.GetAddrBalanceFromBlockChair(monitoredAddrBson.Addr)
		fragment.StopTrace(0)
		if err != nil {
			glog.Infof("get %s balance from Blockchair Failed", monitoredAddrBson.Addr)
		}
		if balanceFromMongo.Balance != balanceFromBlockchair {
			balancePair := new(BalancePair)
			balancePair.Local = balanceFromMongo.Balance
			balancePair.BlockChair = balanceFromBlockchair
			balancePair.Addr = monitoredAddrBson.Addr
			balancePairs = append(balancePairs, balancePair)
		}
	}
	glog.Infof("CheckOwnerBalance %d finish", ownerid)
	return balancePairs, count
}
