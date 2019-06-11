package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/glog"
	"mempool.com/foundation/bitdb/common"
)

type ComparatorManager struct {
	Comparators map[string]*Comparator
}

func (this *ComparatorManager) Init() {
	this.Comparators = make(map[string]*Comparator)
}

func (this *ComparatorManager) AddComparator(coinType string, nodes []string, hub *common.MongoServiceHub) {
	comparator := new(Comparator)
	comparator.Init()
	comparator.AddNodes(nodes)
	comparator.hub = hub
	comparator.coinType = coinType
	this.Comparators[coinType] = comparator
}

type Comparator struct {
	Nodes    []string
	hub      *common.MongoServiceHub
	coinType string
}

func (this *Comparator) Init() {
	this.Nodes = make([]string, 0, 2)
	//初始化条件变量
}

func (this *Comparator) AddNodes(node []string) {
	this.Nodes = append(this.Nodes, node...)
}

func (this *Comparator) GetBalance(appid string, ownerid int, node string) (int64, error) {
	url := fmt.Sprintf("http://%s/api/monitored_address/balance/type/%s/appid/%s/ownerid/%d",
		node, this.coinType, appid, ownerid)
	resp, err := http.Get(url)
	if err != nil {
		return -1, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, err
	}
	balance, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		return -1, err
	}
	return balance, nil
}

func (this *Comparator) DemandSelfCheck(appid string, ownerid int) {
	for _, node := range this.Nodes {
		url := fmt.Sprintf("http://%s/api/test/CheckOwneridBalance/type/%s/appid/%s/ownerid/%d",
			node, this.coinType, appid, ownerid)
		go func() {
			http.Get(url)
		}()
	}
}

func (this *Comparator) ComparaLoop(hub *common.MongoServiceHub) {
	for {
		if len(this.Nodes) > 1 {
			glog.Infof("ComparaLoop start")
			this.hub.ForeachAppOwner(this)
			glog.Infof("ComparaLoop stop")
		}
		time.Sleep(time.Minute * 15)
	}
}

func (this *Comparator) Run(appid string, ownerid int) {
	firstBalance := int64(-1)
	for index, node := range this.Nodes {
		balance, err := this.GetBalance(appid, ownerid, node)
		if err != nil {
			//可能是连不上了,或者其他错误
			glog.Infof("GetBalance fault:%s", err)
			continue
		}
		//检查是否有balance不一致
		if index > 0 {
			if firstBalance != balance {
				//要求各自自我检查
				glog.Infof("btibd %d balance different", ownerid)
				this.DemandSelfCheck(appid, ownerid)
				break
			}
			continue
		}
		firstBalance = balance
	}
}
