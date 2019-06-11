package main

import (
	"fmt"
	"net/http"

	"github.com/golang/glog"
)

const (
	ADD_MONITORED_ADDR_URL_PRE = "/api/monitored_address/add/type"
)

type Synchronizer struct {
	Nodes []string
}

func (this *Synchronizer) Init() {
	this.Nodes = make([]string, 0, 2)
}

// func (this *Synchronizer) AddNode(node string) {
// 	this.Nodes = append(this.Nodes, node)
// }

func (this *Synchronizer) AddNodes(nodes []string) {
	this.Nodes = append(this.Nodes, nodes...)
}

func (this *Synchronizer) Notify(coinType string, appid string, ownerid string, addr string) {
	//对于每一个节点通知
	for _, node := range this.Nodes {
		url := fmt.Sprintf("http://%s/api/monitored_address/sync/type/%s/appid/%s/ownerid/%s/addr/%s",
			node, coinType, appid, ownerid, addr)
		go func() {
			_, err := http.Get(url)
			if err != nil {
				glog.Infof("Notify:%s", err)
			}
		}()
	}
}
