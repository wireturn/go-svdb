package common

import (
	"math/rand"
	"strings"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/golang/glog"
)

type FullnodeMap map[string][]*rpcclient.Client

type FullnodeManager struct {
	Fullnodes FullnodeMap
}

func GetFullnode(fullnodes []*rpcclient.Client) *rpcclient.Client {
	fullnodeCnt := len(fullnodes)
	return fullnodes[rand.Int()%fullnodeCnt]
}

func (fullnodeManager *FullnodeManager) GetFullnode(coinType string) *rpcclient.Client {
	fullnodeMap := fullnodeManager.Fullnodes
	fullnodes := fullnodeMap[coinType]
	return GetFullnode(fullnodes)
}

func (fullnodeManager *FullnodeManager) Init() {
	fullnodeManager.Fullnodes = make(FullnodeMap)
}

func (fullnodeManager *FullnodeManager) AddFullnodeInfo(k string, v FullnodeInfo) ([]*rpcclient.Client, error) {
	if fullnodeManager.Fullnodes == nil {
		fullnodeManager.Fullnodes = make(FullnodeMap)
	}
	clients := make([]*rpcclient.Client, 0, 10)
	for _, url := range v.URL {
		connectInfoArray := strings.Split(url, "@")
		connCfg := &rpcclient.ConnConfig{
			User:         connectInfoArray[0],
			Pass:         connectInfoArray[1],
			Host:         connectInfoArray[2],
			HTTPPostMode: true,
			DisableTLS:   true,
		}
		client, err := rpcclient.New(connCfg, nil)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}

	fullnodeManager.Fullnodes[k] = clients
	glog.Info("AddFullnodeInfo ", k)
	return clients, nil
}
