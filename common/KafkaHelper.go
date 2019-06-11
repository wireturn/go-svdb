package common

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

const topic = "MonitoredAddr_"

type KafkaManager struct {
	KafkaProductors map[string]*MonitroedAddrProductor
	KafkaConsumers  map[string]*MonitroedAddrConsumer
}

type MonitoredAddrInfo struct {
	BitdbIp string `json:"bitdbIp"`
	Appid   string `json:"appid"`
	Ownerid int    `json:"ownerid"`
	Addr    string `json:"addr"`
}

// type KafkaProductor interface {
// 	Init(url []string, Topic string)
// 	Notify(message interface{}) error
// }

type MonitroedAddrProductor struct {
	KafkaUrl []string
	Topic    string
}

// type KafkaConsumer interface {
// 	StartConsume()
// 	Init(url []string, Topic string)
// }

type MonitroedAddrConsumer struct {
	KafkaUrl []string
	Topic    string
	hub      *MongoServiceHub
}

func (this *KafkaManager) Init() {
	this.KafkaProductors = make(map[string]*MonitroedAddrProductor)
	this.KafkaConsumers = make(map[string]*MonitroedAddrConsumer)
}

func (this *KafkaManager) AddKafkaProductor(url []string, coinType string) {
	monitroedAddrProductor := new(MonitroedAddrProductor)
	monitroedAddrProductor.Init(url, coinType)
	this.KafkaProductors[coinType] = monitroedAddrProductor
}

func (this *KafkaManager) AddKafkaConsumer(url []string, coinType string, hub *MongoServiceHub) {
	monitroedAddrConsumer := new(MonitroedAddrConsumer)
	monitroedAddrConsumer.Init(url, coinType, hub)
	this.KafkaConsumers[coinType] = monitroedAddrConsumer
}

func (this *MonitroedAddrProductor) Init(url []string, coinType string) {
	this.KafkaUrl = url
	this.Topic = topic + coinType
}

func (this *MonitroedAddrConsumer) Init(url []string, coinType string, hub *MongoServiceHub) {
	this.KafkaUrl = url
	this.Topic = topic + coinType
	this.hub = hub
}

func (this *MonitroedAddrProductor) Notify(monitoredAddrInfo *MonitoredAddrInfo) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewSyncProducer(this.KafkaUrl, config)
	defer p.Close()
	if err != nil {
		return err
	}
	messageByte, err := json.Marshal(monitoredAddrInfo)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: this.Topic,
		Value: sarama.ByteEncoder(messageByte),
	}
	if _, _, err := p.SendMessage(msg); err != nil {
		return err
	}
	return nil
}

func (this *MonitroedAddrConsumer) ConsumeLoop(bitdbIp string) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(this.KafkaUrl, config)
	if err != nil {
		glog.Infof("Consume:%d", err)
		glog.Flush()
		panic(err)
	}
	partitionConsumer, err := consumer.ConsumePartition(this.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	glog.Infof("consumeLoop start")
	for msg := range partitionConsumer.Messages() {
		monitoredAddrInfo := &MonitoredAddrInfo{}
		msgValueByte := []byte(msg.Value)
		err := json.Unmarshal(msgValueByte, monitoredAddrInfo)
		if err != nil {
			glog.Infof("consumeLoop %s", err)
		}
		if monitoredAddrInfo.BitdbIp != bitdbIp {
			err = this.hub.AddMonitoredAddr(monitoredAddrInfo.Appid, monitoredAddrInfo.Ownerid, monitoredAddrInfo.Addr)
			if err != nil {
				glog.Infof("consumeLoop %d %s %s", monitoredAddrInfo.Ownerid, monitoredAddrInfo.Addr, err)
			}
		}
	}
	glog.Infof("consumeLoop stop")
}
