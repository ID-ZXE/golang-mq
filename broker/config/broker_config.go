package config

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

//{
//  "clusterName":"my_cluster",
//  "brokerName":"my_broker",
//  "brokerId":0,
//  "body":{
//      "topics":[
//          {
//              "topic":"topic_1",
//              "queue":2
//          },
//          {
//              "topic":"topic_2",
//              "queue":4
//          },
//          {
//              "topic":"topic_3",
//              "queue":6
//          }
//      ]
//  }
//}

type TopicUnit struct {
	Topic string `json:"topic"` //topic名称
	Queue int    `json:"queue"` //队列数量
}

type Body struct {
	Topics []*TopicUnit `json:"topics"`
}

type BrokerConfigFile struct {
	ClusterName string `json:"clusterName"`
	BrokerName  string `json:"brokerName"`
	BrokerId    int    `json:"brokerId"`
	Body        *Body  `json:"body"`
}

var (
	BrokerName string
	ConfigBody *Body
)

func init() {
	brokerConfigFile := NewBrokerConfigFileAndLoad()
	BrokerName = brokerConfigFile.BrokerName
	ConfigBody = brokerConfigFile.Body
}

func NewBrokerConfigFileAndLoad() *BrokerConfigFile {
	brokerConfigFile := &BrokerConfigFile{}
	brokerConfigFile.loadBrokerConfigFile()
	return brokerConfigFile
}

func (brokerConfigFile *BrokerConfigFile) loadBrokerConfigFile() {
	file, err := os.OpenFile(BrokerConfigPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	data := make([]byte, 0)
	for {
		line, _, err := reader.ReadLine()
		data = append(data, line...)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
	}
	err = json.Unmarshal(data, brokerConfigFile)
	if err != nil {
		panic(err)
	}
}
