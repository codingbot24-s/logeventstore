package main

import (
	"fmt"

	"github.com/codingbot24-s/helper"
	"github.com/gin-gonic/gin"
)

func startReadConfig() {
	clusterfilename := "../../cluster_meta.json"
	brokerfilename := "broker1.json"
	leaderID, err := helper.Read_cluster_metadata(clusterfilename)
	if err != nil {
		fmt.Println("error reading cluster metadata", err)
	}
	nodeId, err := helper.ReadBrokerConfig(brokerfilename)

	if err != nil {
		fmt.Println("error reading broker config %w", err)
	}
	if helper.CheckLeader(leaderID, nodeId) {
		fmt.Println("I am the leader of the test_topic")
		return
	}
	fmt.Println("I am not the leader of the test_topic")
}

func main() {
	go func() {
		startReadConfig()
	}()

	r := gin.Default()
	r.POST("/produce", Produce)

	fmt.Println("starting the broker1 server on 8082")
	r.Run(":8082")
}

type ProduceStruct struct {
	TopicName string `json:"topicname" binding:"required"`
	Partition int    `json:"partition" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

func Produce(c *gin.Context) {
	var p ProduceStruct
	err := c.BindJSON(&p)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	fmt.Println("received message", p)
}
