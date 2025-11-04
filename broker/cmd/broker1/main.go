package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"github.com/gin-gonic/gin"
)

type Peer struct {
	NodeID int `json:"nodeID"`
	Port   int `json:"port"`
}

type Partition struct {
	LeaderID int   `json:"leaderID"`
	Replicas []int `json:"replicas"`
}
type Topic struct {
	Partitions map[string]Partition
}

type Broker struct {
	NodeID int    `json:"nodeID"`
	Port   int    `json:"port"`
	Peers  []Peer `json:"peers"`
	Topics map[string]Topic
}

// map of topic and topic is map of partition
// with this we can correclty read the cluster metadata
// TODO: we need to create a broker structer with this map data
type clusterMap map[string]map[string]Partition

func read_cluster_metadata(filename string) (int, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	data := make([]byte, 1024)
	_, err = f.Read(data)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	trimmed_data := bytes.Trim(data, "\x00")

	// Marshal the data into a clusterMap
	var c clusterMap
	err = json.Unmarshal(trimmed_data, &c)
	if err != nil {
		return 0, fmt.Errorf("error unmarshalling: %w", err)
	}

	return c["test_topic"]["0"].LeaderID, nil
}

func readBrokerConfig(filename string) (int, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %w", err)
	}

	data := make([]byte, 1023)
	_, err = f.Read(data)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	trimmed_data := bytes.Trim(data, "\x00")

	var b Broker
	err = json.Unmarshal(trimmed_data, &b)

	if err != nil {
		return 0, fmt.Errorf("error unmarshaling json %w", err)
	}

	return b.NodeID, nil
}

func checkLeader(lId, nodeId int) bool {
	if lId == nodeId {
		return true
	}
	return false
}
// TODO: we need to start the gin server here 
// TODO: we can make it concurrent 
func startReadConfig() {
	clusterfilename := "../../cluster_meta.json"
	brokerfilename := "broker1.json"
	leaderID, err := read_cluster_metadata(clusterfilename)
	if err != nil {
		fmt.Println("error reading cluster metadata", err)
	}
	nodeId, err := readBrokerConfig(brokerfilename)

	if err != nil {
		fmt.Println("error reading broker config %w", err)
	}
	if checkLeader(leaderID, nodeId) {
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
	r.POST("/produce",Produce)

	fmt.Println("starting the server on 8081")
  	r.Run(":8082")
}

type ProduceStruct struct {
	TopicName string `json:"topicname" binding:"required"`
	Partition int    `json:"partition" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

func Produce(c* gin.Context) {
	var p ProduceStruct
	err := c.BindJSON(&p)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	
	fmt.Println("received message", p)
}
