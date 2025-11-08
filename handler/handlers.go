package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/codingbot24-s/helper"
	"github.com/gin-gonic/gin"
)

// to create a topic send a data like this struct
// TODO: Messages are not going in the new partitions only going in partitions that have been created with produce in { creation Time } after taht messages are only going in that Npartitions SOLVE THIS

// TODO: send this type of request on produce to leader broker
type createTopicReq struct {
	TopicName          string `json:"topicname" binding:"required"`
	NumberofPartitions int    `json:"Npartitions" binding:"min=1"`
	NumberofNodes      int    `json:"numberofnodes" binding:"min=1"`
}

// we can create a topic map which will store all the topcis and we can use it to read from the topic
// create a map to store all the topics
var topicMap = make(map[string]*helper.Topic)

// controller will find the leader and route the request to the correct leader broker
//TODO:  test this produce route in postman is it working
// THIS WILL CREATE THE LOG FILES IN THE LEADER BROKER DIR
func RouteProduce(c *gin.Context) {
	var req createTopicReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// send the post request to leader broker with this request struct to create a topic
	// get the broker slice
	// pass the broker slice to getleader to get the port
	jsonByte, err := json.Marshal(req)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// TODO: we need this port to be dynamic by leader we need to get the leader from brokers slice
	// we can create the broker slice here if

	for i := 1; i < 2; i++ {
		confFile := fmt.Sprintf("./broker/cmd/broker%d/broker%d.json", i, i)
		_, err := helper.CreateBroker(confFile, "./broker/cluster_meta.json")

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "error creating broker",
				"details": err.Error(),
			})
			return
		}

	}
	brokerSlice, err := helper.GetBrokerSlice()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "error getting broker slice",
			"details": err.Error(),
		})
		return
	}

	port, err := helper.FindLeaderPort(*brokerSlice, "test_topic", "0")
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"Error":  "leader not found",
			"detail": err.Error(),
		})
		return
	}
	addr := fmt.Sprintf("http://localhost:%d/produce", port)
	resp, err := http.Post(addr, "application/json", bytes.NewBuffer(jsonByte))

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "error sending post req",
			"details": err.Error(),
		})
		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Error reading response",
			"details": err.Error(),
		})
		return
	}

	if resp.StatusCode == http.StatusOK {
		c.JSON(http.StatusOK, gin.H{
			"status":   "success",
			"message":  "sent request successfully",
			"response": string(body),
		})

		return
	}
}

type writeMessageReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Message   string `json:"message" binding:"required"`
}
// THIS WILL WRITE IN LEADER BROKER LOG FILE
func WriteMessage(c *gin.Context) {
	//TODO:  we need to define this slice somewhere else so every handler could access it for leader port

	// we can get the broker slice by this function
	brokerSlice, err := helper.GetBrokerSlice()
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"Error":  "erorr getting broker slice",
			"detail": err.Error(),
		})
		return
	}
	// passs the broker slice here to get the leader port
	port, err := helper.FindLeaderPort(*brokerSlice, "test_topic", "0")
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"Error":  "leader not found",
			"detail": err.Error(),
		})
		return
	}
	var req writeMessageReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
	}

	// send the post request on this node id to /produce
	addr := fmt.Sprintf("http://localhost:%d/message", port)
	fmt.Println("addr is ", addr)

	resp, err := http.Post(addr, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Error sending post req",
			"details": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Error reading response",
			"details": err.Error(),
		})
		return
	}

	if resp.StatusCode == http.StatusOK {
		c.JSON(http.StatusOK, gin.H{
			"status":   "success",
			"message":  "sent request successfully",
			"response": string(body),
		})
	} else {
		c.JSON(http.StatusBadGateway, gin.H{
			"error":    "Request failed",
			"status":   resp.StatusCode,
			"response": string(body),
		})
	}
}

type consumeReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Offset    int    `json:"offset" binding:"omitempty"`
}

func Consume(c *gin.Context) {
	var req consumeReq

	if err := c.ShouldBindJSON(&req); err != nil {
		fmt.Printf("Error binding JSON: %v\n", err)
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	// get the topic from the map
	t, ok := topicMap[req.TopicName]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid topic name",
			"details": "Topic does not exist",
		})
		return
	}
	str, err := t.ReadFromPartiton(req.Key, req.Offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to read from log file",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": str,
		"details": "message has been read closing the files",
	})
	//TODO: this will close the all the log file of all topic after reading from single topic this is the problem we need to solve this only close the one file not all file
	// for _, t := range topicMap {
	// 	err := t.CloseP()
	// 	if err != nil {
	// 		fmt.Printf("Error closing topic: %v\n", err)
	// 	}
	// }
}

type createPartitionReq struct {
	TopicName string `json:"topicname" binding:"required"`
}

// create a partition in a topic
func CreatePartitionInTopic(c *gin.Context) {
	var req createPartitionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	existingTopic, ok := topicMap[req.TopicName]

	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{

			"error":   "Invalid topic name",
			"details": "Topic does not exist",
		})
		return
	}
	// get all the logFiles with given topic name
	pts := existingTopic.GetAllPartitions()
	filename := fmt.Sprintf("%s-partition-%d.log", req.TopicName, len(*pts))
	newPart, err := helper.NewLogFile(filename)
	if err != nil {
		log.Fatalf("error creating new partition %s", err.Error())
		return
	}
	// copy the old ring
	oldRing := make([]helper.Node, len(existingTopic.Ring))
	copy(oldRing, existingTopic.Ring)
	// append new part into the currentparts
	*pts = append(*pts, newPart)
	// build the new ring
	existingTopic.BuildRing(3)

	newNodes := make([]helper.Node, 0, len(existingTopic.Ring))
	// oldNodesMap is a map of old nodes
	oldNodesMap := make(map[string]bool)
	for _, node := range oldRing {
		key := fmt.Sprintf("%d-%d", node.Hash, node.PartitionIndex)
		oldNodesMap[key] = true
	}

	// loop and compare all the node
	for _, node := range existingTopic.Ring {
		key := fmt.Sprintf("%d-%d", node.Hash, node.PartitionIndex)
		if !oldNodesMap[key] {
			newNodes = append(newNodes, node)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"messsage":            "partition created successfully",
		"existing partitions": pts,
	})

}

type Replication struct {
	TopicName string `json:"topicname" binding:"required"`
	Partition int    `json:"partition" binding:"required"`
	Offset    int    `json:"offset" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

func Replicate(c *gin.Context) {
	var req Replication
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "message replicated successfully",
	})
}
