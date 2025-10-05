package handlers

import (
	"fmt"
	"log"
	"net/http"

	"github.com/codingbot24-s/helper"
	"github.com/gin-gonic/gin"
)

// to create a topic send a data like this struct
type produceReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	NumberofPartitions int `json:"Npartitions" binding:"min=1"`
	Message   string `json:"message" binding:"required"`
}

type consumeReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Offset    int    `json:"offset" binding:"min=0"`
}
// we can create a topic map which will store all the topcis and we can use it to read from the topic
// create a map to store all the topics
var topicMap = make(map[string]*helper.Topic)

func Produce(c *gin.Context) {
	var req produceReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// creating a topic with hard coded partitions
	topic,err := helper.NewTopic(req.TopicName, req.NumberofPartitions)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to create topic",
			"details": err.Error(),
		})
		return
	}
	topicMap[req.TopicName] = topic
	// TODO: make this hard coded vnode dynamic 
	topic.BuildRing(3)
	// Write message to partition  
	if err := topic.WriteIntoPartition(req.Key, req.Message); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to write message",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Message written successfully",
		"topic":   req.TopicName,
		"key":     req.Key,
	})
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

// TODO: remove print statement
// create a partition in a topic
func CreatePartitionInTopic(c *gin.Context) {
	var req createPartitionReq
	if err := c.ShouldBindJSON(&req); err != nil {
		fmt.Printf("Error binding JSON: %v\n", err)
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
	pts := existingTopic.GetPartitions()
	
	filename := fmt.Sprintf("%s-partition-%d.log", req.TopicName, len(*pts))
	fmt.Println("FILE NAME IS    ", filename)
	newPart,err := helper.NewLogFile(filename)
	if err != nil {
		log.Fatalf("error creating new partition %s",err.Error())	
		return
	}
	// append new part into the currentparts
	*pts = append(*pts, newPart)

	existingTopic.BuildRing(3)
	fmt.Println("Ring is ", existingTopic.Ring)
	c.JSON(http.StatusOK,gin.H{
		"messsage" : "partition created successfully",
		"partitions": pts,
	})	
	
}
