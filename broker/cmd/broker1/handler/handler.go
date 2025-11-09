package brokerHandler

import (
	"net/http"

	"github.com/codingbot24-s/helper"
	"github.com/gin-gonic/gin"
)

type createTopicReq struct {
	TopicName          string `json:"topicname" binding:"required"`
	NumberofPartitions int    `json:"Npartitions" binding:"min=1"`
	NumberofNodes      int    `json:"numberofnodes" binding:"min=1"`
}

// produce will create a topic with given name and number of partitions
var topicMap = make(map[string]*helper.Topic)

func Produce(c *gin.Context) {
	var req createTopicReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// creating a topic
	topic, err := helper.NewTopic(req.TopicName, req.NumberofPartitions)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to create topic",
			"details": err.Error(),
		})
		return
	}
	// insert a topic in the map
	topicMap[req.TopicName] = topic
	// build the ring with the number of nodes
	topic.BuildRing(req.NumberofNodes)

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Topic created successfully",
		"topic":   req.TopicName,
	})
}

type writeMessageReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Message   string `json:"message" binding:"required"`
}
// write message will write a message to a topic partition 
func WriteMessage(c *gin.Context) {
	var req writeMessageReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// find the topin the map 
	topic, ok := topicMap[req.TopicName]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid topic name",
			"details": "Topic does not exist",
		})
		return
	}
	// write the message to the topic partition
	if err := topic.WriteIntoPartition(req.Key, req.Message); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to write message",
			"details": err.Error(),
		})
		return
	}
	//TODO: after wiriting into the log file we need to read cluster metadata to find the replicas for that partition 
	// we can spawn the goroutines that will read the file and then pass the data throw channel 
	go func () {
		// it will return the byte 
		//b,err := helper.ReadClusterMetadataAndGetTheClusterMetadataData("./broker/cluster_meta.json")
		// if err != nil {}
	}()  
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Message written successfully",
		"topic":   req.TopicName,
		"key":     req.Key,
	})
}
