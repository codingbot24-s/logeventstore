package brokerHandler2

import (
	"fmt"
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

type replicateReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

func Replicate(c *gin.Context) {
	var req replicateReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// TODO: ADD the write logic here
	fmt.Println("recived request ", req)
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
	})
}
