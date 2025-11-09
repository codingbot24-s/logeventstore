package brokerHandler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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

	// Read cluster metadata
	b, err := helper.ReadClusterMetadataAndGetTheClusterMetadataData("../../cluster_meta.json")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "failed to read cluster metadata", "error": err.Error()})
		return
	}
	// load cluster metadata
	topicsMapPtr, err := helper.LoadClusterMetadata(b)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "failed to load cluster metadata", "error": err.Error()})
		return
	}
	// topic map pointer that hold topics in the map 
	topics := *topicsMapPtr
	partitionMap, exist := topics[req.TopicName]
	if !exist {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "topic metadata not found", "topic": req.TopicName})
		return
	}

	// partition 0 for now
	pmeta, ok := partitionMap["0"]
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "partition metadata not found", "partition": "0"})
		return
	}

	replicas := pmeta.Replicas
	// get the broker slice*
	brokerSlicePtr, err := helper.GetBrokerSlice()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "error getting broker slice", "error": err.Error()})
		return
	}
	bs := *brokerSlicePtr
	// marshall the request data
	jsonData, err := json.Marshal(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "failed", "message": "error marshalling json", "error": err.Error()})
		return
	}
	// ?
	client := &http.Client{Timeout: 5 * time.Second}
	var replicationErrs []string

	
	for _, replicaID := range replicas {
		var target *helper.Broker
		for _, b := range bs {
			if b.NodeID == replicaID {
				target = b
				break
			}
		}
		if target == nil {
			replicationErrs = append(replicationErrs, fmt.Sprintf("broker for replica %d not found", replicaID))
			continue
		}

		addr := fmt.Sprintf("http://localhost:%d/replicate", target.Port)
		resp, err := client.Post(addr, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			replicationErrs = append(replicationErrs, fmt.Sprintf("post to %s failed: %v", addr, err))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			replicationErrs = append(replicationErrs, fmt.Sprintf("reading response from %s failed: %v", addr, err))
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			replicationErrs = append(replicationErrs, fmt.Sprintf("replica %d returned status %d: %s", replicaID, resp.StatusCode, string(body)))
			continue
		}
	}

	if len(replicationErrs) > 0 {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "partial_failure",
			"message": "message written locally but replication had errors",
			"errors":  replicationErrs,
			"topic":   req.TopicName,
			"key":     req.Key,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Message written and replicated successfully",
		"topic":   req.TopicName,
		"key":     req.Key,
	})

}
