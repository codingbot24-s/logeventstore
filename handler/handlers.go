package handlers

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

// represent 1partition
type LogFile struct {
	fileName string
	file     *os.File
	offset   int
}

// create a new logfile return the * of struct
func NewLogFile(fname string) (*LogFile, error) {
	file, err := os.OpenFile(fname, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	return &LogFile{
		fileName: fname,
		file:     file,
		offset:   0,
	}, nil
}

type Topic struct {
	partitions []*LogFile
}

// create a new topics with given partitions
func NewTopic(name string, numPartitions int) (*Topic, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("number of partitions must be positive")
	}

	partitions := make([]*LogFile, numPartitions)
	for i := 0; i < numPartitions; i++ {
		logFile, err := NewLogFile(fmt.Sprintf("%s-partition-%d.log", name, i))
		if err != nil {
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[i] = logFile
	}

	return &Topic{
		partitions: partitions,
	}, nil
}

// write into correct part
func (t *Topic) writeIntoPartition(key string, message string) error {
	part := t.get_partition(key)
	return t.partitions[part].writeIntoLogFile(message)
}

// read from correct part

func (t *Topic) readFromPartiton(key string, offset int) (string, error) {
	part := t.get_partition(key)
	return t.partitions[part].readFileFromOffset(offset)
}

// close all partitions

func (t *Topic) CloseP() error {
	var Eerr error
	for _, p := range t.partitions {
		if err := p.Close(); err != nil {
			Eerr = err
		}
	}

	return Eerr
}

// logfile write
func (l *LogFile) writeIntoLogFile(str string) error {
	if l.file == nil {
		return fmt.Errorf("log file is not initialized")
	}
	n, err := l.file.Write([]byte(str))
	if err != nil {
		log.Fatal("error writing in logfile", l.fileName, err)
	}
	l.offset = l.offset + n
	fmt.Printf("Writing successfull in %s\n", l.fileName)
	return nil
}

// log file read
func (l *LogFile) readFileFromOffset(offset int) (string, error) {
	if l.file == nil {
		return "", fmt.Errorf("log file is not initialized")
	}
	buf := make([]byte, 1024)
	n, err := l.file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return "", fmt.Errorf("failed to read from log file: %w", err)
	}
	return string(buf[:n]), nil
}

func (t *Topic) get_partition(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % len(t.partitions)
}

// close one log file
func (l *LogFile) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

type produceReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

type consumeReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Offset    int    `json:"offset" binding:"min=0"` 
}

// we can create a topic map which will store all the topcis and we can use it to read from the topic
// create a map to store all the topics
var topicMap = make(map[string]*Topic)

func Produce(c *gin.Context) {
	var req produceReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}
	// create a topic
	topic, err := NewTopic(req.TopicName, 2)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to create topic",
			"details": err.Error(),
		})
		return
	}
	topicMap[req.TopicName] = topic

	// Write message to partition
	if err := topic.writeIntoPartition(req.Key, req.Message); err != nil {
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
	str, err := t.readFromPartiton(req.Key, req.Offset)
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
	//TODO: this will close the all the log file of all topic after reading from single topic this is the problem we need to solve this
	// 
	for _, t := range topicMap {
		err := t.CloseP()	
		if err != nil {
			fmt.Printf("Error closing topic: %v\n", err)
		}
	}	
}
