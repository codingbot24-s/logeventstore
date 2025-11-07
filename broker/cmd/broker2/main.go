package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
)



func main() {
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
