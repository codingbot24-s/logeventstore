package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)



func main() {
	r := gin.Default()
	r.POST("/replicate",replicate)
	
	fmt.Println("starting the broker1 server on 8082")
	r.Run(":8082")
}

type replicateReq struct {
	TopicName string `json:"topicname" binding:"required"`
	Key       string `json:"key" binding:"required"`
	Message   string `json:"message" binding:"required"`
}

func replicate(c *gin.Context) {
	// var req replicateReq 
	// if err := c.BindJSON(req);err != nil {
	// 	c.JSON(http.StatusBadRequest, gin.H{
	// 		"error":   "Invalid request format",
	// 		"details": err.Error(),
	// 	})
	// 	return
	// }
	c.JSON(http.StatusOK,gin.H{
		"status" : "success",

	})
}

