package main

import (
	"fmt"

	brokerHandler "github.com/codingbot24-s/broker/cmd/broker1/handler"
	"github.com/gin-gonic/gin"
)

func main() {
	

	r := gin.Default()
	r.POST("/produce", brokerHandler.Produce)
	r.POST("/message", brokerHandler.WriteMessage)
	fmt.Println("starting the broker1 server on 8081")
	r.Run(":8081")
}
