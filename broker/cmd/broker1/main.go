package main

import (
	"fmt"

	brokerHandler1 "github.com/codingbot24-s/broker/cmd/broker1/handler"
	"github.com/gin-gonic/gin"
)

func main() {
	

	r := gin.Default()
	r.POST("/produce", brokerHandler1.Produce)
	r.POST("/message", brokerHandler1.WriteMessage)
	r.GET("/sync", brokerHandler1.Sync)
	fmt.Println("starting the broker1 server on 8081")
	r.Run(":8081")
}
