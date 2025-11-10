package main

import (
	"fmt"
	brokerHandler2 "github.com/codingbot24-s/broker/cmd/broker2/handler.go"
	"github.com/gin-gonic/gin"
)



func main() {
	r := gin.Default()
	r.POST("/replicate", brokerHandler2.Replicate)
	r.POST("/produce",brokerHandler2.Produce)	
	fmt.Println("starting the broker1 server on 8082")
	r.Run(":8082")
}

