package main

import (
	"fmt"

	handlers "github.com/codingbot24-s/handler"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	// will create a topic
	r.POST("/topic", handlers.Produce)
	// will write a message to a topic
	r.POST("/message", handlers.WriteMessage)
	// will read a message from a topic
	r.POST("/consume", handlers.Consume)
	// will create a partition in a topic
	r.POST("/createpartition", handlers.CreatePartitionInTopic)
	fmt.Println("server started on :8080")
	r.Run()
}
