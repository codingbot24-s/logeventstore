package main

import (
	"fmt"

	handlers "github.com/codingbot24-s/handler"
	"github.com/gin-gonic/gin"
)

// represent 1partition

// TODO: expose the api for writing
func main() {
	r := gin.Default()
	r.POST("/produce", handlers.Produce)
	r.POST("/consume", handlers.Consume)
	fmt.Println("server started on :8080")
	r.Run()
}
