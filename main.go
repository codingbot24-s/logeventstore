package main

import (
	"fmt"

	handlers "github.com/codingbot24-s/handler"
	"github.com/gin-gonic/gin"
)


func main() {
	r := gin.Default()
	r.POST("/produce", handlers.Produce)
	r.POST("/consume", handlers.Consume)
	fmt.Println("server started on :8080")
	r.Run()
}
