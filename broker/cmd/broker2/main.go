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

func replicate(c *gin.Context) {
	c.JSON(http.StatusOK,gin.H{
		"message" : "hello from replicate",
	})
}

