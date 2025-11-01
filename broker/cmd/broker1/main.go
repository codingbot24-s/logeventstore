package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type Broker struct {
	NodeID int    `json:"nodeID"`
	Port   int    `json:"port"`
	Peers  []Peer `json:"peers"`
}

type Peer struct {
	NodeID int `json:"nodeID"`
	Port   int `json:"port"`
}

func main() {
	filename := "broker1.json"

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		log.Printf("error opening file", err.Error())
		return
	}
	data := make([]byte, 1024)
	_, err = f.Read(data)
	trimmed_data := bytes.Trim(data, "\x00")
	if err != nil && err != io.EOF {
		log.Printf("error reading file")
		return
	}
	var b Broker
	err = json.Unmarshal(trimmed_data, &b)
	if err != nil {
		fmt.Println("error unmarshalling", err.Error())
	}
	defer f.Close()
	fmt.Println("Broker1 nodeID", b.NodeID)
	fmt.Println("Broker1 port", b.Port)
	for _, p := range b.Peers {
		fmt.Println("Peer nodeID", p.NodeID)
		fmt.Println("Peer port", p.Port)
	}

}
