package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Peer struct {
	NodeID int `json:"nodeID"`
	Port   int `json:"port"`
}

type Partition struct {
	LeaderID int   `json:"leaderID"`
	Replicas []int `json:"replicas"`
}
type Topic struct {
	Partitions map[string]Partition
}

type Broker struct {
	NodeID int    `json:"nodeID"`
	Port   int    `json:"port"`
	Peers  []Peer `json:"peers"`
	Topics map[string]Topic
}
// map of topic and topic is map of partition
type clusterMap map[string]map[string]Partition

func read_clusrter_meta(filename string) (*clusterMap, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	data := make([]byte, 1024)
	_, err = f.Read(data)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	trimmed_data := bytes.Trim(data, "\x00")
	var c clusterMap
	// Marshal the data into a clusterMap
	err = json.Unmarshal(trimmed_data, &c)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling: %w", err)
	}

	// returing the clusterMap pointer
	return &c, nil
}

// func readBrokerConfig(filename string) (*Broker, error) {

// }

func main() {
	filename := "cluster_meta.json"
	_, err := read_clusrter_meta(filename)
	if err != nil {
		fmt.Println("error reading cluster meta", err.Error())
		return
	}
}
