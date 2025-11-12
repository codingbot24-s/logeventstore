package broker2Helper

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/codingbot24-s/helper"
)

// When a follower restarts, it should:
// 1. Read its local log file to find the latest offset (last message index) // DONE
// 2. Call the leader’s /sync endpoint with that offset
// 3. The leader should respond with all messages after that offset
// 4. The follower then appends those to its local log.
// we can to this on another goroutine

type resp struct{}

func StartReadingLogFiles(topicName string) error {
	metadata, err := helper.ReadClusterMetadataAndGetTheClusterMetadataData("../../cluster_meta.json")

	if err != nil {
		return fmt.Errorf("error reading cluster metadata: %w", err)
	}

	t, err := helper.LoadClusterMetadata(metadata)
	if err != nil {
		return fmt.Errorf("error loading cluster metadata: %w", err)
	}
	topicMap := *t
	partitions, ok := topicMap[topicName]
	if !ok {
		return fmt.Errorf("topic %s not found", topicName)
	}
	// every partition
	files := make([]string, 0)
	for k := range partitions {
		partName := fmt.Sprintf("%s-partition-%s.log", topicName, k)
		files = append(files, partName)
	}

	var wg sync.WaitGroup

	offsetch := make(chan int64)
	for _, f := range files {
		wg.Add(1)
		go getOffset(f, offsetch, &wg)
	}

	go func() {
		wg.Wait()
		close(offsetch)
	}()
	// partition number

	respch := make(chan resp)
	part := 0
	for offset := range offsetch {
		go SyncWithLeader(offset, topicName, part,respch)
		part = part + 1
	}

	return nil
}

func getOffset(filename string, offsetch chan int64, wg *sync.WaitGroup) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		wg.Done()
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		wg.Done()
		return fmt.Errorf("failed getting offset: %w", err)
	}

	offsetch <- offset
	wg.Done()

	return nil
}

// after we get the offset send a get req to /sync
func SyncWithLeader(offset int64, topic string, partition int, respch chan resp) error {
	// how to find the leader port on follower
	// we can fetch the broker slice and findout the leader
	// one more http call

	// get the broker slice
	resp, err := http.Get("http://localhost:8080/getbrokers")
	if err != nil {
		return fmt.Errorf("error sending request %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("something went wrong %w", err)
	}

	var bs []*helper.Broker
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&bs); err != nil {
		return fmt.Errorf("error decoding json %s", err)
	}

	if len(bs) == 0 {
		return fmt.Errorf("erorr no broker found in slice")
	}
	// we can get the leader id from cluster metadata
	strPart := fmt.Sprintf("%d",partition)
	leaderPort, err := helper.FindLeaderPort(bs, topic,strPart)
	if err != nil {
		return fmt.Errorf("error getting leader %w", err)
	}
	// send the request on this leader port on /sync with offset
	baseUrl := fmt.Sprintf("http://localhost:%d/sync", leaderPort)
	queryParams := url.Values{}
	queryParams.Add(baseUrl, "")
	// create a url
	return nil
}
