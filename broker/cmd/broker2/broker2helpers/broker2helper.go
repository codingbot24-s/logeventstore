package broker2Helper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/codingbot24-s/helper"
)

// When a follower restarts, it should:
// 1. Read its local log file to find the latest offset (last message index) // DONE
// 2. Call the leader’s /sync endpoint with that offset
// 3. The leader should respond with all messages after that offset
// 4. The follower then appends those to its local log.
// we can to this on another goroutine

type ResposneStruct struct {
	Status    string `json:"status" binding:"required"`
	Topic     string `json:"topic" binding:"required"`
	Partition string 	`json:"partition" binding:"required"`
	Messages  []string `json:"messages" binding:"required"`
}

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
	// get the offset for every files
	for _, f := range files {
		wg.Add(1)
		go getOffset(f, offsetch, &wg)
	}

	go func() {
		wg.Wait()
		close(offsetch)
	}()
	urlch := make(chan string)
	part := 0
	// for every offset get the url
	for offset := range offsetch {
		fmt.Printf("offset is %d\n", offset)
		wg.Add(1)
		p := part
		off := offset
		go func(offsetVal int64, partition int) {
			if err := SyncWithLeader(offsetVal, topicName, partition, urlch, &wg); err != nil {
				fmt.Printf("SyncWithLeader(part=%d) error: %v\n", partition, err)
			}
		}(off, p)
		part = part + 1
	}

	go func() {
		wg.Wait()
		close(urlch)
	}()

	respch := make(chan ResposneStruct)
	// send the request to every url
	for url := range urlch {
		wg.Add(1)
		u := url
		go func(target string) {
			if err := SendRequest(target, respch, &wg); err != nil {
				fmt.Printf("SendRequest(url=%s) error: %v\n", target, err)
			}
		}(u)
	}

	go func() {
		wg.Wait()
		close(respch)
	}()

	// write the response in log file
	for resp := range respch {
		wg.Add(1)
		r := resp
		go func(rr ResposneStruct) {
			if err := WriteResponse(rr.Topic, rr.Partition, rr.Messages, &wg); err != nil {
				fmt.Printf("error in writing response: %v\n", err)
			}
		}(r)
	}

	
	wg.Wait()

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
func SyncWithLeader(offset int64, topic string, partition int, respch chan string, wg *sync.WaitGroup) error {
	defer wg.Done()
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
		return fmt.Errorf("erorr no broker found in slice %w", err)
	}
	// we can get the leader id from cluster metadata
	strPart := fmt.Sprintf("%d", partition)
	strOffset := fmt.Sprintf("%d", offset)
	leaderPort, err := helper.FindLeaderPort(bs, topic, strPart)
	if err != nil {
		return fmt.Errorf("error getting leader %w", err)
	}
	// send the request on this leader port on /sync with offset
	baseUrl := fmt.Sprintf("http://localhost:%d/sync", leaderPort)
	base, err := url.Parse(baseUrl)
	if err != nil {
		return fmt.Errorf("error parsing url %w", err)
	}
	queryParams := url.Values{}
	queryParams.Add("topic", topic)
	queryParams.Add("partition", strPart)
	queryParams.Add("offset", strOffset)
	base.RawQuery = queryParams.Encode()
	respch <- base.String()
	return nil
}

func SendRequest(url string, respch chan ResposneStruct, wg *sync.WaitGroup) error {
	// ensure wg.Done is called exactly once for this goroutine
	defer wg.Done()

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error sending request %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("something went wrong %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response %w", err)
	}

	var respReq ResposneStruct
	if err := json.Unmarshal(body,&respReq); err != nil {
		return fmt.Errorf("error unmarshalling json %w", err)
	}
	
	respch <- respReq

	return nil
}
func WriteResponse(topicName string, partition string, message []string, wg *sync.WaitGroup) error {
	defer wg.Done()
	for _, msg := range message {
		fmt.Printf("message is %s\n", msg)
	}
	fileName := fmt.Sprintf("%s-partition-%s.log", topicName, partition)
	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("error opening file %w", err)
	}
	defer fd.Close()

	var buf bytes.Buffer
	for _, msg := range message {
		if msg == "" {
			continue
		}
		buf.WriteString(msg)
		// ensure each message is on its own line
		if !strings.HasSuffix(msg, "\n") {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return nil
	}

	if _, err := fd.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("error writing to file %w", err)
	}

	return nil
}
