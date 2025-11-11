package broker2Helper

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/codingbot24-s/helper"
)

// When a follower restarts, it should:
// 1. Read its local log file to find the latest offset (last message index)
// 2. Call the leader’s /sync endpoint with that offset
// 3. The leader should respond with all messages after that offset
// 4. The follower then appends those to its local log.
// we can to this on another goroutine

//1.1 TODO: can we find the file pointer when we start the follower beacause topicMap is in memory so how can we read the log files??

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

	for offset := range offsetch {
		fmt.Printf("offset is %d", offset)
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
