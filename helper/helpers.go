package helper

import (
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sort"
)

// represent one partition
type LogFile struct {
	FileName string
	file     *os.File
	offset   int
}

// create a new logfile return the * of struct
func NewLogFile(fname string) (*LogFile, error) {
	file, err := os.OpenFile(fname, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	return &LogFile{
		FileName: fname,
		file:     file,
		offset:   0,
	}, nil
}

// we can craete a hash when we are creating the partition
type Node struct {
	Hash           int
	PartitionIndex int
}
type Topic struct {
	partitions []*LogFile
	Ring       []Node
}

// building ring with Nvnode append it to the ring with hash
func (t *Topic) BuildRing(vNode int) {
	for p := 0; p < len(t.partitions); p++ {
		for v := range vNode {
			str := fmt.Sprintf("partition-%d-node-%d", p, v)
			hashVal := crc32.ChecksumIEEE([]byte(str))

			n := Node{
				Hash:           int(hashVal),
				PartitionIndex: p,
			}
			t.Ring = append(t.Ring, n)
		}

	}
	// sort the ring slice in < > 
	sort.Slice(t.Ring, func(i, j int) bool {
		return t.Ring[i].Hash < t.Ring[j].Hash
	})
}

// create a new topics with given partitions
func NewTopic(name string, numPartitions int) (*Topic, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("number of partitions must be positive")
	}

	partitions := make([]*LogFile, numPartitions)
	for i := 0; i < numPartitions; i++ {
		logFile, err := NewLogFile(fmt.Sprintf("%s-partition-%d.log", name, i))
		if err != nil {
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[i] = logFile
	}

	return &Topic{
		partitions: partitions,
	}, nil
}

// write into correct part
func (t *Topic) WriteIntoPartition(key string, message string) error {
	part := t.Get_partition(key)
	return t.partitions[part].WriteIntoLogFile(message)
}

// read from correct part

func (t *Topic) ReadFromPartiton(key string, offset int) (string, error) {
	part := t.Get_partition(key)
	return t.partitions[part].ReadFileFromOffset(offset)
}


func (t *Topic) GetPartitions() *[]*LogFile {
	return &t.partitions
}

func (t *Topic) CloseP() error {
	var Eerr error
	for _, p := range t.partitions {
		if err := p.Close(); err != nil {
			Eerr = err
		}
	}

	return Eerr
}

// logfile write
func (l *LogFile) WriteIntoLogFile(str string) error {
	if l.file == nil {
		return fmt.Errorf("log file is not initialized")
	}
	n, err := l.file.Write([]byte(str))
	if err != nil {
		log.Fatal("error writing in logfile", l.FileName, err)
	}
	l.offset = l.offset + n
	fmt.Printf("Writing successfull in %s\n", l.FileName)
	return nil
}

// log file read
func (l *LogFile) ReadFileFromOffset(offset int) (string, error) {
	if l.file == nil {
		return "", fmt.Errorf("log file is not initialized")
	}
	buf := make([]byte, 1024)
	n, err := l.file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return "", fmt.Errorf("failed to read from log file: %w", err)
	}
	return string(buf[:n]), nil
}

// get the partition number
func (t *Topic) Get_partition(key string) int {
	hash := crc32.ChecksumIEEE([]byte(key))
	// find the hash in the ring == this hash or > hash
	// with binary search
	low := 0
	high := len(t.Ring) - 1
	for low <= high {

		mid := low + (high-low)/2

		if t.Ring[mid].Hash == int(hash) {
			return t.Ring[mid].PartitionIndex
		} else if t.Ring[mid].Hash > int(hash) {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	if low < len(t.Ring) {
		return t.Ring[low].PartitionIndex
	}
	// first entry of ring
	return t.Ring[0].PartitionIndex
}

// close one log file
func (l *LogFile) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// TODO: impl add and remove partition then update the ring move affected keys and message to new partition and verify rebalancing 	

