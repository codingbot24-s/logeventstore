package helper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

// represent one partition
type LogFile struct {
	FileName string
	file     *os.File
	mu       sync.Mutex
	index    []*Message
}

// every message will have its offset and Hash
type Message struct {
	Offset  int
	Hash    int
	Message string
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
		for v := 0; v < vNode; v++ {
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
	part, err := t.GetPartitionForWrite(key)
	if err != nil {
		return err
	}

	err = t.partitions[part].WriteIntoLogFile(message)
	if err != nil {
		return err
	}
	return nil
}

// read from correct part

func (t *Topic) ReadFromPartiton(key string, offset int) (string, error) {
	part, err := t.GetPartitionForWrite(key)
	if err != nil {
		return "error in getting partition", err
	}

	return t.partitions[part].ReadFileFromOffset(offset)
}

func (t *Topic) GetAllPartitions() *[]*LogFile {
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

// slice of *message

// TODO: message offset done now we need to consume it with offset
// logfile write

//TODO: we cann implement the write worker pool

func (l *LogFile) WriteIntoLogFile(str string) error {
	if l.file == nil {
		return fmt.Errorf("log file is not initialized")
	}
	newStr := fmt.Sprintf("%d| %s", len(str), str)
	l.mu.Lock()
	defer l.mu.Unlock()
	offset, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("error getting offset for %s: %w", l.FileName, err)
	}
	_, err = l.file.Write([]byte(newStr + "\n"))
	if err != nil {
		return fmt.Errorf("error writing to logfile %s: %w", l.FileName, err)
	}

	h := crc32.ChecksumIEEE([]byte(str))
	// message offset

	m := Message{
		Offset:  int(offset),
		Hash:    int(h),
		Message: str,
	}

	l.index = append(l.index, &m)
	return nil
}

// log file read we could take the offset as a args
func (l *LogFile) ReadFileFromOffset(offset int) (string, error) {
	if l.file == nil {
		return "", fmt.Errorf("log file is not initialized")
	}
	// reading file from start we need to read it from the offset
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.file.Seek(int64(offset), io.SeekStart)
	buf := make([]byte, 1024)
	n, err := l.file.Read(buf)
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return "", fmt.Errorf("failed to read from log file: %w", err)
	}
	return string(buf[:n]), nil
}

// TODO: when we restart the server the topic is missing so we cant read from the topic fix this;
// TODO: we need to move all the messages from the old partition to the new partition after adding the partition mean affected entries will be moved to new partition

func (t *Topic) GetPartitionForWrite(key string) (int, error) {

	if len(t.Ring) == 0 {
		return 0, fmt.Errorf("no partitions in ring ")
	}

	hash := int(crc32.ChecksumIEEE([]byte(key)))
	// last node < Hash
	if hash > t.Ring[len(t.Ring)-1].Hash {
		return t.Ring[0].PartitionIndex, nil
	}

	low := 0
	high := len(t.Ring) - 1

	for low <= high {
		mid := low + (high-low)/2

		if t.Ring[mid].Hash == hash {
			// Exact match found
			return t.Ring[mid].PartitionIndex, nil
		} else if t.Ring[mid].Hash > hash {
			if mid == 0 || t.Ring[mid-1].Hash < hash {
				return t.Ring[mid].PartitionIndex, nil
			}
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return t.Ring[0].PartitionIndex, nil
}

// close one log file
func (l *LogFile) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// TODO: impl add partition then update the ring move affected keys and message to new partition and verify rebalancing
// TODO: add partition to the topic done


// READ CLUSTER META
type Peer struct {
	NodeID int `json:"nodeID"`
	Port   int `json:"port"`
}

type Partition struct {
	LeaderID int   `json:"leaderID"`
	Replicas []int `json:"replicas"`
}
type BrokerTopic struct {
	Partitions map[string]Partition
}

type Broker struct {
	NodeID int    `json:"nodeID"`
	Port   int    `json:"port"`
	Peers  []Peer `json:"peers"`
	Topics map[string]BrokerTopic
}


type clusterMap map[string]map[string]Partition
// currently returning the leader id of the topic
func Read_cluster_metadata(filename string) (int, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	data := make([]byte, 1024)
	_, err = f.Read(data)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	trimmed_data := bytes.Trim(data, "\x00")

	// Marshal the data into a clusterMap
	var c clusterMap
	err = json.Unmarshal(trimmed_data, &c)
	if err != nil {
		return 0, fmt.Errorf("error unmarshalling: %w", err)
	}

	return c["test_topic"]["0"].LeaderID, nil
}



func ReadBrokerConfig(filename string) (int, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %w", err)
	}

	data := make([]byte, 1023)
	_, err = f.Read(data)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	trimmed_data := bytes.Trim(data, "\x00")

	var b Broker
	err = json.Unmarshal(trimmed_data, &b)

	if err != nil {
		return 0, fmt.Errorf("error unmarshaling json %w", err)
	}

	return b.NodeID, nil
}

func CheckLeader(lId, nodeId int) bool {
	if lId == nodeId {
		return true
	}
	return false
}


