package helper

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
)
// represent one partition
type LogFile struct {
	fileName string
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
		fileName: fname,
		file:     file,
		offset:   0,
	}, nil
}

type Node struct {
	Hash 	int
		
}
type Topic struct {
	partitions []*LogFile
	ring 	 []Node
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

// close all partitions

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
		log.Fatal("error writing in logfile", l.fileName, err)
	}
	l.offset = l.offset + n
	fmt.Printf("Writing successfull in %s\n", l.fileName)
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
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % len(t.partitions)
}

// close one log file
func (l *LogFile) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
// TODO: implement consistent hashing for partition



	


	