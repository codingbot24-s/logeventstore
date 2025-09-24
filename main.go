package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
)

// represent 1partition
type LogFile struct {
	fileName string
	file     *os.File
	offset   int
}
// create a new logfile return the * of struct
func NewLogFile(fname string) (*LogFile, error) {
	file, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	return &LogFile{
		fileName: fname,
		file:     file,
		offset:   0,
	}, nil
}
type Topic struct {
	partitions []*LogFile
}
// create a new topics with given partitions
func NewTopic(name string, numPartitions int) (*Topic, error) {
    if numPartitions <= 0 {
        return nil, fmt.Errorf("number of partitions must be positive")
    }
    
    partitions := make([]*LogFile, numPartitions)
    for i := range numPartitions {
        logFile, err := NewLogFile(fmt.Sprintf("%s-partition-%d.log", name, i))
        if err != nil {
            return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
        }
        partitions[i] = logFile
    }
    
	return &Topic{
		partitions: partitions,
	},nil
}
// write into correct part
func (t* Topic) writeIntoPartition(key string,message string) error {
	part := t.get_partition(key)
	return t.partitions[part].writeIntoLogFile(message)
}
// read from correct part

func (t* Topic) readFromPartiton(key string,offset int) (string,error ){
	part := t.get_partition(key)
	return t.partitions[part].readFileFromOffset(offset)
}

// close all partitions 

func (t *Topic) CloseP () error {
	var err error
	for _, p := range t.partitions{
		if err := p.Close(); err != nil  {
			err = err
		}	
	}

	return err
}


// logfile write
func (l *LogFile) writeIntoLogFile(str string) error{
	if l.file == nil {
		return fmt.Errorf("log file is not initialized")
	}	
	n, err := l.file.Write([]byte(str))
	if err != nil {
		log.Fatal("error writing in logfile", l.fileName, err)
	}
	l.offset = l.offset + n
	fmt.Println("Writing successfull")
	return nil
}
// log file read 
func (l *LogFile) readFileFromOffset(offset int) (string,error) {
	if l.file == nil {
		return "" ,fmt.Errorf("log file is not initialized")
	}	
	buf := make([]byte, 1024)
	n, err := l.file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return "", fmt.Errorf("failed to read from log file: %w", err)
	}
	return string(buf[:n]),nil	
}

func (t *Topic) get_partition(key string) int {
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

func main() {
	t,err := NewTopic("logs",2)
	if err != nil {
		log.Fatal(err)
	}

	defer t.CloseP()

	err = t.writeIntoPartition("user-123","user logged in")
	if err != nil {
		log.Printf("error writing in to the partition")
	}

 	 err = t.writeIntoPartition("user-123","user performed some action")
	if err != nil {
		log.Printf("error writing in to the partition")
	}  
}
