package main

import (
	"fmt"
	"io"
	"log"
	"os"
)
// represent 1partition
type Log_file struct {
	fileName string
	file   *os.File
	offset int
}

func New_file(fname string) *Log_file{
	l := Log_file{
		fileName: fname,
	} 	
	return &l
}

func(l* Log_file) write_into_file(str string) {
	var err error
	l.file, err = os.OpenFile(l.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("error opening file ", l.fileName, err)
	}
	n, err := l.file.Write([]byte(str))
	if err !=  nil {
		log.Fatal("error opening file ", l.fileName, err)
	}
	l.offset = l.offset + n
	fmt.Println("Writing successfull")
	defer l.file.Close()
}

func(l *Log_file) readFileFromoffset(fileName string, offset int) {
	// check is file open 
	
	buf := make([]byte, 1024)
	n, err := l.file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return
	}
	fmt.Printf("Read :%s\n", string(buf[:n]))

	defer l.file.Close()
}

// repsresent the multiple partitions
type Topic struct {
	partitions []*Log_file
}

func (t *Topic) get_partition(key string) int {
	// TODO: consistent hashing
	// round robin
	counter := 0
	counter++
	p := counter * len(t.partitions)
	return p
}

func main() {
}
