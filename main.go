package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

func setOFFSet(n byte, msgLen int) (int, int) {
	store := make([]int, 1024)
	store[0] = int(n)
	store[1] = msgLen

	return store[0], store[1]
}

func openFileAndWrite(fileName string) (int, int) {

	f, err := os.OpenFile(fileName, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatal("error opening file ", fileName, err)
	}
	currentEndOfSet := 0

	n, err := f.Write([]byte("HelloWorld"))
	if err != nil {
		log.Println("Error in writing file", err)
	}
	currentEndOfSet += n
	offset, len := setOFFSet(byte(currentEndOfSet), n)
	defer f.Close()
	return offset, len
}

func readFileFromoffset(fileName string, offset int) {

	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal("error opening file ", fileName, err)
	}
	buf := make([]byte,1024)
	n, err := f.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF { 
		log.Println("Error from reading in offset", err.Error())
		return
	}
	fmt.Printf("Read :%s\n", string(buf[:n]))

	defer f.Close()
}

func main() {
	fileName := "t.txt"
	ofset, len := openFileAndWrite(fileName)
	fmt.Printf("ofset is %d, and len is also %d\n", ofset, len)
	readFileFromoffset("t.txt", 2)
}
