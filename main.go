package main

import (
	"fmt"
	"io"
	"log"
	"os"
)



func openFileAndWrite(str string,fileName string) {

	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)	
	if err != nil {
		log.Fatal("error opening file ", fileName, err)
	}
	if err != nil {
		log.Fatal("error getting offset of file ", fileName, err)
	}

	_ ,err = f.Write([]byte(str))
	if err != nil {
		log.Fatal("error opening file ", fileName, err)
	}
	fmt.Println("Writing successfull")
}

func readFileFromoffset(fileName string, offset int) {

	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal("error opening file ", fileName, err)
	}
	buf := make([]byte, 1024)
	n, err := f.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		log.Println("Error from reading in offset", err.Error())
		return
	}
	fmt.Printf("Read :%s\n", string(buf[:n]))

	defer f.Close()
}

func main() {
	file := "log.txt"
	openFileAndWrite("Hello4 ", file)

}
