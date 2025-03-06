package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func main() {

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("Error: ", err)
	}

	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Listener connection error: ", err)
		return
	}
	defer conn.Close()
	for {
		buff := make([]byte, 1024)
		_, err = conn.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("error reading from client: ", err.Error())
			os.Exit(1)
		}
		conn.Write([]byte("+OK\r\n"))
	}

}
