package main

import (
	"fmt"
	"log"
	"net"
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

		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(value)
		writer := NewWriter(conn)
		_ = writer.Write(Value{typ: "string", str: "OK"})
	}

}
