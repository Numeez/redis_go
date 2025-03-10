package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

func main() {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("Error: ", err)
	}
	aof, err := NewAOF("database.aof")
	if err != nil {
		log.Fatal("Error: ", err)
	}
	defer aof.Close()
	_ = aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

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
		if value.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}
		if len(value.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(conn)
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			_ = writer.Write(Value{typ: "string", str: ""})
			continue
		}
		if command == "SET" || command == "HSET" {
			_ = aof.Write(value)
		}
		result := handler(args)
		_ = writer.Write(result)
	}

}
