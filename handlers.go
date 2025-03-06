package main

import (
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var SETs = map[string]string{}
var SETMu = sync.RWMutex{}
var HSETs = map[string]map[string]string{}
var HSETMu = sync.RWMutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{
			typ: "string",
			str: "PONG",
		}
	}
	return Value{
		typ: "string",
		str: args[0].bulk,
	}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}
	key := args[0].bulk
	val := args[1].bulk
	SETMu.Lock()
	SETs[key] = val
	SETMu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}
	key := args[0].bulk
	SETMu.RLock()
	value, ok := SETs[key]
	SETMu.RUnlock()
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk
	HSETMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETMu.Unlock()

	return Value{typ: "string", str: "OK"}

}
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	HSETMu.RLock()
	if _, ok := HSETs[hash]; !ok {
		return Value{typ: "error", str: "ERR Invalid hash: hash does not exists"}
	}
	if _, ok := HSETs[hash][key]; !ok {
		return Value{typ: "error", str: "ERR Invalid key for the hash: key does not exists"}
	}
	value := HSETs[hash][key]
	HSETMu.RUnlock()

	return Value{typ: "bulk", bulk: value}

}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}
	hash := args[0].bulk
	HSETMu.RLock()
	if _, ok := HSETs[hash]; !ok {
		return Value{typ: "error", str: "ERR Invalid hash: hash does not exists"}
	}

	data := HSETs[hash]
	HSETMu.RUnlock()
	var result []Value
	for _, value := range data {
		result = append(result, Value{typ: "bulk", bulk: value})
	}

	return Value{typ: "array", array: result}
}
