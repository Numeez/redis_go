package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAOF(filePath string) (*AOF, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	aof := &AOF{
		file: file,
		rd:   bufio.NewReader(file),
	}
	go func() {
		aof.mu.Lock()
		_ = aof.file.Sync()
		aof.mu.Unlock()
		time.Sleep(1 * time.Second)
	}()
	return aof, nil
}

func (aof *AOF) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *AOF) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}
	return nil
}

func (aof *AOF) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	resp := NewResp(aof.file)
	for {
		value, err := resp.Read()
		if err == nil {
			callback(value)
		}
		if err == io.EOF {
			break
		}
		return err
	}
	return nil
}
