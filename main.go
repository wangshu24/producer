package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

const (
	TargetURL   = "https://localhost::8080/ingest"
	WorkerCount = 10
	BatchSize   = 1000
)

type Payload struct {
	ID       int     `json:"id"`
	Timestap int64   `json:"timestamp`
	Sensor   string  `json:"sensor`
	Value    float64 `json:"value"`
}

func main() {
	fmt.Println("Hello world! From Producer!")
	var wg sync.WaitGroup
	start := time.Now()

	fmt.Printf("Starting load testing with %d producers hitting %s\n", WorkerCount, TargetURL)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for j := 0; j < BatchSize/WorkerCount; j++ {
				sendData(client, wid, j)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("\nFinished. Duration since start: %v\n", time.Since(start))
}

func sendData(client *http.Client, workerID, seq int) {
	data := Payload{
		ID:       (workerID * 1000) + seq,
		Timestap: time.Now().UnixNano(),
		Sensor:   fmt.Sprintf("sensor-%d", workerID),
		Value:    rand.Float64() * 100,
	}

	jsond, _ := json.Marshal(data)

	resp, err := client.Post(TargetURL, "application/json", bytes.NewBuffer(jsond))
	if err != nil {
		fmt.Printf("Worker %d: Error sending %v\n", workerID, err)
		return
	}

	defer resp.Body.Close()
}
