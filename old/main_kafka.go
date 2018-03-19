package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	logPath := os.Args[1]

	logFile, err := os.Open(logPath)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	r := bufio.NewReader(logFile)

	k := kafka.NewWriter(kafka.WriterConfig{
		//		Brokers: []string{"54.233.238.100:9092"},
		Brokers: []string{"172.31.42.185:9092"},
		Topic:   "logs",
	})
	defer k.Close()

	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}

		err = k.WriteMessages(context,
			kafka.Message{
				Key:   []byte("liave"),
				Value: line,
			},
		)

		if err != nil {
			fmt.Println("Error sending to kafka", err)
		}
	}
}
