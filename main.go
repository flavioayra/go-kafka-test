package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/Shopify/sarama"
)

func main() {

	logPath := os.Args[1]

	logFile, err := os.Open(logPath)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()

	r := bufio.NewReader(logFile)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = 10000000

	//brokers := []string{"54.233.238.100:9092"}
	brokers := []string{"172.31.42.185:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}

		msg := &sarama.ProducerMessage{
			Topic: "logs",
			Value: sarama.StringEncoder(line),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}

		if err != nil {
			fmt.Println("Error sending to kafka", err)
		}
	}
}
