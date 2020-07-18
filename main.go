package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaConfs struct {
	kafkaBrokerURL []string
	kafkaVerbose   bool
	kafkaTopic     string
	kafkaClientID  string
}

type pedido struct {
	IDPedido    string `json:"id_pedido"`
	Valortotal  string `json:"valortotal"`
	Qntparcelas string `json:"qntparcelas"`
	Datacompra  string `json:"datacompra"`
}

func main() {
	fmt.Println("Starting program....")

	listpedidos := []pedido{}

	myConf := kafkaConfs{}
	myConf.kafkaBrokerURL = strings.Split("localhost:9092,localhost:9093", ",")
	myConf.kafkaClientID = "test"
	myConf.kafkaVerbose = true
	myConf.kafkaTopic = "Pedidos"

	config := kafka.ReaderConfig{
		Brokers:         myConf.kafkaBrokerURL,
		GroupID:         myConf.kafkaClientID,
		Topic:           myConf.kafkaTopic,
		MaxWait:         10 * time.Second,
		ReadLagInterval: -1,
	}
	reader := kafka.NewReader(config)
	defer reader.Close()
	fmt.Println("Receiving messages....")
	//pedidos := []pedidos{}
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("error while receiving message: ", err.Error())
			continue
		}
		msg := string(m.Value)

		msg = strings.ReplaceAll(msg, "Struct{", `{"`)
		msg = strings.ReplaceAll(msg, "=", `":"`)
		msg = strings.ReplaceAll(msg, ",", `","`)
		msg = strings.ReplaceAll(msg, "}", `"}`)
		fmt.Println("message receive: ", msg)

		pedidos := pedido{}

		err = json.Unmarshal([]byte(msg), &pedidos)
		if err != nil {
			panic(err)
		}

		listpedidos = append(listpedidos, pedidos)

	}

}
