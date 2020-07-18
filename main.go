package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
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

// AggregationProducts list all pedidos and send a new topic with total pedidos after has 5
func AggregationProducts(parent context.Context, listpedidos []pedido, writer *kafka.Writer) (listpedidoss []pedido) {
	if len(listpedidos) > 5 {
		var totalcompras float64
		totalcompras = 0
		currentTime := time.Now()
		strcurrentTime := currentTime.Format("2006.01.02 15:04:05")

		for _, item := range listpedidos {
			valor, _ := strconv.ParseFloat(item.Valortotal, 2)
			totalcompras = totalcompras + valor
		}
		strtotal := strconv.FormatFloat(totalcompras, 'f', 6, 64)
		msg := fmt.Sprintf(` {"totalcompras": %s,"datapush:" %s }  `, strtotal, strcurrentTime)

		message := kafka.Message{
			Key:   nil,
			Value: []byte(msg),
		}
		writer.WriteMessages(parent, message)

		// clean list after send topic
		listpedidos = []pedido{}
		return listpedidos
	}
	return listpedidos
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

	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: myConf.kafkaClientID,
	}

	// Defined configs about writer in topic totalpedidos
	configWriter := kafka.WriterConfig{
		Brokers:          myConf.kafkaBrokerURL,
		Topic:            "totalpedidos",
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}

	// Instance objects writer and reader topics
	writer := kafka.NewWriter(configWriter)
	reader := kafka.NewReader(config)

	defer reader.Close()

	fmt.Println("Receiving messages....")
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

		listpedidos = AggregationProducts(context.Background(), listpedidos, writer)

	}

}
