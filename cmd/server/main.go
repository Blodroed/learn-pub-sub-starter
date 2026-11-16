package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("error connecting to amqp server: %s", err)
	}
	defer conn.Close()
	fmt.Println("Amqp connection successful")

	// AMQP channel
	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("error creating channel: %s", err)
	}
	defer channel.Close()

	// send a message, remove later
	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		fmt.Printf("error publishing message: %s", err)
	}

	// OS signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Interrupt received, shutting down...")
}
