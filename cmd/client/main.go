package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("error connecting to amqp server: %s", err)
	}
	defer conn.Close()
	fmt.Println("Amqp connection successful")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error during client welcome: %s", err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(
		conn, routing.ExchangePerilDirect,
		routing.PauseKey+"."+username, routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		fmt.Printf("error declaring and binding queue: %s", err)
		return
	}

	// OS signal handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Interrupt received, shutting down...")
}
