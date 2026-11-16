package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/cmd"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Printf("error connecting to amqp server: %s", err)
		return
	}
	defer conn.Close()
	fmt.Println("Amqp connection successful")

	// AMQP channel
	channel, err := conn.Channel()
	if err != nil {
		log.Printf("error creating channel: %s", err)
		return
	}
	defer channel.Close()

	// Topic channel
	_, _, err = pubsub.DeclareAndBind(
		conn, routing.ExchangePerilTopic, routing.GameLogSlug,
		routing.GameLogSlug+".*", pubsub.Durable,
	)
	if err != nil {
		fmt.Printf("error declaring and binding topic queue: %s", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gamelogic.PrintServerHelp()
	inputs := cmd.ReadInput(ctx)

	for {
		fmt.Print("> ")
		var input []string

		input, ok := cmd.WaitForInput(ctx, inputs)
		if !ok {
			return
		}

		if len(input) == 0 {
			continue
		}

		// Input command handling
		switch input[0] {
		case "pause":
			log.Default().Println("Sending a pause message")
			if err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			}); err != nil {
				log.Default().Printf("error publishing pause message: %s\n", err)
			}
			continue
		case "resume":
			log.Default().Println("Sending a resume message")
			if err := pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			}); err != nil {
				log.Default().Printf("error publishing resume message: %s\n", err)
			}
			continue
		case "quit":
			log.Default().Println("Quitting...")
			return
		default:
			log.Default().Println("Unknown command")
			continue
		}
	}
}
