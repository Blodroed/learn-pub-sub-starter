package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gamelogic.PrintServerHelp()
	inputs := make(chan []string)
	// this goroutine reads user input from stdin and sends it to the inputs channel
	go func() {
		defer close(inputs)
		scanner := bufio.NewScanner(os.Stdin)
		for {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Printf("error reading input: %s", err)
				}
				return
			}
			select {
			case inputs <- strings.Fields(strings.TrimSpace(scanner.Text())):
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		fmt.Print("> ")
		var input []string

		// Shutdown handling
		select {
		case <-ctx.Done():
			fmt.Println()
			log.Println("Shutdown signal received, shutting down...")
			return
		case nextInput, ok := <-inputs:
			if !ok {
				return
			}
			if ctx.Err() != nil {
				fmt.Println()
				log.Println("Shutdown signal received, shutting down...")
				return
			}
			input = nextInput
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
