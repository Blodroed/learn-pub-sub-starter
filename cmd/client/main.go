package main

import (
	"context"
	"fmt"
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
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("error connecting to amqp server: %s", err)
		return
	}
	defer conn.Close()
	fmt.Println("Amqp connection successful")

	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("error creating channel: %s", err)
		return
	}
	defer channel.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error during client welcome: %s", err)
		return
	}

	gamestate := gamelogic.NewGameState(username)

	// Subscribe to pause messages from the server
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect,
		"pause."+username, routing.PauseKey,
		pubsub.Transient, handlerPause(gamestate),
	); err != nil {
		fmt.Printf("error subscribing to pause messages: %s\n", err)
		return
	}

	// Subsrcribe to the army_moves.* messages
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*",
		pubsub.Transient, handlerMove(channel, gamestate),
	); err != nil {
		fmt.Printf("error subscribing to army move messages: %s\n", err)
		return
	}

	// Subscribe to war messages from the server
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,
		"war", routing.WarRecognitionsPrefix+".*",
		pubsub.Durable, handlerConsumeWarMessage(channel, gamestate),
	); err != nil {
		fmt.Printf("error subscribing to war messages: %s\n", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	inputs := cmd.ReadInput(ctx)

	for {
		fmt.Print("> ")
		var input []string

		input, ok := cmd.WaitForInput(ctx, inputs)
		if !ok {
			return
		}

		// Process the input command
		switch input[0] {
		case "spawn":
			if err := gamestate.CommandSpawn(input); err != nil {
				fmt.Printf("error executing spawn command: %s\n", err)
			}
		case "move":
			if len(input[1:]) < 2 {
				fmt.Println("The move command requires at least 2 arguments: <destination> <unit id>")
				continue
			}
			armyMove, err := gamestate.CommandMove(input)
			if err != nil {
				fmt.Printf("error executing move command: %s\n", err)
				continue
			}

			if err := pubsub.PublishJSON(channel, routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+username, armyMove,
			); err != nil {
				fmt.Printf("error publishing army move: %s\n", err)
				continue
			}

			fmt.Printf("Moved successfully to %s\n", armyMove.ToLocation)
			continue
		case "status":
			gamestate.CommandStatus()
			continue
		case "help":
			gamelogic.PrintClientHelp()
			continue
		case "spam":
			fmt.Println("Spamming not allowed yet!")
			continue
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
			continue
		}
	}
}
