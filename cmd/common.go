package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ReadInput(ctx context.Context) <-chan []string {
	inputs := make(chan []string)
	go func() {
		defer close(inputs)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input := strings.Fields(strings.TrimSpace(scanner.Text()))
			select {
			case inputs <- input:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("error reading input: %s", err)
		}
	}()
	return inputs
}

func WaitForInput(ctx context.Context, inputs <-chan []string) ([]string, bool) {
	select {
	case <-ctx.Done():
		fmt.Println()
		log.Println("Shutdown signal received, shutting down...")
		return nil, false
	case input, ok := <-inputs:
		if !ok || ctx.Err() != nil {
			if ctx.Err() != nil {
				fmt.Println()
				log.Println("Shutdown signal received, shutting down...")
			}
			return nil, false
		}
		return input, true
	}
}

func PublishGameLog(ch *amqp.Channel, username, log string) error {
	var gl routing.GameLog
	gl.Message = log
	gl.Username = username
	gl.CurrentTime = time.Now()

	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gl)
	if err != nil {
		return err
	}

	return nil
}
