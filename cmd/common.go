package cmd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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
