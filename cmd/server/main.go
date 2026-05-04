package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// Show commands
	gamelogic.PrintServerHelp()

	connStr := "amqp://guest:guest@localhost:5672/"

	// Connect
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()

	// Channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("❌ Failed to open channel: %v", err)
	}
	defer ch.Close()

	// Declare durable queue: game_logs
	q, err := ch.QueueDeclare(
		routing.GameLogSlug, // "game_logs"
		true,  // durable ✅
		false, // autoDelete ❌
		false, // exclusive ❌
		false, // noWait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("❌ Failed to declare queue: %v", err)
	}

	// Bind to topic exchange
	err = ch.QueueBind(
		q.Name,
		routing.GameLogSlug+".*", // "game_logs.*"
		routing.ExchangePerilTopic, // "peril_topic"
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("❌ Failed to bind queue: %v", err)
	}

	// REPL loop
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {

		case "pause":
			fmt.Println("⏸ Sending pause message...")

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Printf("❌ Failed to publish pause: %v", err)
			}

		case "resume":
			fmt.Println("▶️ Sending resume message...")

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Printf("❌ Failed to publish resume: %v", err)
			}

		case "quit":
			fmt.Println("🛑 Shutting down server...")
			return

		default:
			fmt.Println("❓ Unknown command")
		}
	}
}