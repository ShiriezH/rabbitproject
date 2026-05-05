package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler for pause/resume messages
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

// Handler for move messages
func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		result := gs.HandleMove(move)

		switch result {
		case 0, 1: // safe, make war
			return pubsub.Ack

		case 2: // same player
			return pubsub.NackDiscard

		default:
			return pubsub.NackDiscard
		}
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	// Get username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get username: %v", err)
	}

	// Connect to RabbitMQ
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Channel for publishing
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	// Create game state
	gameState := gamelogic.NewGameState(username)

	// Subscribe to pause
	pauseQueue := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		pauseQueue,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe (pause): %v", err)
	}

	// Subscribe to moves
	moveQueue := fmt.Sprintf("army_moves.%s", username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		moveQueue,
		"army_moves.*",
		pubsub.Transient,
		handlerMove(gameState),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe (moves): %v", err)
	}

	fmt.Printf("Listening on queues: %s and %s\n", pauseQueue, moveQueue)

	// Show help
	gamelogic.PrintClientHelp()

	// REPL loop
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {

		case "spawn":
			gameState.CommandSpawn(words)

		case "move":
			moveEvent, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("Move failed:", err)
				continue
			}

			fmt.Println("Move successful")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("army_moves.%s", username),
				moveEvent,
			)
			if err != nil {
				fmt.Println("Failed to publish move:", err)
			} else {
				fmt.Println("Move published")
			}

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}

