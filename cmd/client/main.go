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
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	// Get username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("❌ Failed to get username: %v", err)
	}

	// Connect to RabbitMQ
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create game state
	gameState := gamelogic.NewGameState(username)

	// Queue name: pause.username
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	// Subscribe instead of just declaring queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("❌ Failed to subscribe: %v", err)
	}

	fmt.Printf("✅ Listening on queue: %s\n", queueName)

	// Show help
	gamelogic.PrintClientHelp()

	// REPL loop
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {

		// spawn
		case "spawn":
			gameState.CommandSpawn(words)

		// move
		case "move":
			msg, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("❌ Move failed:", err)
			} else {
				fmt.Println(msg)
			}

		// status
		case "status":
			gameState.CommandStatus()

		// help
		case "help":
			gamelogic.PrintClientHelp()

		// spam
		case "spam":
			fmt.Println("🚫 Spamming not allowed yet!")

		// quit
		case "quit":
			gamelogic.PrintQuit()
			return

		// unknown
		default:
			fmt.Println("❓ Unknown command")
		}
	}
}

