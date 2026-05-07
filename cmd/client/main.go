package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)

		switch outcome {

		case gamelogic.MoveOutcomeMakeWar:

			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetPlayerSnap()),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)

			if err != nil {
				fmt.Println("Failed to publish war:", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard

		default:
			return pubsub.Ack
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(w gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(w)

		var message string

		switch outcome {

		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon,
			gamelogic.WarOutcomeYouWon:

			message = fmt.Sprintf(
				"%s won a war against %s",
				winner,
				loser,
			)

		case gamelogic.WarOutcomeDraw:

			message = fmt.Sprintf(
				"A war between %s and %s resulted in a draw",
				winner,
				loser,
			)

		default:
			return pubsub.NackDiscard
		}

		err := pubsub.PublishGob(
			ch,
			routing.ExchangePerilTopic,
			fmt.Sprintf("%s.%s", routing.GameLogSlug, winner),
			routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    winner,
			},
		)

		if err != nil {
			fmt.Println("Failed to publish log:", err)
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get username: %v", err)
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Pause subscribe failed: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("army_moves.%s", username),
		"army_moves.*",
		pubsub.Transient,
		handlerMove(gameState, ch),
	)
	if err != nil {
		log.Fatalf("Move subscribe failed: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		handlerWar(gameState, ch),
	)
	if err != nil {
		log.Fatalf("War subscribe failed: %v", err)
	}

	fmt.Println("Client ready")

	gamelogic.PrintClientHelp()

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

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("army_moves.%s", username),
				moveEvent,
			)

			if err != nil {
				fmt.Println("Failed to publish move:", err)
			}

		case "spam":

			if len(words) < 2 {
				fmt.Println("Usage: spam <number>")
				continue
			}

			var amount int

			_, err := fmt.Sscanf(words[1], "%d", &amount)
			if err != nil {
				fmt.Println("Invalid number")
				continue
			}

			for i := 0; i < amount; i++ {

				logMsg := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     gamelogic.GetMaliciousLog(),
					Username:    username,
				}

				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					logMsg,
				)

				if err != nil {
					fmt.Println("Failed to publish spam log:", err)
				}
			}

			fmt.Printf("Published %d malicious logs\n", amount)

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}