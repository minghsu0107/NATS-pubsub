package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
)

func main() {
	marshaler := &nats.GobMarshaler{}
	logger := watermill.NewStdLogger(false, false)
	options := []nc.Option{
		nc.RetryOnFailedConnect(true),
		nc.Timeout(30 * time.Second),
		nc.ReconnectWait(1 * time.Second),
	}
	subscribeOptions := []nc.SubOpt{
		// Read from the beginning of the channel (default)
		// nc.DeliverAll(),

		// Only receive messages that were created after the consumer was created
		// nc.DeliverNew(),

		// Start receiving messages with the last message added to the stream,
		// or the last message in the stream that matches the consumer's filter subject if defined
		// nc.DeliverLast(),

		// Read from a specific time the message arrived in the channel
		// nc.StartTime(startTime),

		// MaxAckPending sets the number of outstanding acks that are allowed before message delivery is halted
		// if it is too large, the subscriber will have not enough time processing messages
		// before NATS timeout. In this case, NATS will send the same batch of messages
		// to another subscriber in the same queue group. Thus, messages may be processed twice
		nc.MaxAckPending(2048),

		// MaxDeliver sets the number of redeliveries for a message
		// Applies to any message that is re-sent due to a negative ack, or no ack sent by the client
		nc.MaxDeliver(15),
		nc.AckExplicit(),

		// LimitsPolicy (default) means that messages are retained until any given limit is reached
		// This could be one of MaxMsgs, MaxBytes, or MaxAge.

		// Discard Policy can be either Old (default) or New. It affects how MaxMessages and MaxBytes operate.
		// If a limit is reached and the policy is Old, the oldest message is removed.
		// If the policy is New, new messages are refused if it would put the stream over the limit.
	}

	jsConfig := nats.JetStreamConfig{
		Disabled:         false,
		AutoProvision:    false,
		SubscribeOptions: subscribeOptions,
		TrackMsgId:       false,
		AckAsync:         false,
		DurablePrefix:    "my-durable",
	}
	subscriber1, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL: os.Getenv("NATS_URL"),
			// For non durable queue subscribers, when the last member leaves the group,
			// that group is removed. A durable queue group (DurableName) allows you to have all members leave
			// but still maintain state. When a member re-joins, it starts at the last position in that group.
			// When QueueGroup is empty, subscribe without QueueGroup will be used
			// in this case, SubscribersCount should be set to 1 to avoid duplication
			QueueGroupPrefix: "example",
			SubscribersCount: 4, // how many goroutines should consume messages
			CloseTimeout:     time.Minute,
			// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
			AckWaitTimeout: time.Second * 30,
			NatsOptions:    options,
			Unmarshaler:    marshaler,
			JetStream:      jsConfig,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	subscriber2, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:              os.Getenv("NATS_URL"),
			QueueGroupPrefix: "example",
			SubscribersCount: 4,
			CloseTimeout:     time.Minute,
			AckWaitTimeout:   time.Second * 30,
			NatsOptions:      options,
			Unmarshaler:      marshaler,
			JetStream:        jsConfig,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal - closing subscriber")
		subscriber1.Close()
		os.Exit(0)
	}()

	messages1, err := subscriber1.Subscribe(context.Background(), "example_topic.>")
	if err != nil {
		panic(err)
	}
	go processJS(messages1, "subscriber1")

	messages2, err := subscriber2.Subscribe(context.Background(), "example_topic.>")
	if err != nil {
		panic(err)
	}
	go processJS(messages2, "subscriber2")

	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL:         os.Getenv("NATS_URL"),
			NatsOptions: options,
			Marshaler:   marshaler,
			JetStream: nats.JetStreamConfig{
				Disabled:      false,
				AutoProvision: false,
				ConnectOptions: []nc.JSOpt{
					// the maximum outstanding async publishes that can be inflight at one time
					nc.PublishAsyncMaxPending(16384),
				},
				PublishOptions: nil,
				TrackMsgId:     false,
				AckAsync:       false,
			},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	i := 0
	var id string
	for {
		id = strconv.Itoa(i)
		msgA := message.NewMessage(id, []byte("hello from a"))
		msgB := message.NewMessage(id, []byte("hello from b"))
		msgATest := message.NewMessage(id, []byte("hello from a.test"))
		msgBTest := message.NewMessage(id, []byte("hello from b.test"))

		if err := publisher.Publish("example_topic.a", msgA); err != nil {
			panic(err)
		}
		if err := publisher.Publish("example_topic.b", msgB); err != nil {
			panic(err)
		}

		if err := publisher.Publish("example_topic.a.test", msgATest); err != nil {
			panic(err)
		}
		if err := publisher.Publish("example_topic.b.test", msgBTest); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
		i++
	}
}

func processJS(messages <-chan *message.Message, from string) {
	for msg := range messages {
		log.Printf("[%s] received message: %s, payload: %s", from, msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
