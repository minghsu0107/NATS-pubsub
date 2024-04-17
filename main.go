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
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	nc "github.com/nats-io/nats.go"
	natsJS "github.com/nats-io/nats.go/jetstream"
)

// pull-based consumer example (multiple subscriptions bound to the same consumer on a workqueue stream)
func main() {
	logger := watermill.NewStdLogger(false, false)
	options := []nc.Option{
		nc.RetryOnFailedConnect(true),
		nc.Timeout(30 * time.Second),
		nc.ReconnectWait(1 * time.Second),
	}
	conn1, natsConnectErr := nc.Connect(os.Getenv("NATS_URL"), options...)
	if natsConnectErr != nil {
		panic(natsConnectErr)
	}
	conn2, natsConnectErr := nc.Connect(os.Getenv("NATS_URL"), options...)
	if natsConnectErr != nil {
		panic(natsConnectErr)
	}

	consumerConfig := natsJS.ConsumerConfig{
		Name: "myconsumer",
		// Durable:       "myconsumer",
		DeliverPolicy: natsJS.DeliverAllPolicy,
		AckPolicy:     natsJS.AckExplicitPolicy,
		MaxDeliver:    15,
		// For push consumers, MaxAckPending is the only form of flow control.
		// However, for pull consumers because the delivery of the messages to the client application is client-driven
		// rather than server initiated, there is an implicit one-to-one flow control with the subscribers (the maximum batch size of the Fetch calls)
		MaxAckPending:     2048,
		InactiveThreshold: 30 * time.Second,

		// the followings are pull-specific options
		// The maximum number of inflight pull requests
		// server default is 512
		MaxWaiting: 4096,
		// The maximum total bytes that can be requested in a given batch
		MaxRequestMaxBytes: 1024 * 1024,
	}
	namer := func(_ string, _ string) natsJS.ConsumerConfig {
		return consumerConfig
	}
	subscriber1, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
		URL:                 os.Getenv("NATS_URL"),
		Logger:              logger,
		Conn:                conn1,
		AckWaitTimeout:      5 * time.Second,
		ResourceInitializer: simpleResourceInitializer(namer, "example_topic", ""),
		ConsumeOptions: []natsJS.PullConsumeOpt{
			// max_bytes limit on a fetch request
			natsJS.PullMaxBytes(1024 * 1024),
			// timeout on a single pull request to the server
			// waiting until at least one message is available
			natsJS.PullExpiry(1 * time.Second),
			// the byte count on which Consume will trigger new pull request to the server
			// Defaults to 50% of MaxBytes
			natsJS.PullThresholdBytes(1024 * 512),
		},
		// AckAsync enables asynchronous acknowledgement
		AckAsync: true,
	})
	if err != nil {
		panic(err)
	}

	subscriber2, err := jetstream.NewSubscriber(jetstream.SubscriberConfig{
		URL:                 os.Getenv("NATS_URL"),
		Logger:              logger,
		Conn:                conn2,
		AckWaitTimeout:      5 * time.Second,
		ResourceInitializer: simpleResourceInitializer(namer, "example_topic", ""),
		ConsumeOptions: []natsJS.PullConsumeOpt{
			natsJS.PullMaxBytes(1024 * 1024),
			natsJS.PullExpiry(1 * time.Second),
			natsJS.PullThresholdBytes(1024 * 512),
		},
		AckAsync: true,
	})
	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal - closing subscriber")
		subscriber1.Close()
		subscriber2.Close()
		os.Exit(0)
	}()

	messages1, err := subscriber1.Subscribe(context.Background(), "example_topic.test")
	if err != nil {
		panic(err)
	}
	go processJS(messages1, "subscriber1")

	messages2, err := subscriber2.Subscribe(context.Background(), "example_topic.test")
	if err != nil {
		panic(err)
	}
	go processJS(messages2, "subscriber2")

	pubConn, err := nc.Connect(os.Getenv("NATS_URL"), options...)
	if err != nil {
		panic(err)
	}
	defer pubConn.Close()
	jsOpts := []natsJS.JetStreamOpt{
		// sets the maximum outstanding async publishes that can be inflight at one time
		// natsJS.WithPublishAsyncMaxPending(16384),
	}
	js, err := natsJS.New(pubConn, jsOpts...)
	if err != nil {
		panic(err)
	}
	marshaler := &nats.NATSMarshaler{}

	pubOpts := []natsJS.PublishOpt{
		natsJS.WithRetryWait(100 * time.Millisecond),
		natsJS.WithRetryAttempts(3),
	}

	i := 0
	var id string
	for {
		id = strconv.Itoa(i)
		msg, err := marshaler.Marshal("example_topic.test", message.NewMessage(id, []byte("hello world")))
		if err != nil {
			fmt.Println(err)
		}
		if _, err = js.PublishMsg(context.Background(), msg, pubOpts...); err != nil {
			panic(err)
		}

		fmt.Printf("sent message %v\n", i)

		time.Sleep(50 * time.Millisecond)
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

func simpleResourceInitializer(consumerNamer jetstream.ConsumerConfigurator, stream, group string) jetstream.ResourceInitializer {
	return func(ctx context.Context, js natsJS.JetStream, topic string) (natsJS.Consumer, func(context.Context, watermill.LoggerAdapter), error) {
		s, err := js.Stream(ctx, stream)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get stream for stream %s: %w", stream, err)
		}
		consumer, err := s.CreateOrUpdateConsumer(ctx, consumerNamer(topic, group))

		return consumer, nil, err
	}
}
