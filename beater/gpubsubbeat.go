package beater

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"golang.org/x/net/context"

	"github.com/nugielim/gpubsubbeat/config"
)

type Gpubsubbeat struct {
	done         chan struct{}
	config       config.Config
	client       publisher.Client
	pubSubClient *pubsub.Client
	topic        *pubsub.Topic
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Gpubsubbeat{
		done:   make(chan struct{}),
		config: config,
	}

	err := bt.createPubSubClient()
	return bt, err
}

func (bt *Gpubsubbeat) Run(b *beat.Beat) error {
	var sub string
	if bt.config.SubscriberPrefix != "" {
		sub = fmt.Sprintf("%s%d", bt.config.SubscriberPrefix, time.Now().UnixNano())
	} else {
		sub = bt.config.SubscriberName
	}

	logp.Info("gpubsubbeat is running! Topic: '%s' Subscriber: '%s' Hit CTRL-C to stop it.", bt.config.TopicName, sub)

	bt.client = b.Publisher.Connect()

	err := bt.createTopicIfNotExists()
	if err != nil {
		return err
	}

	if err := bt.manageSubscription(sub); err != nil {
		return err
	}

	err = bt.subscribeAndPullMsg(bt.pubSubClient, sub)
	if err != nil {
		logp.Err("Error!", err)
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
	}
}

func (bt *Gpubsubbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Gpubsubbeat) createPubSubClient() error {
	var proj string
	var err error

	if bt.config.ProjectID == "env" {
		logp.Info("Project ID is not being set in the config. Fallback to GOOGLE_CLOUD_PROJECT environment variable.")
		proj = os.Getenv("GOOGLE_CLOUD_PROJECT")
		if proj == "" {
			return fmt.Errorf("GOOGLE_CLOUD_PROJECT environment variable must be set.")
		}
	} else {
		proj = bt.config.ProjectID
	}

	bt.pubSubClient, err = pubsub.NewClient(context.Background(), proj)
	if err != nil {
		return fmt.Errorf("Could not create pubsub Client: %v", err)
	}
	return nil
}

func (bt *Gpubsubbeat) manageSubscription(name string) error {
	ctx := context.Background()
	subscription := bt.pubSubClient.Subscription(name)

	ok, err := subscription.Exists(ctx)
	if err != nil {
		fmt.Errorf("Failed to verify if %s exist: %v", name, err)
	}
	if ok {
		subscriptionConfig, err := subscription.Config(ctx)
		if err != nil {
			return err
		}
		logp.Info("%+v", subscriptionConfig.Topic)

		// you can have a deleted topic in a subscriber, so we need to verify that too
		okt, errt := subscriptionConfig.Topic.Exists(ctx)
		if errt != nil {
			return errt
		}
		if !okt {
			return fmt.Errorf("Subscriber %s topic is deleteted. Quiting...", name)
		}
		if subscriptionConfig.Topic.ID() != bt.topic.ID() {
			return fmt.Errorf("Subscription %s topic doesn't match %s != %s", name, subscriptionConfig.Topic.ID(), bt.topic.ID())
		}
		return nil
	}

	sub, err := bt.pubSubClient.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic:       bt.topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	logp.Info("Created subscription: %v\n", sub.ID())
	return nil
}

func (bt *Gpubsubbeat) createTopicIfNotExists() error {
	ctx := context.Background()

	t := bt.pubSubClient.Topic(bt.config.TopicName)
	ok, err := t.Exists(ctx)
	if err != nil {
		return fmt.Errorf("Failed to verify if %s exist: %v", bt.config.TopicName, err)
	}
	if ok {
		bt.topic = t
		return nil
	}

	if bt.config.CreateTopic {
		logp.Info("Creating topic %s", bt.config.TopicName)
		t, err = bt.pubSubClient.CreateTopic(ctx, bt.config.TopicName)
		if err != nil {
			return fmt.Errorf("Failed to create the topic %s: %v", bt.config.TopicName, err)
		}
		bt.topic = t
	} else {
		return fmt.Errorf("Topic %s doesn't exists. Quiting.", bt.config.TopicName)
	}
	return nil
}

func (bt *Gpubsubbeat) subscribeAndPullMsg(client *pubsub.Client, name string) error {
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	logp.Info("Start fetching message(s)")
	sub := client.Subscription(name)
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		logp.Info("Got message: %q\n", string(msg.Data))

		var data map[string]interface{}

		if err := json.Unmarshal(msg.Data, &data); err != nil {
			logp.Warn("Unable unmarshal the data with error: '%s'. Ignoring. ", err)
			msg.Ack()
			return
		}

		var timeParsed time.Time
		var err error

		timestampStr, ok := data["timestamp"].(string)
		if ok {
			timeParsed, err = time.Parse(time.RFC3339, timestampStr)
			if err != nil {
				logp.Warn("timestamp field with value '%s' not properly formated ... Ignoring.", timestampStr)
				msg.Ack()
				return
			}
		} else {
			logp.Warn("Missing 'timestamp' field. Ignoring")
			msg.Ack()
			return
		}

		var event common.MapStr

		if jsonPayload, ok := data["jsonPayload"].(map[string]interface{}); ok {
			var user, event_subtype, event_type, zone, instance, instance_id, error_code string

			if actor, ok := jsonPayload["actor"].(map[string]interface{}); ok {
				user = actor["user"].(string)
			}
			event_type = jsonPayload["event_type"].(string)
			event_subtype = jsonPayload["event_subtype"].(string)
			if resource, ok := jsonPayload["resource"].(map[string]interface{}); ok {
				instance_id = resource["id"].(string)
				zone = resource["zone"].(string)
				instance = resource["name"].(string)
			}
			if error, ok := jsonPayload["error"].(map[string]interface{}); ok {
				error_code = error["code"].(string)

			}

			event = common.MapStr{
				"@timestamp":       common.Time(timeParsed.UTC()),
				"type":             "jsonPayload",
				"user":             user,
				"event_subtype":    event_subtype,
				"event_type":       event_type,
				"instance":         instance,
				"zone":             zone,
				"instance_id":      instance_id,
				"error_code":       error_code,
				"raw_data":         string(msg.Data),
				"receiveTimestamp": common.Time(time.Now()),
			}

		} else {
			event = common.MapStr{
				"@timestamp":       common.Time(timeParsed.UTC()),
				"type":             "protoPayload",
				"raw_data":         string(msg.Data),
				"receiveTimestamp": common.Time(time.Now()),
			}
		}
		bt.client.PublishEvent(event)
		msg.Ack()
	})

	if err != nil {
		logp.Err("Error fetching message")
		return err
	}

	return nil
}
