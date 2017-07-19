package beater

import (
	"fmt"
	"os"
	"time"
	"encoding/json"
	"os/signal"

	// [START imports]
	"golang.org/x/net/context"
	"cloud.google.com/go/pubsub"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/nugielim/gpubsubbeat/config"
	// [END imports]
)

type Gpubsubbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client
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
	return bt, nil
}

func (bt *Gpubsubbeat) Run(b *beat.Beat) error {
	logp.Info("gpubsubbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	ticker := time.NewTicker(bt.config.Period)
	client := bt.createPubSubClient()
	topicSuffix := bt.config.Region
	t := createTopicIfNotExists(client,topicSuffix)
	var sub = "gpubsubbeat-subscription-"+topicSuffix
	// Create a new subscription.
	if err := create(client, sub, t); err != nil {
		logp.Info("Error creating subscription", err)
	}

	err := bt.pullMsgsSettings(b,client,sub,bt.config.MaxFetchMsg)
	if err != nil {
		logp.Err("Error!", err)
	}

	// counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		/*event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       b.Name,
			"counter":    counter,
		}*/
		// bt.client.PublishEvent(event)
		logp.Info("Event sent")
		//counter++
	}
}

func (bt *Gpubsubbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Gpubsubbeat) createPubSubClient() *pubsub.Client {
	ctx := context.Background()
	proj := ""
	// [START auth]
	if bt.config.ProjectID == "env" {
		logp.Info("Project ID is not being set in the config. Fallback to GOOGLE_CLOUD_PROJECT environment variable.")
		proj = os.Getenv("GOOGLE_CLOUD_PROJECT")
		if proj == "" {
			logp.Critical("GOOGLE_CLOUD_PROJECT environment variable must be set.\n")
			os.Exit(1)
		}
	} else {
		proj = bt.config.ProjectID
	}
	client, err := pubsub.NewClient(ctx, proj)
	if err != nil {
		logp.Info("Could not create pubsub Client: %v", err)
		return nil
	}
	return client

}

func create(client *pubsub.Client, name string, topic *pubsub.Topic) error {
	ctx := context.Background()
	// [START create_subscription]
	sub, err := client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	logp.Info("Created subscription: %v\n", sub)
	// [END create_subscription]
	return nil
}

func createTopicIfNotExists(c *pubsub.Client, topicSuffix string) *pubsub.Topic {
	ctx := context.Background()

	var topic = "gpubsubbeat-" + topicSuffix
	// Create a topic to subscribe to.
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		logp.Info("Topic %v exist, no need to create new topic", err)
	}
	if ok {
		return t
	}

	t, err = c.CreateTopic(ctx, topic)
	if err != nil {
		logp.Critical("Failed to create the topic: %v", err)
		os.Exit(1)
	}
	return t
}

func (bt *Gpubsubbeat) pullMsgsSettings(b *beat.Beat, client *pubsub.Client, name string, maxnomsg int) error {
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

	// [START pull_messages_settings]
	logp.Info("Start fetching message")
	sub := client.Subscription(name)
	sub.ReceiveSettings.MaxOutstandingMessages = maxnomsg
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		logp.Info("Got message: %q\n", string(msg.Data))

		var data map[string]interface{}

		if err := json.Unmarshal(msg.Data, &data); err != nil {
			logp.Critical("Unable unmarshal the data. %v",err)
		}
		timestampStr := data["timestamp"].(string)

		timeParsed, _ := time.Parse(time.RFC3339,timestampStr)

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
				"@timestamp": common.Time(timeParsed.UTC()),
				"type":       "jsonPayload",
				"user":	user,
				"event_subtype": event_subtype,
				"event_type": event_type,
				"instance": instance,
				"zone": zone,
				"instance_id": instance_id,
				"error_code": error_code,
				"raw_data": string(msg.Data),
				"receiveTimestamp": common.Time(time.Now()),
			}

		} else {
			event = common.MapStr{
				"@timestamp": common.Time(timeParsed.UTC()),
				"type":       "protoPayload",
				"raw_data": string(msg.Data),
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
	// [END pull_messages_settings]
}
