// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period           time.Duration `config:"period"`
	ProjectID        string        `config:"projectid"`
	TopicName        string        `config:"topic_name"`
	CreateTopic      bool          `config:"create_topic"`
	SubscriberName   string        `config:"subscriber_name"`
	SubscriberPrefix string        `config:"subscriber_prefix"`
}

var DefaultConfig = Config{
	Period:         1 * time.Second,
	ProjectID:      "env",
	TopicName:      "gpubsubbeat-all",
	SubscriberName: "gpubsubbeat-subscription",
}
