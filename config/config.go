// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period      time.Duration `config:"period"`
	ProjectID   string        `config:"projectid"`
	MaxFetchMsg int           `config:"maxfetchmsg"`
	Region      string        `config:"region"`
}

var DefaultConfig = Config{
	Period:      1 * time.Second,
	ProjectID:   "env",
	Region:      "us-central1",
}