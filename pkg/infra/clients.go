package infra

import (
	"github.com/secmon-lab/swarm/pkg/domain/interfaces"
	"github.com/secmon-lab/swarm/pkg/infra/policy"
)

type Clients struct {
	bq     interfaces.BigQuery
	cs     interfaces.CloudStorage
	topic  interfaces.PubSubTopic
	sub    interfaces.PubSubSubscription
	policy *policy.Client
	db     interfaces.Database
}

func New(options ...Option) *Clients {
	c := &Clients{}
	for _, option := range options {
		option(c)
	}

	return c
}

func (x *Clients) BigQuery() interfaces.BigQuery         { return x.bq }
func (x *Clients) CloudStorage() interfaces.CloudStorage { return x.cs }
func (x *Clients) PubSub() interfaces.PubSubTopic        { return x.topic }
func (x *Clients) PubSubSubscription() interfaces.PubSubSubscription {
	return x.sub
}
func (x *Clients) Policy() *policy.Client        { return x.policy }
func (x *Clients) Database() interfaces.Database { return x.db }

type Option func(*Clients)

func WithBigQuery(bq interfaces.BigQuery) Option {
	return func(c *Clients) {
		c.bq = bq
	}
}

func WithCloudStorage(cs interfaces.CloudStorage) Option {
	return func(c *Clients) {
		c.cs = cs
	}
}

func WithPubSubTopic(topic interfaces.PubSubTopic) Option {
	return func(c *Clients) {
		c.topic = topic
	}
}

func WithPubSubSubscription(sub interfaces.PubSubSubscription) Option {
	return func(c *Clients) {
		c.sub = sub
	}
}

func WithPolicy(policy *policy.Client) Option {
	return func(c *Clients) {
		c.policy = policy
	}
}

func WithDatabase(db interfaces.Database) Option {
	return func(c *Clients) {
		c.db = db
	}
}
