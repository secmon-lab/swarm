package infra

import (
	"github.com/m-mizutani/swarm/pkg/domain/interfaces"
	"github.com/m-mizutani/swarm/pkg/infra/policy"
)

type Clients struct {
	bq     interfaces.BigQuery
	cs     interfaces.CloudStorage
	pubsub interfaces.PubSub
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
func (x *Clients) PubSub() interfaces.PubSub             { return x.pubsub }
func (x *Clients) Policy() *policy.Client                { return x.policy }
func (x *Clients) Database() interfaces.Database         { return x.db }

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

func WithPubSub(pubsub interfaces.PubSub) Option {
	return func(c *Clients) {
		c.pubsub = pubsub
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
