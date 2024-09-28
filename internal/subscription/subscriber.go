package subscription

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Subscriber struct {
	name         string
	source       *pgxpool.Conn
	target       *pgxpool.Conn
	publication  string
}

// Create a new subscriber
// TODO: Support multiple publications
func (s *Subscriber) New(name string, source *pgxpool.Conn, target *pgxpool.Conn, publication string) Subscriber {
	return Subscriber{
		name: name,
		source: source,
		target: target,
		publication: publication,
	}
}

// Fetches the list of tables that are part of the publication and
// populates go_pg_subscriber.subscription_rel
func (s *Subscriber) Refresh(ctx context.Context) error {
	pubTables, err := s.getPublicationTables(ctx)
}
