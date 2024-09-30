package subscription

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/pg-subscriber/internal/api"
)

type subscriber struct {
	name        string
	source      *pgxpool.Conn
	target      *pgxpool.Conn
	publisher api.Publisher
}

// Create a new subscriber
// TODO: Support multiple publications
func New(name string, source *pgxpool.Conn, target *pgxpool.Conn, publisher api.Publisher) api.Subscriber {
	return &subscriber{
		name,
		source,
		target,
		publisher,
	}
}

func (s *subscriber) upsertIntoCatalog(ctx context.Context, t *api.PublicationRelation) error {
	return nil
}

// Fetches the list of tables that are part of the publication and
// populates go_pg_subscriber.subscription_rel
func (s *subscriber) Refresh(ctx context.Context) error {
	tables, err := s.publisher.FetchTables(ctx)
	for _, t := range tables {
		err = s.upsertIntoCatalog(ctx, &t)
		if err != nil {
			return err
		}
	}

	return err
}

// Implements the Subscriber interface Name method
func (s *subscriber) Name() string {
	return s.name
}
