package publication

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/pg-subscriber/internal/api"
)

type publisher struct {
	name   string
	source *pgxpool.Conn
}

func NewPublisher(name string, source *pgxpool.Conn) api.Publisher {
	return &publisher{
		name,
		source,
	}
}

// Implements the Publisher interface Name method
func (p *publisher) Name() string {
	return p.name
}

// Implements the Publisher interface FetchTables method
func (p *publisher) FetchTables(ctx context.Context) ([]api.Relation, error) {
	return nil, nil
}
