package publication

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/timescale/pg-subscriber/internal/api"
)

type publisher struct {
	name   string
	source *pgxpool.Pool
}

func New(name string, source *pgxpool.Pool) api.Publisher {
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
func (p *publisher) FetchTables(ctx context.Context) ([]api.PublicationRelation, error) {
	zap.L().Debug("Fetching tables for publication", zap.String("publication", p.name))
	query := `SELECT
		schemaname,
		tablename
		FROM pg_publication_tables
		WHERE pubname = $1::name`

	rows, err := p.source.Query(ctx, query, p.name)
	if err != nil {
		return nil, fmt.Errorf("error fetching tables for publication %s: %w", p.name, err)
	}
	defer rows.Close()

	var tables []api.PublicationRelation
	for rows.Next() {
		var table api.PublicationRelation
		err := rows.Scan(&table.SchemaName, &table.TableName)
		if err != nil {
			return nil, fmt.Errorf("error scanning table: %w", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}
