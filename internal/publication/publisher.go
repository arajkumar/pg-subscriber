package publication

import (
	"context"
	"fmt"

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
	query := `SELECT
		schemaname,
		tablename
		FROM pg_publication_tables
		WHERE pubname = $1::name`

	rows, err := p.source.Query(ctx, query, p.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fmt.Println("Fetching tables for publication: ", p.name)

	var tables []api.PublicationRelation
	for rows.Next() {
		var table api.PublicationRelation
		err := rows.Scan(&table.SchemaName, &table.TableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}
