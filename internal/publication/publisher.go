package publication

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type Relation struct {
	SchemaName string
	TableName  string
	AttNames   []string
}

type PublicationRelation struct {
	Relation
	RowFilter string
}

type Publisher struct {
	name   string
	source *pgx.Conn
}

func New(publications []string, source *pgx.Conn) *Publisher {
	if len(publications) > 1 {
		panic("Multiple publications are not supported yet.")
	}

	name := publications[0]
	return &Publisher{
		name,
		source,
	}
}

func (p *Publisher) FetchTables(ctx context.Context) ([]PublicationRelation, error) {
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

	var tables []PublicationRelation
	for rows.Next() {
		var table PublicationRelation
		err := rows.Scan(&table.SchemaName, &table.TableName)
		if err != nil {
			return nil, fmt.Errorf("error scanning table: %w", err)
		}
		tables = append(tables, table)
	}

	zap.L().Debug("Fetched tables from publication", zap.String("publication", p.name), zap.Int("count", len(tables)))

	return tables, nil
}
