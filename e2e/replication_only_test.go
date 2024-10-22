package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/timescale/timescaledb-cdc/pkg/entry"
)

func TestCatalogPopulation(t *testing.T) {
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

	ddl := `
	CREATE TABLE metrics (id integer, time timestamptz, name text, value numeric);
	CREATE PUBLICATION pub FOR TABLE metrics;
	`
	dbs.Exec(t, ctx, ddl)

	publications := []string{
		"pub",
	}
	subscriptions := []string{
		"sub",
	}

	{
		// This should be pretty quick
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	var exists bool
	dbs.target.QueryRow(t, ctx,
		`SELECT true FROM _timescaledb_cdc.subscription_rel WHERE subname=$1
		 AND schemaname=$2 AND tablename=$3`,
		"sub", "public", "metrics").Scan(&exists)

	require.True(t, exists)
}
