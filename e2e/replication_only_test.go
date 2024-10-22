package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/timescale/timescaledb-cdc/pkg/entry"
)

func TestLiveReplication(t *testing.T) {
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

	dbs.Exec(t, ctx,
	`CREATE TABLE metrics (
		id integer,
		time timestamptz,
		name text,
		value numeric)`)

	publications := []string {
		"pub",
	}
	subscriptions := []string {
		"sub",
	}

	err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
		publications,
		subscriptions)
	require.NoError(t, err)
}
