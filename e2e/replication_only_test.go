package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/timescale/timescaledb-cdc/pkg/entry"
)

func TestCatalogPopulation(t *testing.T) {
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

	ddl := `
	CREATE TABLE metrics (id integer, time timestamptz, name text, value numeric);
	CREATE TABLE metrics1 (LIKE metrics);
	CREATE TABLE metrics2 (LIKE metrics);
	`
	dbs.Exec(t, ctx, ddl)

	dbs.source.Exec(t, ctx, `CREATE PUBLICATION pub FOR TABLE metrics, metrics1`)

	publications := []string{
		"pub",
	}
	subscriptions := []string{
		"sub",
	}

	sourceAssert := NewDBAssert("source", dbs.source, t, ctx)
	targetAssert := NewDBAssert("target", dbs.target, t, ctx)

	sourceAssert.HasPublicationRelsCount("pub", 2)
	endLSN := dbs.source.WalFlushLSN(t, ctx)

	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
	}

	rels := []SubscriptionRel{
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics",
			Exists:  true,
		},
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics1",
			Exists:  true,
		},
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics2",
			Exists:  false,
		},
	}

	targetAssert.SubscriptionHasRels(rels)
	targetAssert.HasSubsciptionRelsCount("sub", 2)

	// Add metrics2, remove metrics
	dbs.source.Exec(t, ctx, `ALTER PUBLICATION pub ADD TABLE metrics2`)
	dbs.source.Exec(t, ctx, `ALTER PUBLICATION pub DROP TABLE metrics`)

	{
		// Run again to auto refresh catalog
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions, 0)
		require.NoError(t, err)
	}
	rels = []SubscriptionRel{
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics",
			Exists:  false,
		},
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics1",
			Exists:  true,
		},
		{
			SubName: "sub",
			Schema:  "public",
			Table:   "metrics2",
			Exists:  true,
		},
	}

	targetAssert.SubscriptionHasRels(rels)
	targetAssert.HasSubsciptionRelsCount("sub", 2)

	{
		// Launching again shouldn't cause error
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
	}
}

func TestReplicationSlotAndOriginExistence(t *testing.T) {
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

	sourceAssert := NewDBAssert("source", dbs.source, t, ctx)
	targetAssert := NewDBAssert("target", dbs.target, t, ctx)

	ddl := `
	CREATE TABLE metrics (id integer, time timestamptz, name text, value numeric);
	`
	dbs.Exec(t, ctx, ddl)
	dbs.source.Exec(t, ctx, `CREATE PUBLICATION pub FOR TABLE metrics`)

	publications := []string{
		"pub",
	}
	subscriptions := []string{
		"sub",
	}

	endLSN := dbs.source.WalFlushLSN(t, ctx)
	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
	}

	sourceAssert.HasReplicationSlot("sub")
	targetAssert.HasReplicationOrigin("sub")

	{
		// Launching again shouldn't cause error
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
	}
}

func TestLiveReplicationWithoutExistingData(t *testing.T) {
	// This should be equval to creating subscription with
	// copy_data option as false.
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

	ddl := `
	CREATE TABLE metrics (id INTEGER, time TIMESTAMPTZ, name TEXT, value NUMERIC,
		PRIMARY KEY(id, time)
	);
	CREATE PUBLICATION pub FOR TABLE metrics;
	`
	dbs.Exec(t, ctx, ddl)

	publications := []string{
		"pub",
	}
	subscriptions := []string{
		"sub",
	}

	sourceAssert := NewDBAssert("source", dbs.source, t, ctx)
	targetAssert := NewDBAssert("target", dbs.target, t, ctx)

	sourceAssert.HasTableCount("metrics", 0)
	targetAssert.HasTableCount("metrics", 0)

	endLSN := dbs.source.WalFlushLSN(t, ctx)
	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
	}

	sourceAssert.HasPublicationRelsCount("pub", 1)
	sourceAssert.HasReplicationSlot("sub")
	targetAssert.HasReplicationOrigin("sub")

	insert := `
		INSERT INTO metrics(id, time, name, value)
		SELECT random(), time, 'metric_' || random(), random() FROM
		generate_series('2024-01-01 00:00:00', '2024-01-31 23:00:00', INTERVAL'1 hour') as time LIMIT 10;
		`
	dbs.source.Exec(t, ctx, insert)

	sourceAssert.HasTableCount("metrics", 10)

	endLSN = dbs.source.WalFlushLSN(t, ctx)

	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
		targetAssert.HasTableCount("metrics", 10)
	}

	// Run multiple times to check whether the tool is capable of ignoring
	// already applied txns.
	for range 3 {
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions,
			endLSN,
		)
		require.NoError(t, err)
		targetAssert.HasTableCount("metrics", 10)
	}
}
