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

	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
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
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
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

	sourceAssert.HasReplicationSlot("sub")
	targetAssert.HasReplicationOrigin("sub")

	{
		// Launching again shouldn't cause error
		ctx, _ := context.WithTimeout(ctx, 1*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestReplicationSlotAndOriginExistence(t *testing.T) {
	ctx := context.TODO()
	dbs := prepareDBS(t, ctx)

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

	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	var exists bool
	// Verify the existence of replication slot on source
	err := dbs.source.QueryRow(t, ctx,
		`SELECT true FROM pg_stat_replication_slots
		 WHERE slot_name=$1`, "sub").Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists)

	// Verify the existence of replication origin on target
	err = dbs.target.QueryRow(t, ctx,
		`SELECT true FROM pg_replication_origin
		 WHERE roname=$1`, "sub").Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists)

	{
		// Launching again shouldn't cause error
		ctx, _ := context.WithTimeout(ctx, 1*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestLiveReplicationWithoutExistingData(t *testing.T) {
	// This should be equval to creating subscription with
	// copy_data option as false.
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
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}
