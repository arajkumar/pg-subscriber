package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

	{
		// This should be pretty quick. It also creates replication slot
		// on the source, replication origin on target.
		ctx, _ := context.WithTimeout(ctx, 2*time.Second)
		err := entry.Run(ctx, dbs.source.Conn(t, ctx), dbs.target.Conn(t, ctx),
			publications,
			subscriptions)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	// Verify the catalog is populated with right state.
	catalogCheck := func(subname, schema, table string, expectedExists bool) {
		var exists bool
		err := dbs.target.QueryRow(t, ctx,
			`SELECT true FROM _timescaledb_cdc.subscription_rel WHERE subname=$1
		AND schemaname=$2 AND tablename=$3 AND state='i'`,
			subname, schema, table).Scan(&exists)
		errMsg := fmt.Sprintf("Failed for subname %s schema %s table %s expected %t",
			subname, schema, table, expectedExists)
		if !expectedExists {
			require.ErrorIs(t, err, pgx.ErrNoRows, errMsg)
		}
		require.Equal(t, expectedExists, exists, errMsg)
	}

	for _, t := range []struct {
		subname string
		schema  string
		table   string
		exists  bool
	}{
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics",
			exists:  true,
		},
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics1",
			exists:  true,
		},
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics2",
			exists:  false,
		},
	} {
		catalogCheck(t.subname, t.schema, t.table, t.exists)
	}

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
	for _, t := range []struct {
		subname string
		schema  string
		table   string
		exists  bool
	}{
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics",
			exists:  false,
		},
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics1",
			exists:  true,
		},
		{
			subname: "sub",
			schema:  "public",
			table:   "metrics2",
			exists:  true,
		},
	} {
		catalogCheck(t.subname, t.schema, t.table, t.exists)
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

	var exists bool
	dbs.target.QueryRow(t, ctx,
		`SELECT true FROM _timescaledb_cdc.subscription_rel WHERE subname=$1
		 AND schemaname=$2 AND tablename=$3`,
		"sub", "public", "metrics").Scan(&exists)

	require.True(t, exists)
}
