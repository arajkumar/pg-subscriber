package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

type TestDB struct {
	container *postgres.PostgresContainer
}

type TestDBS struct {
	source *TestDB
	target *TestDB
}

type DBAssert struct {
	*TestDB
	name string
	t    *testing.T
	ctx  context.Context
	conn *pgx.Conn
}

func prepareDBS(t *testing.T, ctx context.Context) TestDBS {
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	sourceCon, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "log_statement=all",
					"-c", "wal_level=logical",
				},
			},
		}),
	)

	require.NoError(t, err)
	require.NotNilf(t, sourceCon, "source container is nil")

	targetCon, err := postgres.Run(ctx,
		"docker.io/timescale/timescaledb-ha:pg16-ts2.16",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)

	require.NotNilf(t, targetCon, "target container is nil")
	require.NoError(t, err)

	dbs := TestDBS{
		&TestDB{container: sourceCon},
		&TestDB{container: targetCon},
	}

	return dbs
}

func (d *TestDB) QueryRow(t *testing.T, ctx context.Context, query string, params ...interface{}) pgx.Row {
	dbURL, err := d.container.ConnectionString(ctx)
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	return conn.QueryRow(ctx, query, params...)
}

func (d *TestDB) Exec(t *testing.T, ctx context.Context, query string, params ...interface{}) {
	dbURL, err := d.container.ConnectionString(ctx)
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, query, params...)
	require.NoError(t, err)
}

func (d *TestDB) Conn(t *testing.T, ctx context.Context) string {
	dbURL, err := d.container.ConnectionString(ctx)
	require.NoError(t, err)

	return dbURL
}

func (d *TestDBS) Exec(t *testing.T, ctx context.Context, query string, params ...interface{}) {
	d.source.Exec(t, ctx, query, params...)
	d.target.Exec(t, ctx, query, params...)
}

func NewDBAssert(name string, db *TestDB, t *testing.T, ctx context.Context) DBAssert {
	dbURL, err := db.container.ConnectionString(ctx)
	require.NoError(t, err)

	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)

	return DBAssert{
		TestDB: db,
		name:   name,
		t:      t,
		ctx:    ctx,
		conn:   conn,
	}
}

func (d *DBAssert) HasReplicationSlot(slot string) {
	var exists bool
	// Verify the existence of replication slot on source
	err := d.TestDB.QueryRow(d.t, d.ctx,
		`SELECT true FROM pg_stat_replication_slots
		 WHERE slot_name=$1`, slot).Scan(&exists)
	require.NoError(d.t, err)
	require.True(d.t, exists)
}

func (d *DBAssert) HasReplicationOrigin(origin string) {
	var exists bool
	// Verify the existence of replication slot on source
	err := d.TestDB.QueryRow(d.t, d.ctx,
		`SELECT true FROM pg_replication_origin
		 WHERE roname=$1`, origin).Scan(&exists)
	require.NoError(d.t, err)
	require.True(d.t, exists)
}

type SubscriptionRel struct {
	SubName string
	Schema  string
	Table   string
	Exists  bool
}

func (d *DBAssert) SubscriptionHasRels(rels []SubscriptionRel) {
	for _, r := range rels {
		var exists bool
		err := d.TestDB.QueryRow(d.t, d.ctx,
			`SELECT true FROM _timescaledb_cdc.subscription_rel WHERE subname=$1
		AND schemaname=$2 AND tablename=$3 AND state='i'`,
			r.SubName, r.Schema, r.Table).Scan(&exists)
		errMsg := fmt.Sprintf("subname:%s schema:%s:table %s expected:%t",
			r.SubName, r.Schema, r.Table, r.Exists)
		if !r.Exists {
			require.ErrorIs(d.t, err, pgx.ErrNoRows, errMsg)
		} else {
			require.NoError(d.t, err, errMsg)
		}
		require.Equal(d.t, r.Exists, exists, errMsg)
	}
}

func (d *DBAssert) HasTableCount(rel string, expectedCount int) {
	q := fmt.Sprintf(`SELECT count(*) FROM %s`, rel)
	var count int
	err := d.TestDB.QueryRow(d.t, d.ctx, q).Scan(&count)
	require.NoError(d.t, err)
	require.Equal(d.t, expectedCount, count)
}

func (d *DBAssert) HasSubsciptionRelsCount(subname string, expectedCount int) {
	q := `SELECT count(*) FROM _timescaledb_cdc.subscription_rel WHERE subname=$1`
	var count int
	err := d.TestDB.QueryRow(d.t, d.ctx, q, subname).Scan(&count)
	require.NoError(d.t, err)
	require.Equal(d.t, expectedCount, count)
}
