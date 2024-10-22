package e2e

import (
	"context"
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

func (d *TestDB) Query(t *testing.T, ctx context.Context, query string, params ...interface{}) pgx.Row {
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
