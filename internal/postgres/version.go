package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
)

const (
	// The minimum version of PostgreSQL that we support
	MinVersion = 120000

	PG12 = 120000
	PG13 = 130000
	PG14 = 140000
	PG15 = 150000
	PG16 = 160000
	PG17 = 170000
)

func Version(ctx context.Context, conn *pgx.Conn) uint {
	var version uint
	const versionSql = "SELECT current_setting('server_version_num')"
	// This shouldn't fail, but if it does, we want to panic
	if err := conn.QueryRow(ctx, versionSql).Scan(&version); err != nil {
		panic(err)
	}

	return version
}
