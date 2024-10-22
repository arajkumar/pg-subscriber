package postgres

import (
	"context"
	"fmt"
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

	switch {
	case version < PG12:
		panic(fmt.Errorf("Unsupported PostgreSQL version: %d", version))
	case version < PG13:
		return PG12
	case version < PG14:
		return PG13
	case version < PG15:
		return PG14
	case version < PG16:
		return PG15
	case version < PG17:
		return PG16
	default:
		fmt.Printf("WARNING: Newer PostgreSQL version %d than supported, using latest supported version %d\n", version, PG17)
		return PG17
	}
}
