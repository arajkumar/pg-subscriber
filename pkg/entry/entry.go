package entry

import (
	"context"
	"fmt"
	"math"

	"github.com/timescale/timescaledb-cdc/pkg/conn"
	"github.com/timescale/timescaledb-cdc/pkg/publication"
	"github.com/timescale/timescaledb-cdc/pkg/subscription"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

var NeverEnd pglogrepl.LSN = pglogrepl.LSN(math.MaxUint64)

func Run(ctx context.Context, sourceConn, targetConn string, publications, subscriptions []string, endLSN pglogrepl.LSN) error {
	zap.L().Info("Starting..")
	source, err := conn.Parse(sourceConn)
	if err != nil {
		return fmt.Errorf("Error parsing source conn string: %w", err)
	}

	target, err := conn.Parse(targetConn)
	if err != nil {
		return fmt.Errorf("Error parsing target conn string: %w", err)
	}

	pub, err := publication.New(ctx, publications, source.AsSource())
	if err != nil {
		return fmt.Errorf("Error creating publication: %w", err)
	}

	sub, err := subscription.New(ctx, subscriptions[0], target.AsTarget(), pub)
	if err != nil {
		return fmt.Errorf("Error creating subcription: %w", err)
	}

	err = sub.Sync(ctx, endLSN)
	if err != nil {
		return fmt.Errorf("Error on sync: %w", err)
	}

	return nil
}
