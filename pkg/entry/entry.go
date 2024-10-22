package entry

import (
	"context"
	"fmt"

	"github.com/timescale/timescaledb-cdc/pkg/conn"
	"github.com/timescale/timescaledb-cdc/pkg/publication"
	"github.com/timescale/timescaledb-cdc/pkg/subscription"

	"go.uber.org/zap"
)

func Run(ctx context.Context, sourceConn, targetConn string, publications, subscriptions []string) error {
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

	err = sub.Sync(ctx)
	if err != nil {
		return fmt.Errorf("Error on sync: %w", err)
	}

	return nil
}
