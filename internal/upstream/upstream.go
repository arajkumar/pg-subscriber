package upstream

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/timescale/pg-subscriber/internal/timescale"
	"time"
)

func refreshSubscription(ctx context.Context, conn *pgx.Conn, subscriptions []string) error {
	for _, subscriptionName := range subscriptions {
		sql := fmt.Sprintf("ALTER SUBSCRIPTION %s REFRESH PUBLICATION",
			pgx.Identifier{subscriptionName}.Sanitize())
		_, err := conn.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func Run(ctx context.Context, subscriptions []string, publications []string, sourcePGURI string, targetPGURI string) {
	sourceConn, err := pgx.Connect(ctx, sourcePGURI)
	if err != nil {
		panic(err)
	}

	targetConn, err := pgx.Connect(ctx, targetPGURI)
	if err != nil {
		panic(err)
	}

	// Tracks whether a chunk has been created on the target.
	// We couldn't rely on pg_subscription as we don't have privileges
	// to query it.
	chunksOnSubscription := make(map[string]struct{})

	for {

		chunks, err := timescale.ListChunksOnPublication(ctx, sourceConn, publications)
		if err != nil {
			panic(err)
		}

		// TODO: Handle chunks which are automatically added to publication
		// due to FOR ALL TABLES or FOR ALL TABLES IN SCHEMA clause in
		// CREATE PUBLICATION statement.
		for _, chunk := range chunks {
			if _, ok := chunksOnSubscription[chunk.String()]; !ok {
				fmt.Println("Creating chunk on target: ", chunk)
				err := chunk.Create(ctx, targetConn)
				if err != nil {
					fmt.Println("Error creating chunk: ", err)
					continue
				}
				chunksOnSubscription[chunk.String()] = struct{}{}
			}
		}

		for _, chunk := range chunks {
			if !chunk.IsPublished {
				fmt.Println("Adding chunk to publication: ", chunk)
				err := chunk.AddToPublication(ctx, sourceConn)
				if err != nil {
					fmt.Println("Error adding chunk to publication: ", err)
					continue
				}
			}
		}

		if len(chunks) > 0 {
			err := refreshSubscription(ctx, targetConn, subscriptions)
			if err != nil {
				fmt.Println("Error refreshing subscription: ", err)
			}
		}

		time.Sleep(2 * time.Second)
	}
}
