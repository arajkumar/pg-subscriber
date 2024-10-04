package cmd

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/timescale/pg-subscriber/internal/publication"
	"github.com/timescale/pg-subscriber/internal/subscription"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var run = &cobra.Command{
	Use:   "run",
	Short: "Implementation of Postgres logical replication subscriber in Go.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		zap.L().Info("Starting pg-subscriber")
		subscriptions, _ := cmd.Flags().GetStringArray("subscription")
		publications, _ := cmd.Flags().GetStringArray("publication")

		if len(publications) > 1 {
			panic("Multiple publications are not supported yet.")
		}

		source, _ := cmd.Flags().GetString("source")
		target, _ := cmd.Flags().GetString("target")

		sourcePool, err := pgxpool.New(cmd.Context(), source)
		if err != nil {
			panic(err)
		}

		targetPool, err := pgxpool.New(cmd.Context(), target)
		if err != nil {
			panic(err)
		}

		pub := publication.New(publications[0], sourcePool)
		sub := subscription.New(subscriptions[0], sourcePool, targetPool, pub)

		sub.Run(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(run)

	run.Flags().StringP("source", "s", "", "Source PGURI")
	run.MarkFlagRequired("source")
	run.Flags().StringP("target", "t", "", "Target PGURI")
	run.MarkFlagRequired("target")
	run.Flags().StringArrayP("subscription", "u", []string{}, "Subscription to refresh")
	run.MarkFlagRequired("subscription")
	run.Flags().StringArrayP("publication", "p", []string{}, "Publications to fetch chunks from")
	run.MarkFlagRequired("publication")
}
