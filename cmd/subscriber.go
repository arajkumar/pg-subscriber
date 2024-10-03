package cmd

import (
	"github.com/spf13/cobra"

	"github.com/timescale/pg-subscriber/internal/subscription"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var run = &cobra.Command{
	Use:   "run",
	Short: "Implementation of Postgres logical replication subscriber in Go.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		subscriptions, _ := cmd.Flags().GetStringArray("subscription")
		publications, _ := cmd.Flags().GetStringArray("publication")
		subscription.Run(
			cmd.Context(),
			subscriptions,
			publications,
			cmd.Flag("source").Value.String(),
			cmd.Flag("target").Value.String())
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
