/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/timescale/pg-subscriber/internal/subscription"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var subscriptionHelperCmd = &cobra.Command{
	Use:   "upstream",
	Short: "Upstream subscription helper for TimescaleDB replication.",
	Long: `TimescaleDB extension for PostgreSQL creates chunk tables dynamically
	based on the data that is inserted into the hypertable. Since the upstream
	logical replication doesn't replicate DDL changes, the chunks that are
	created on the source need to be created on the target manually. This
	utility helps in creating the missing chunks on the target.

	Additionally, the chunks that are created on the source also need to be
	added to the publication so that the changes that are made to the chunk
	tables are replicated to the target. This utility also helps in adding the
	chunks to the publication.

	Finally, the utility refreshes the subscription on the target so that the
	changes that are made to the publication are replicated to the target.
	`,
	Run: func(cmd *cobra.Command, args []string) {
		subscriptions, _ := cmd.Flags().GetStringArray("subscription")
		subscription.Run(
			cmd.Context(),
			subscriptions,
			cmd.Flag("source").Value.String(),
			cmd.Flag("target").Value.String())
	},
}

func init() {
	rootCmd.AddCommand(subscriptionHelperCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// subscriptionHelperCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// subscriptionHelperCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	subscriptionHelperCmd.Flags().StringP("source", "s", "", "Source PGURI")
	subscriptionHelperCmd.MarkFlagRequired("source")
	subscriptionHelperCmd.Flags().StringP("target", "t", "", "Target PGURI")
	subscriptionHelperCmd.MarkFlagRequired("target")
	subscriptionHelperCmd.Flags().StringArrayP("subscription", "u", []string{}, "Subscription to refresh")
	subscriptionHelperCmd.MarkFlagRequired("subscription")
}
