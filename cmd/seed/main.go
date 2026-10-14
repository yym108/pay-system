package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/yym108/pay-system/internal/seed"
)

func main() {
	if err := run(); err != nil {
		slog.Error("seed failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key, err := base64.StdEncoding.DecodeString(os.Getenv("SECRET_MASTER_KEY"))
	if err != nil {
		return fmt.Errorf("SECRET_MASTER_KEY is not base64")
	}
	subs := seed.DefaultPlatformSubAccounts
	if raw := os.Getenv("PLATFORM_SUB_ACCOUNTS"); raw != "" {
		subs, err = strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("PLATFORM_SUB_ACCOUNTS is not an integer")
		}
	}
	result, err := seed.Run(ctx, seed.Config{
		AccountDSN0:         os.Getenv("ACCOUNT_DB_DSN_0"),
		AccountDSN1:         os.Getenv("ACCOUNT_DB_DSN_1"),
		PaymentDSN:          os.Getenv("MYSQL_DSN"),
		MasterKey:           key,
		PlatformSubAccounts: subs,
		SecretFormat:        os.Getenv("DEMO_APP_SECRET_FORMAT"),
		Load:                os.Getenv("SEED_LOAD") == "1",
	})
	if err != nil {
		return err
	}
	slog.Info("seed applied",
		"accounts_shard_0", result.Accounts[0],
		"accounts_shard_1", result.Accounts[1],
		"merchants", result.Merchants,
		"load", os.Getenv("SEED_LOAD") == "1",
	)
	return nil
}
