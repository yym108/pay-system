package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/yym108/pay-system/internal/bootstrap"
)

func main() {
	if err := run(); err != nil {
		slog.Error("bootstrap failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	accountPass, err := passwordFromDSN("ACCOUNT_DB_DSN_0", bootstrap.UserAccount)
	if err != nil {
		return err
	}
	accountPass1, err := passwordFromDSN("ACCOUNT_DB_DSN_1", bootstrap.UserAccount)
	if err != nil {
		return err
	}
	if accountPass != accountPass1 {
		return fmt.Errorf("ACCOUNT_DB_DSN_0 and ACCOUNT_DB_DSN_1 passwords differ")
	}
	paymentPass, err := passwordFromDSN("MYSQL_DSN", bootstrap.UserPayment)
	if err != nil {
		return err
	}
	cfg := bootstrap.Config{
		Host:                envOr("MYSQL_HOST", "127.0.0.1"),
		Port:                envOr("MYSQL_PORT", "3306"),
		RootPassword:        os.Getenv("MYSQL_ROOT_PASSWORD"),
		AccountPassword:     accountPass,
		PaymentPassword:     paymentPass,
		ReconPassword:       os.Getenv("MYSQL_RECON_PASSWORD"),
		ChannelMockPassword: os.Getenv("MYSQL_CHANNEL_MOCK_PASSWORD"),
		MigratorPassword:    os.Getenv("MYSQL_MIGRATOR_PASSWORD"),
	}
	if err := bootstrap.Apply(ctx, cfg); err != nil {
		return err
	}
	slog.Info("database users ready")
	return nil
}

func passwordFromDSN(envName, wantUser string) (string, error) {
	user, password, err := bootstrap.SplitUserPassword(os.Getenv(envName))
	if err != nil {
		return "", fmt.Errorf("%s: %w", envName, err)
	}
	if user != wantUser {
		return "", fmt.Errorf("%s user is %s, want %s", envName, user, wantUser)
	}
	return password, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
