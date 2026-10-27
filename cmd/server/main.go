package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhook-platform/internal/api"
	"webhook-platform/internal/app"
	"webhook-platform/internal/auth"
	"webhook-platform/internal/urlcheck"
)

func main() {
	addr := flag.String("addr", ":8097", "HTTP listen address")
	db := flag.String("db", "data/webhooks.db", "SQLite database path")
	dev := flag.Bool("dev", true, "enable POST /dev/token and allow http://localhost endpoints")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *db, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, db string, dev bool, logger *slog.Logger) error {
	secret, internal := os.Getenv("JWT_SECRET"), os.Getenv("INTERNAL_TOKEN")
	if secret == "" || internal == "" {
		if !dev {
			return errors.New("JWT_SECRET and INTERNAL_TOKEN are required when -dev=false")
		}
		secret, internal = "dev-only-secret", "dev-internal-token"
		logger.Warn("JWT_SECRET / INTERNAL_TOKEN not set, using insecure development values")
	}

	opts := app.DefaultOptions(db)
	opts.Logger = logger
	opts.URLs = urlcheck.Policy{AllowLoopbackHTTP: dev}
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a.Start(ctx)

	srv := &http.Server{
		Addr: addr,
		Handler: api.NewRouter(api.Deps{Signer: auth.NewSigner([]byte(secret)), Endpoints: a.Endpoints, Store: a.Store,
			Publisher: a, InternalToken: internal, Log: logger, Dev: dev}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("webhook platform listening", "addr", addr, "db", db)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
