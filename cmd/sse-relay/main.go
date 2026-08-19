// Command sse-relay runs the relay as a standalone HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jamesh337/sse-relay/internal/hub"
	"github.com/jamesh337/sse-relay/internal/relay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sse-relay:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", ":8080", "listen address")
	buffer := flag.Int("buffer", 1024, "events kept per stream for replay")
	heartbeat := flag.Duration("heartbeat", 15*time.Second, "delay between heartbeat comment frames")
	retry := flag.Duration("retry", 2*time.Second, "reconnect delay advertised to clients")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
	flag.Parse()

	h := hub.New(*buffer)
	srv := relay.NewServer(h, relay.Config{
		Heartbeat: *heartbeat,
		RetryHint: *retry,
		Token:     os.Getenv("RELAY_TOKEN"),
	})

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: srv,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("sse-relay listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down")

	// Finish every stream before the listener stops accepting requests, so
	// open subscriptions receive event: done instead of being cut mid frame.
	h.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
