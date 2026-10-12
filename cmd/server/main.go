package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"collab-mvp/internal/auth"
	"collab-mvp/internal/handler"
	"collab-mvp/internal/repository"
	"collab-mvp/internal/room"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	tokens, err := auth.FromEnv()
	if err != nil {
		return err
	}
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return &configError{"MYSQL_DSN is required"}
	}
	repo, err := repository.Open(dsn)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := repo.AutoMigrate(ctx); err != nil {
		return err
	}
	rooms := room.NewManager(repo)
	defer rooms.Close()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := &http.Server{Addr: addr, Handler: handler.NewRouter(repo, rooms, tokens), ReadHeaderTimeout: 5 * time.Second}
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopCtx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("listening on %s", addr)
	err = server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

type configError struct{ message string }

func (e *configError) Error() string { return e.message }
