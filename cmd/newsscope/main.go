package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/server"
	"newsscope/internal/storage"
)

func main() {
	logger := log.New(os.Stdout, "newsscope ", log.LstdFlags|log.LUTC|log.Lmicroseconds)
	if err := run(logger); err != nil {
		logger.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func run(logger *log.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	store, err := storage.Open(cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer store.DB.Close()
	model := analyzer.New(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey, cfg.OpenAIModel, cfg.Categories, cfg.MaxBodyChars, cfg.MaxDuplicateDiffChars, cfg.MaxSummaryChars, cfg.TimeoutSec)
	api := server.New(store, cfg, logger, model)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go maintainSQLite(ctx, store, logger)
	baseContext := func(net.Listener) context.Context { return ctx }
	servers := []*http.Server{
		{Addr: "127.0.0.1:" + itoa(cfg.CollectorPort), Handler: api.CollectorHandler(), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: time.Duration(cfg.TimeoutSec+10) * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: cfg.NormalAddr, Handler: api.QueryHandler(storage.AudienceNormal), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: cfg.VIPAddr, Handler: api.QueryHandler(storage.AudienceVIP), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: "127.0.0.1:" + itoa(cfg.AdminPort), Handler: api.AdminHandler(), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second},
	}
	errCh := make(chan error, len(servers))
	for _, srv := range servers {
		srv := srv
		go func() {
			logger.Printf("listening on %s", srv.Addr)
			err := srv.ListenAndServe()
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}
	var workers sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			worker(ctx, store, model, cfg, logger)
		}()
	}
	var serverErr error
	select {
	case <-ctx.Done():
	case err = <-errCh:
		serverErr = fmt.Errorf("HTTP server stopped unexpectedly: %w", err)
		cancel()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil && serverErr == nil {
			serverErr = fmt.Errorf("shut down HTTP server on %s: %w", srv.Addr, err)
		}
	}
	workers.Wait()
	return serverErr
}

func maintainSQLite(ctx context.Context, store *storage.Store, logger *log.Logger) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			maintenanceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := store.Maintain(maintenanceCtx); err != nil && ctx.Err() == nil {
				logger.Printf("SQLite maintenance failed: %v", err)
			}
			cancel()
		}
	}
}

func worker(ctx context.Context, store *storage.Store, model *analyzer.Client, cfg config.Config, logger *log.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, ok, err := store.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Printf("claim analysis job: %v", err)
			wait(ctx, time.Second)
			continue
		}
		if !ok {
			store.WaitForJob(ctx, 5*time.Second)
			continue
		}
		analysisCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSec+5)*time.Second)
		result, err := model.Analyze(analysisCtx, job.Title, job.Body)
		cancel()
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		if err == nil {
			err = store.Complete(ctx, job, result.Category, result.Keywords, result.Summary, result.Importance, cfg.OpenAIModel)
		}
		if err != nil {
			if ctx.Err() != nil {
				releaseOnShutdown(store, job, logger)
				return
			}
			logger.Printf("analysis for article %d failed (attempt %d): %v", job.ArticleID, job.Attempts, err)
			if e := store.Fail(ctx, job, cfg.MaxAttempts, err); e != nil {
				logger.Printf("save analysis failure for article %d: %v", job.ArticleID, e)
				if ctx.Err() != nil {
					releaseOnShutdown(store, job, logger)
					return
				}
			}
			continue
		}
		logger.Printf("analysis completed for article %d (%s)", job.ArticleID, strings.TrimSpace(result.Category))
	}
}

func releaseOnShutdown(store *storage.Store, job storage.Job, logger *log.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Release(ctx, job); err != nil {
		logger.Printf("release analysis job for article %d: %v", job.ArticleID, err)
	}
}

func wait(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
func itoa(n int) string { return strconv.Itoa(n) }
