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
	"strings"
	"sync"
	"syscall"
	"time"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/server"
	"newsscope/internal/storage"
)

const ingestWorkerCount = 8

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
	defer store.Close()
	model := analyzer.New(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey, cfg.OpenAIModel, cfg.Categories, cfg.MaxBodyChars, cfg.MaxDuplicateDiffChars, cfg.MaxSummaryChars, cfg.TimeoutSec)
	api := server.New(store, cfg, logger, model)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go maintainSQLite(ctx, store, logger, cfg)
	baseContext := func(net.Listener) context.Context { return ctx }
	servers := []*http.Server{
		{Addr: cfg.CollectorAddr, Handler: api.CollectorHandler(), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: time.Duration(cfg.TimeoutSec+10) * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: cfg.NormalAddr, Handler: api.QueryHandler(storage.AudienceNormal), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: cfg.VIPAddr, Handler: api.QueryHandler(storage.AudienceVIP), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second},
		{Addr: cfg.AdminAddr, Handler: api.AdminHandler(), BaseContext: baseContext, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second},
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
	for i := 0; i < ingestWorkerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ingestWorker(ctx, store, api, logger)
		}()
	}
	for i := 0; i < cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			worker(ctx, store, model, cfg, logger)
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		idleMaintenanceWorker(ctx, api, logger, cfg)
	}()
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

func ingestWorker(ctx context.Context, store *storage.Store, api *server.API, logger *log.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, ok, err := store.ClaimIngest(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Printf("claim ingestion job: %v", err)
			wait(ctx, time.Second)
			continue
		}
		if !ok {
			store.WaitForIngest(ctx, 5*time.Second)
			continue
		}
		result, err := api.ProcessQueuedArticle(ctx, job.Article, job.Audience)
		if err != nil {
			if ctx.Err() != nil {
				releaseIngestOnShutdown(store, job, logger)
				return
			}
			if retryErr := store.RetryIngest(ctx, job, err); retryErr != nil {
				logger.Printf("retry ingestion job %d: %v", job.ID, retryErr)
			}
			logger.Printf("ingestion job %d failed (attempt %d), queued for retry: %v", job.ID, job.Attempts, err)
			continue
		}
		if err := store.CompleteIngest(ctx, job); err != nil {
			if ctx.Err() != nil {
				releaseIngestOnShutdown(store, job, logger)
				return
			}
			if retryErr := store.RetryIngest(ctx, job, err); retryErr != nil {
				logger.Printf("retry ingestion job %d after completion error: %v", job.ID, retryErr)
			}
			continue
		}
		switch result.Status {
		case "rejected":
			logger.Printf("ingestion job %d was rejected after queueing: %s", job.ID, result.Reason)
		case "deferred":
			// The server already logs why review could not run; this line adds
			// the queue id so the event can be correlated with the pending table.
			logger.Printf("ingestion job %d deferred to duplicate review as pending %d", job.ID, result.ArticleID)
		}
	}
}

func releaseIngestOnShutdown(store *storage.Store, job storage.IngestJob, logger *log.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.ReleaseIngest(ctx, job); err != nil {
		logger.Printf("release ingestion job %d: %v", job.ID, err)
	}
}

func maintainSQLite(ctx context.Context, store *storage.Store, logger *log.Logger, cfg config.Config) {
	run := func() {
		maintenanceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := store.Maintain(maintenanceCtx); err != nil && ctx.Err() == nil {
			logger.Printf("SQLite maintenance failed: %v", err)
		}
		if trimmed, err := store.TrimOldBodies(maintenanceCtx,
			time.Duration(cfg.BodyRetentionDays)*24*time.Hour, cfg.BodyRetentionChars); err != nil && ctx.Err() == nil {
			logger.Printf("body retention failed: %v", err)
		} else if trimmed > 0 {
			logger.Printf("trimmed the stored body of %d old articles to %d characters", trimmed, cfg.BodyRetentionChars)
		}
		if pruned, err := store.PruneCompletedIngest(maintenanceCtx, 24*time.Hour); err != nil && ctx.Err() == nil {
			logger.Printf("ingest queue pruning failed: %v", err)
		} else if pruned > 0 {
			logger.Printf("pruned %d completed ingest queue rows", pruned)
		}
	}
	run()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
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

// idleMaintenanceWorker runs idle maintenance, skipping the pass entirely while
// the collector is still delivering. When a pass makes no progress the gap
// between attempts widens, so a backlog that cannot be cleared stops costing
// model calls.
func idleMaintenanceWorker(ctx context.Context, api *server.API, logger *log.Logger, cfg config.Config) {
	quietFor := time.Duration(cfg.IdleRetryInterval) * time.Second
	cap := time.Duration(cfg.IdleRetryMaxBackoff) * time.Second
	backoff := time.Duration(0)

	for {
		if !sleepUnlessDone(ctx, 0) {
			return
		}
		passCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		report, err := api.IdleMaintenance(passCtx, quietFor, cfg.IdleRetryBatch)
		cancel()

		var delay time.Duration
		switch {
		case ctx.Err() != nil:
			return
		case !report.Idle:
			// Busy: wait for the stream to fall quiet again. This is the path
			// that keeps a busy period from spending the model budget on retries.
			backoff = 0
			delay = quietFor
		case err != nil:
			// Progressively widen the gap so an endpoint that cannot help is
			// probed less and less often instead of continuously.
			backoff = nextBackoff(backoff, quietFor, cap)
			logger.Printf("idle maintenance could not progress (after %s): %v", backoff, err)
			delay = backoff
		case report.Promoted+report.Discarded+report.Requeued > 0:
			// Real progress: keep draining while there is more to do.
			backoff = 0
			delay = 0
			continue
		default:
			// Nothing outstanding and nothing wrong: idle cheaply.
			backoff = 0
			delay = quietFor
		}
		if !sleepUnlessDone(ctx, delay) {
			return
		}
	}
}

// nextBackoff doubles the current gap, starting from the idle interval.
func nextBackoff(current, base, cap time.Duration) time.Duration {
	if current <= 0 {
		return base
	}
	if current >= cap {
		return cap
	}
	next := current * 2
	if next > cap {
		return cap
	}
	return next
}

// sleepUnlessDone waits for the delay and reports whether the worker should
// keep running. A zero delay means "immediately".
func sleepUnlessDone(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
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
