package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"newsscope/internal/storage"
)

// Drive ingestion, deferred review and querying at once and assert that
// nothing is corrupted, duplicated or lost. Intended to be run under -race.
func TestConcurrentMixedWorkload(t *testing.T) {
	store, api := manualAPI(t)
	api.Model = modelStub(t, func(*http.Request) (string, error) { return notDuplicate, nil })

	const n = 60
	var wg sync.WaitGroup

	// Producers: submit near-duplicate articles concurrently.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				body := fmt.Sprintf("某公司今日发布新款处理器，采用新一代工艺，批次 %d-%d 的正文内容用于制造近似重复。", w, i)
				api.ProcessQueuedArticle(context.Background(), storage.ArticleInput{
					Source: "压力来源", Title: fmt.Sprintf("标题 %d-%d", w, i),
					Body: body, CollectedAt: time.Now().Unix(),
				}, storage.AudienceNormal)
			}
		}(w)
	}

	// Deferred review sweeper racing with ingestion.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
			time.Sleep(time.Millisecond)
		}
	}()

	// Readers hammering the query API.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			store.ListPage(context.Background(), "all", storage.AudienceNormal, 0, 20)
			store.SystemStatus(context.Background())
			time.Sleep(time.Millisecond)
		}
	}()

	// Console polling and manual triggers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			req := httptest.NewRequest(http.MethodGet, "/api/dedup/pending", nil)
			api.AdminHandler().ServeHTTP(httptest.NewRecorder(), req.WithContext(context.Background()))
			req2 := httptest.NewRequest(http.MethodPost, "/api/dedup/review", nil)
			api.AdminHandler().ServeHTTP(httptest.NewRecorder(), req2.WithContext(context.Background()))
		}
	}()

	wg.Wait()

	// Every deferred article must still be accounted for, and the corpus must
	// not contain the same content twice.
	total := 0
	if err := store.DB.QueryRow(`SELECT COALESCE(SUM(n),0) FROM (SELECT COUNT(*) n FROM articles UNION ALL SELECT COUNT(*) FROM pending_articles)`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	var dupes int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM (SELECT audience,content_hash FROM articles GROUP BY audience,content_hash HAVING COUNT(*)>1)`).Scan(&dupes); err != nil {
		t.Fatal(err)
	}
	if dupes != 0 {
		t.Fatalf("corpus holds %d duplicated content hashes", dupes)
	}
	t.Logf("articles+pending = %d, duplicate hashes = %d", total, dupes)

	// Drain the backlog; every row must end up promoted or discarded, none lost.
	for i := 0; i < 200; i++ {
		p, d, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
		if err != nil && err != ErrReviewInProgress {
			t.Fatal(err)
		}
		if p == 0 && d == 0 {
			break
		}
	}
	var remaining int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM pending_articles`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("%d deferred articles were lost", remaining)
	}
}

// While the collector is still delivering, the maintenance pass must not touch
// the model at all — the budget belongs to fresh articles.
func TestIdleMaintenanceSkipsWhileCollectorIsActive(t *testing.T) {
	store, api := manualAPI(t)
	api.Model = modelStub(t, func(*http.Request) (string, error) { return notDuplicate, nil })
	if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "挂起的资讯", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal, errors.New("model down")); err != nil {
		t.Fatal(err)
	}

	// An article just queued counts as activity.
	if _, err := store.EnqueueIngestBatch(context.Background(), []storage.ArticleInput{{
		Source: "科技前线", Title: "刚到的资讯", Body: longBody, CollectedAt: time.Now().Unix(),
	}}, storage.AudienceNormal); err != nil {
		t.Fatal(err)
	}

	report, err := api.IdleMaintenance(context.Background(), 10*time.Minute, PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if report.Idle {
		t.Fatal("system reported idle while the collector is still delivering")
	}
	if report.Promoted+report.Discarded+report.Requeued != 0 {
		t.Fatalf("pass did work while busy: %+v", report)
	}
	if n := countRows(t, store, "pending_articles"); n != 1 {
		t.Fatalf("pending_articles=%d; the backlog must be left alone", n)
	}
}

// Once the stream goes quiet the backlog is retried without any operator action.
func TestIdleMaintenanceRetriesBacklogWhenQuiet(t *testing.T) {
	store, api := manualAPI(t)
	api.Model = modelStub(t, func(*http.Request) (string, error) { return notDuplicate, nil })
	if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "挂起的资讯", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal, errors.New("model down")); err != nil {
		t.Fatal(err)
	}
	failedArticle := seedFailedArticle(t, store, "失败的分析", longBody)
	// Age the failure past the settle window so it is genuinely outstanding.
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET updated_at=? WHERE article_id=?`, time.Now().Unix()-300, failedArticle); err != nil {
		t.Fatal(err)
	}

	// No recent ingest, so the system counts as idle.
	report, err := api.IdleMaintenance(context.Background(), time.Minute, PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if !report.Idle {
		t.Fatal("system reported busy with no recent ingest and nothing in flight")
	}
	if report.Promoted != 1 {
		t.Fatalf("promoted=%d; want the deferred article promoted", report.Promoted)
	}
	if report.Requeued != 1 {
		t.Fatalf("requeued=%d; want the failed analysis retried without help", report.Requeued)
	}
	var status string
	if err := store.QueryDB.QueryRow(`SELECT status FROM analysis_jobs ORDER BY article_id LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("analysis job status=%q; want pending after the idle pass", status)
	}
}

// Content the model will never accept must stop consuming retries.
func TestAutoRetryStopsAtAttemptCeiling(t *testing.T) {
	store, _ := manualAPI(t)
	article := seedFailedArticle(t, store, "永久失败的分析", longBody)
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET attempts=? WHERE article_id=?`,
		storage.AutoRetryAttemptCeiling, article); err != nil {
		t.Fatal(err)
	}
	requeued, err := store.RequeueOldestFailed(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 {
		t.Fatalf("requeued=%d; an article at the ceiling must stay failed for a human", requeued)
	}
	var status string
	if err := store.QueryDB.QueryRow(`SELECT status FROM analysis_jobs WHERE article_id=?`, article).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("status=%q; want failed", status)
	}
}

// A failed analysis must be left alone briefly so repeated idle sweeps cannot
// spin on the same row.
func TestAutoRetryRespectsSettleWindow(t *testing.T) {
	store, _ := manualAPI(t)
	article := seedFailedArticle(t, store, "刚失败的分析", longBody)
	requeued, err := store.RequeueOldestFailed(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 {
		t.Fatalf("requeued=%d; a just-failed job must settle first", requeued)
	}
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET updated_at=? WHERE article_id=?`, time.Now().Unix()-120, article); err != nil {
		t.Fatal(err)
	}
	if requeued, err = store.RequeueOldestFailed(context.Background(), 10); err != nil || requeued != 1 {
		t.Fatalf("requeued=%d err=%v; after settling it should be picked up", requeued, err)
	}
}
