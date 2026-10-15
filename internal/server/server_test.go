package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/storage"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

// openTestStore returns an in-memory store closed when the test ends.
func openTestStore(t *testing.T) *storage.Store {
	t.Helper()
	store, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.DB.Close() })
	return store
}

// modelStub builds a model client whose replies the caller controls, so a test
// only has to state the behaviour it actually depends on.
func modelStub(t *testing.T, reply func(r *http.Request) (string, error)) *analyzer.Client {
	t.Helper()
	model := analyzer.New("http://model.invalid/v1", "test-key", "test-model", nil, 2000, 30000, 120, 5)
	model.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		content, err := reply(r)
		if err != nil {
			return nil, err
		}
		body := fmt.Sprintf(`{"choices":[{"message":{"content":%q}}]}`, content)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader([]byte(body))),
			Request:    r,
		}, nil
	})}
	return model
}

// stubDecision is the common case: always answer with one decision.
func stubDecision(t *testing.T, decision string) *analyzer.Client {
	t.Helper()
	return modelStub(t, func(*http.Request) (string, error) { return decision, nil })
}

const notDuplicate = `{"duplicate":false,"matched_id":0}`

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestIngestRejectsBadItemsIndividually(t *testing.T) {
	store := openTestStore(t)
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), nil)
	body := `{"version":1,"articles":[{"source":"示例","title":"有效资讯","body":"正文","collected_at":100},{"source":"示例","body":"缺少标题","collected_at":101},{"source":7,"title":"类型错误","body":"正文","collected_at":102},{"source":"示例","title":"毫秒时间","body":"正文","collected_at":1791798120000}]}`
	req := httptest.NewRequest(http.MethodPost, "/ingest/normal", bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	api.CollectorHandler().ServeHTTP(response, req.WithContext(context.Background()))

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got struct {
		Accepted   int `json:"accepted"`
		Queued     int `json:"queued"`
		Duplicates int `json:"duplicates"`
		Rejected   int `json:"rejected"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Accepted != 1 || got.Queued != 1 || got.Duplicates != 0 || got.Rejected != 3 {
		t.Fatalf("counts=%+v; want accepted=1 queued=1 duplicates=0 rejected=3", got)
	}
	var queued int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM ingest_queue WHERE status='pending'`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("pending queue jobs=%d; want 1", queued)
	}

	// A client retry after a lost ACK must reuse the existing durable queue row.
	retry := httptest.NewRequest(http.MethodPost, "/ingest/normal", bytes.NewBufferString(body))
	retryResponse := httptest.NewRecorder()
	api.CollectorHandler().ServeHTTP(retryResponse, retry)
	if retryResponse.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", retryResponse.Code, retryResponse.Body.String())
	}
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM ingest_queue WHERE status='pending'`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("pending queue jobs after retry=%d; want one idempotent job", queued)
	}
}

func TestIngestUsesModelToConfirmCodeSimilarity(t *testing.T) {
	store := openTestStore(t)

	first, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "城市日报", Title: "市政府公布城市更新计划",
		Body:        "市政府今日公布城市更新计划，将改造老旧社区并新增公共服务设施。项目将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "accepted" {
		t.Fatalf("first article status=%q", first.Status)
	}

	calls := 0
	model := modelStub(t, func(*http.Request) (string, error) {
		calls++
		if calls == 1 {
			return fmt.Sprintf(`{"duplicate":true,"matched_id":%d}`, first.ArticleID), nil
		}
		return notDuplicate, nil
	})
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), model)
	type ingestCounts struct {
		Accepted   int `json:"accepted"`
		Queued     int `json:"queued"`
		Duplicates int `json:"duplicates"`
	}

	post := func(title string) ingestCounts {
		t.Helper()
		body := fmt.Sprintf(`{"version":1,"articles":[{"source":"本地新闻","title":%q,"body":%q,"collected_at":%d}]}`,
			title,
			"市政府今日公布城市更新计划，改造老旧社区并新增公共服务设施。有关部门称将分阶段推进。",
			time.Now().Unix(),
		)
		req := httptest.NewRequest(http.MethodPost, "/ingest/normal", bytes.NewBufferString(body))
		response := httptest.NewRecorder()
		api.CollectorHandler().ServeHTTP(response, req)
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		var got ingestCounts
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	processNext := func() storage.IngestResult {
		t.Helper()
		job, ok, err := store.ClaimIngest(context.Background())
		if err != nil || !ok {
			t.Fatalf("claim ingestion job: job=%+v ok=%v err=%v", job, ok, err)
		}
		result, err := api.ProcessQueuedArticle(context.Background(), job.Article, job.Audience)
		if err != nil {
			t.Fatalf("process queued article: %v", err)
		}
		if err := store.CompleteIngest(context.Background(), job); err != nil {
			t.Fatalf("complete ingestion job: %v", err)
		}
		return result
	}

	duplicate := post("老旧社区更新项目公布")
	if duplicate.Accepted != 1 || duplicate.Queued != 1 || duplicate.Duplicates != 0 || calls != 0 {
		t.Fatalf("duplicate candidate should be acknowledged before async review: counts=%v model_calls=%d", duplicate, calls)
	}
	if result := processNext(); result.Status != "duplicate" || result.ArticleID != first.ArticleID {
		t.Fatalf("duplicate review result=%+v; want existing article %d", result, first.ArticleID)
	}
	nonDuplicate := post("另一个城市更新项目的新阶段")
	if nonDuplicate.Accepted != 1 || nonDuplicate.Queued != 1 || nonDuplicate.Duplicates != 0 {
		t.Fatalf("non-duplicate decision counts=%v", nonDuplicate)
	}
	if result := processNext(); result.Status != "accepted" {
		t.Fatalf("non-duplicate review result=%+v; want accepted", result)
	}
	if calls != 2 {
		t.Fatalf("model review calls=%d; want 2", calls)
	}
	var count int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM articles`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("stored article count=%d; want existing plus model-approved new article", count)
	}
}

// An unreachable model must not reject or strand a submission: the article is
// parked outside the corpus and promoted once review can run again.
func TestUnreachableModelDefersArticleAndReviewPromotesIt(t *testing.T) {
	store := openTestStore(t)

	seed, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "城市日报", Title: "市政府公布城市更新计划",
		Body:        "市政府今日公布城市更新计划，将改造老旧社区并新增公共服务设施。项目将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil || seed.Status != "accepted" {
		t.Fatalf("seed article: %+v err=%v", seed, err)
	}

	reachable := false
	model := modelStub(t, func(*http.Request) (string, error) {
		if !reachable {
			return "", errors.New("model endpoint is down")
		}
		return notDuplicate, nil
	})
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), model)

	article := storage.ArticleInput{
		Source: "本地新闻", Title: "老旧社区更新项目公布",
		Body:        "市政府今日公布城市更新计划，改造老旧社区并新增公共服务设施。有关部门称将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}

	// The model is down: the article is deferred, not rejected and not lost.
	result, err := api.ProcessQueuedArticle(context.Background(), article, storage.AudienceNormal)
	if err != nil {
		t.Fatalf("a model outage must not fail the submission: %v", err)
	}
	if result.Status != "deferred" {
		t.Fatalf("result=%+v; want deferred", result)
	}
	if n := countRows(t, store, "articles"); n != 1 {
		t.Fatalf("articles=%d; want the seed only, deferred article must stay out of the corpus", n)
	}
	if n := countRows(t, store, "pending_articles"); n != 1 {
		t.Fatalf("pending_articles=%d; want 1", n)
	}

	// A review pass while the model is still down reports the failure without
	// consuming the article.
	if _, _, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize); err == nil {
		t.Fatal("expected the review pass to report the model failure")
	}
	if n := countRows(t, store, "pending_articles"); n != 1 {
		t.Fatalf("pending_articles=%d after a failed pass; the article must be kept", n)
	}

	// Once the model recovers, the deferred article enters the corpus with its
	// analysis job and leaves the pending table.
	reachable = true
	promoted, discarded, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("review pass: %v", err)
	}
	if promoted != 1 || discarded != 0 {
		t.Fatalf("promoted=%d discarded=%d; want 1/0", promoted, discarded)
	}
	if n := countRows(t, store, "pending_articles"); n != 0 {
		t.Fatalf("pending_articles=%d after promotion; want 0", n)
	}
	if n := countRows(t, store, "articles"); n != 2 {
		t.Fatalf("articles=%d; want 2 after promotion", n)
	}
	if n := countRows(t, store, "analysis_jobs"); n != 2 {
		t.Fatalf("analysis_jobs=%d; want both articles analysed", n)
	}
}

// Review must still drop a deferred article that turns out to be a duplicate.
func TestPendingReviewDiscardsConfirmedDuplicate(t *testing.T) {
	store := openTestStore(t)

	seed, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "城市日报", Title: "市政府公布城市更新计划",
		Body:        "市政府今日公布城市更新计划，将改造老旧社区并新增公共服务设施。项目将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil || seed.Status != "accepted" {
		t.Fatalf("seed article: %+v err=%v", seed, err)
	}

	reachable := false
	model := modelStub(t, func(*http.Request) (string, error) {
		if !reachable {
			return "", errors.New("model endpoint is down")
		}
		return fmt.Sprintf(`{"duplicate":true,"matched_id":%d}`, seed.ArticleID), nil
	})
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), model)

	if _, err := api.ProcessQueuedArticle(context.Background(), storage.ArticleInput{
		Source: "本地新闻", Title: "老旧社区更新项目公布",
		Body:        "市政府今日公布城市更新计划，改造老旧社区并新增公共服务设施。有关部门称将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal); err != nil {
		t.Fatalf("defer: %v", err)
	}

	reachable = true
	promoted, discarded, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("review pass: %v", err)
	}
	if promoted != 0 || discarded != 1 {
		t.Fatalf("promoted=%d discarded=%d; want 0/1", promoted, discarded)
	}
	if n := countRows(t, store, "articles"); n != 1 {
		t.Fatalf("articles=%d; the corpus must keep only the original", n)
	}
}

func countRows(t *testing.T, store *storage.Store, table string) int {
	t.Helper()
	var n int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func manualAPI(t *testing.T) (*storage.Store, *API) {
	t.Helper()
	store := openTestStore(t)
	cfg := config.Config{
		Categories:      []config.Category{{Name: "科技", Description: "技术与产品"}, {Name: "财经", Description: "经济与市场"}},
		MaxSummaryChars: 120,
		MaxQueryCount:   100,
	}
	return store, New(store, cfg, log.New(io.Discard, "", 0), nil)
}

// seedFailedArticle stores an article whose analysis job sits in the failed
// state, which is exactly what the manual endpoints exist for.
func seedFailedArticle(t *testing.T, store *storage.Store, title, body string) int64 {
	t.Helper()
	result, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: title, Body: body, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil || result.Status != "accepted" {
		t.Fatalf("seed article: %+v err=%v", result, err)
	}
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET status='failed',attempts=5,last_error='model API returned 422' WHERE article_id=?`, result.ArticleID); err != nil {
		t.Fatal(err)
	}
	return result.ArticleID
}

const longBody = "某公司今日发布新款处理器，采用新一代工艺，产品将在下季度上市首发，官方尚未公布具体售价区间与合作伙伴名单。"

// overLimitBody exceeds the 120-character summary limit, so a summary has to be
// supplied rather than taken verbatim from the body.
var overLimitBody = strings.Repeat(longBody, 3)

func TestManualAnalysisSourceExposesBodyAndConstraints(t *testing.T) {
	store, api := manualAPI(t)
	id := seedFailedArticle(t, store, "某公司发布新款处理器", longBody)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/analysis/%d", id), nil)
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got struct {
		Body           string `json:"body"`
		Status         string `json:"status"`
		LastError      string `json:"last_error"`
		Writable       bool   `json:"writable"`
		SummaryMaxChar int    `json:"summary_max_chars"`
		Categories     []struct {
			Name string `json:"name"`
		} `json:"categories"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Body != longBody {
		t.Fatalf("body was not returned in full: %q", got.Body)
	}
	if got.Status != "failed" || got.LastError == "" || !got.Writable {
		t.Fatalf("state=%+v; want a writable failed job", got)
	}
	if got.SummaryMaxChar != 120 || len(got.Categories) != 2 {
		t.Fatalf("constraints=%+v; want the summary limit and configured categories", got)
	}
}

func TestManualAnalysisCompletesAFailedArticle(t *testing.T) {
	store, api := manualAPI(t)
	id := seedFailedArticle(t, store, "某公司发布新款处理器", longBody)

	req := httptest.NewRequest(http.MethodPost, "/api/analysis/manual", bytes.NewBufferString(fmt.Sprintf(
		`{"article_id":%d,"category":"科技","keywords":["处理器","新一代工艺"],"summary":"某公司发布采用新一代工艺的处理器，下季度上市。","importance":3}`, id)))
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	var status string
	if err := store.DB.QueryRow(`SELECT status FROM analysis_jobs WHERE article_id=?`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("job status=%q; want completed", status)
	}
	var category, summary, model, keywords string
	var importance int
	if err := store.DB.QueryRow(`SELECT category,keywords_json,summary,importance,model FROM analyses WHERE article_id=?`, id).
		Scan(&category, &keywords, &summary, &importance, &model); err != nil {
		t.Fatal(err)
	}
	if category != "科技" || summary == "" || importance != 3 {
		t.Fatalf("stored analysis=%q/%q/%d", category, summary, importance)
	}
	if model != "manual" {
		t.Fatalf("model=%q; want the row marked as manual", model)
	}
	var lastError sql.NullString
	if err := store.DB.QueryRow(`SELECT last_error FROM analysis_jobs WHERE article_id=?`, id).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if lastError.Valid {
		t.Fatalf("last_error=%q; want it cleared", lastError.String)
	}

	// The article now surfaces through the query API like any other.
	items, err := store.List(context.Background(), "all", storage.AudienceNormal, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != id || items[0].Category != "科技" {
		t.Fatalf("query items=%+v; want the manually analysed article", items)
	}
	if len(items[0].Keywords) != 2 {
		t.Fatalf("keywords=%v; want the submitted pair", items[0].Keywords)
	}
}

func TestManualAnalysisRejectsInvalidInput(t *testing.T) {
	store, api := manualAPI(t)
	id := seedFailedArticle(t, store, "某公司发布新款处理器", longBody)

	// A body past the summary limit forces the operator to supply a summary.
	overLimitID := seedFailedArticle(t, store, "超长正文资讯", overLimitBody)
	cases := []struct {
		name    string
		payload string
	}{
		{"unknown category", `{"article_id":%d,"category":"体育","keywords":["a","b"],"summary":"x","importance":3}`},
		{"category all", `{"article_id":%d,"category":"all","keywords":["a","b"],"summary":"x","importance":3}`},
		{"one keyword", `{"article_id":%d,"category":"科技","keywords":["a"],"summary":"x","importance":3}`},
		{"seven keywords", `{"article_id":%d,"category":"科技","keywords":["a","b","c","d","e","f","g"],"summary":"x","importance":3}`},
		{"blank keyword", `{"article_id":%d,"category":"科技","keywords":["a","  "],"summary":"x","importance":3}`},
		{"keyword too long", `{"article_id":%d,"category":"科技","keywords":["a","` + strings.Repeat("词", 41) + `"],"summary":"x","importance":3}`},
		{"importance too high", `{"article_id":%d,"category":"科技","keywords":["a","b"],"summary":"x","importance":6}`},
		{"importance too low", `{"article_id":%d,"category":"科技","keywords":["a","b"],"summary":"x","importance":0}`},
		{"summary too long", `{"article_id":%d,"category":"科技","keywords":["a","b"],"summary":"` + strings.Repeat("长", 121) + `","importance":3}`},
		{"missing summary for an over-limit body", `{"article_id":%d,"category":"科技","keywords":["a","b"],"summary":"","importance":3}`},
		{"unknown article", `{"article_id":999999,"category":"科技","keywords":["a","b"],"summary":"x","importance":3}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := id
			if tc.name == "missing summary for an over-limit body" {
				target = overLimitID
			}
			req := httptest.NewRequest(http.MethodPost, "/api/analysis/manual", bytes.NewBufferString(fmt.Sprintf(tc.payload, target)))
			response := httptest.NewRecorder()
			api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s; want 400", response.Code, response.Body.String())
			}
		})
	}

	if n := countRows(t, store, "analyses"); n != 0 {
		t.Fatalf("analyses=%d; a rejected submission must not write", n)
	}
}

// A short body is summarised verbatim, matching what the model path stores.
func TestManualAnalysisUsesShortBodyAsSummary(t *testing.T) {
	store, api := manualAPI(t)
	body := "很短的一条资讯正文。"
	id := seedFailedArticle(t, store, "短讯", body)

	req := httptest.NewRequest(http.MethodPost, "/api/analysis/manual", bytes.NewBufferString(fmt.Sprintf(
		`{"article_id":%d,"category":"科技","keywords":["短讯","测试"],"summary":"会被正文覆盖","importance":1}`, id)))
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var summary string
	if err := store.DB.QueryRow(`SELECT summary FROM analyses WHERE article_id=?`, id).Scan(&summary); err != nil {
		t.Fatal(err)
	}
	if summary != body {
		t.Fatalf("summary=%q; want the body verbatim", summary)
	}
}

// A job a worker still owns must not be overwritten underneath it.
func TestManualAnalysisRejectsInProgressJob(t *testing.T) {
	store, api := manualAPI(t)
	id := seedFailedArticle(t, store, "某公司发布新款处理器", longBody)
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET status='processing' WHERE article_id=?`, id); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/analysis/manual", bytes.NewBufferString(fmt.Sprintf(
		`{"article_id":%d,"category":"科技","keywords":["a","b"],"summary":"x","importance":3}`, id)))
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s; want 409", response.Code, response.Body.String())
	}
	if n := countRows(t, store, "analyses"); n != 0 {
		t.Fatalf("analyses=%d; the job must be left untouched", n)
	}
}

// The new wildcard route must not shadow the existing failed-job listing.
func TestAdminRoutesDoNotConflict(t *testing.T) {
	store, api := manualAPI(t)
	seedFailedArticle(t, store, "某公司发布新款处理器", longBody)

	for _, path := range []string{"/api/analysis/failed?limit=10", "/api/analysis/1", "/api/dedup/pending"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	var listing struct {
		Items []struct {
			ArticleID int64 `json:"article_id"`
		} `json:"items"`
	}
	req := httptest.NewRequest(http.MethodGet, "/api/analysis/failed?limit=10", nil)
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if err := json.Unmarshal(response.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Items) != 1 || listing.Items[0].ArticleID != 1 {
		t.Fatalf("failed listing=%+v; the literal route must still match", listing)
	}
}

func TestPanelAndStatus(t *testing.T) {
	store, api := manualAPI(t)
	id := seedFailedArticle(t, store, "某公司发布新款处理器", longBody)

	t.Run("panel is served at the admin root", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		response := httptest.NewRecorder()
		api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d", response.Code)
		}
		if got := response.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
			t.Fatalf("content type=%q", got)
		}
		for header, want := range map[string]string{
			"X-Frame-Options":        "DENY",
			"X-Content-Type-Options": "nosniff",
			"Cache-Control":          "no-store",
		} {
			if got := response.Header().Get(header); got != want {
				t.Errorf("%s=%q; want %q", header, got, want)
			}
		}
		if csp := response.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
			t.Errorf("CSP=%q; want a deny-by-default policy", csp)
		}
		body := response.Body.String()
		for _, marker := range []string{"<title>资讯管理 · NewsScope</title>", "api/status", "api/analysis/manual", "<dialog"} {
			if !strings.Contains(body, marker) {
				t.Errorf("panel is missing %q", marker)
			}
		}
		// The console must never hard-code a model endpoint or key.
		for _, secret := range []string{"OPENAI_API_KEY", "api.example.com", "sk-"} {
			if strings.Contains(body, secret) {
				t.Errorf("panel leaks %q", secret)
			}
		}
	})

	t.Run("unknown admin paths are not the panel", func(t *testing.T) {
		for _, path := range []string{"/index.html", "/api/unknown"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
			if response.Code == http.StatusOK {
				t.Errorf("%s returned 200; unknown paths must not serve the console", path)
			}
		}
	})

	t.Run("retired admin paths are gone", func(t *testing.T) {
		for _, path := range []string{"/hide", "/analysis/failed", "/analysis/1", "/dedup/pending"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
			if response.Code == http.StatusOK {
				t.Errorf("%s still responds; the old paths must be removed", path)
			}
		}
	})

	t.Run("status aggregates both audiences", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
		response := httptest.NewRecorder()
		api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		var got struct {
			Articles        int64 `json:"articles"`
			AnalysisFailed  int64 `json:"analysis_failed"`
			DedupPending    int64 `json:"dedup_pending"`
			SummaryMaxChars int   `json:"summary_max_chars"`
			Audiences       []struct {
				Audience   string           `json:"audience"`
				Articles   int64            `json:"articles"`
				Categories map[string]int64 `json:"categories"`
			} `json:"audiences"`
			Categories []struct {
				Name string `json:"name"`
			} `json:"categories"`
			IngestQueue struct {
				Pending int64 `json:"pending"`
			} `json:"ingest_queue"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.SummaryMaxChars != 120 {
			t.Errorf("summary_max_chars=%d; the console needs the limit to validate", got.SummaryMaxChars)
		}
		if len(got.Audiences) != 2 {
			t.Fatalf("audiences=%d; want both normal and vip even when empty", len(got.Audiences))
		}
		if got.Audiences[0].Audience != storage.AudienceNormal || got.Audiences[1].Audience != storage.AudienceVIP {
			t.Errorf("audience order=%v", got.Audiences)
		}
		if got.Articles != 1 || got.AnalysisFailed != 1 {
			t.Errorf("articles=%d failed=%d; want 1/1", got.Articles, got.AnalysisFailed)
		}
		if len(got.Categories) != 2 {
			t.Errorf("categories=%d; the console needs the configured list", len(got.Categories))
		}
		if id <= 0 {
			t.Fatal("unreachable")
		}
	})
}

// A busy corpus must not make one submission spin forever. Invalidating the
// window from inside the review makes every verdict stale, so the bounded retry
// has to defer rather than loop.
func TestConcurrentInsertsDeferInsteadOfSpinning(t *testing.T) {
	store, _ := manualAPI(t)
	similar := "某公司今日发布新款处理器，采用新一代工艺，产品将在下季度上市首发，官方尚未公布具体售价区间与合作伙伴名单。"
	if _, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "某公司发布新款处理器", Body: similar, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal); err != nil {
		t.Fatal(err)
	}

	api := New(store, config.Config{Categories: []config.Category{{Name: "科技", Description: "d"}}, MaxSummaryChars: 120},
		log.New(io.Discard, "", 0), nil)
	calls := 0
	model := modelStub(t, func(*http.Request) (string, error) {
		calls++
		// A concurrent writer lands right after the window was observed, so the
		// verdict below is stale by the time it is committed.
		api.normalCommit.Lock()
		api.normalRev++
		api.normalCommit.Unlock()
		return notDuplicate, nil
	})
	api.Model = model

	result, err := api.ProcessQueuedArticle(context.Background(), storage.ArticleInput{
		Source: "本地新闻", Title: "老旧社区更新项目公布", Body: similar, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if result.Status != "deferred" {
		t.Fatalf("result=%+v; want deferred once the retries are exhausted", result)
	}
	if calls != maxDedupRetries+1 {
		t.Fatalf("model called %d times; want %d, one per bounded attempt", calls, maxDedupRetries+1)
	}
	if n := countRows(t, store, "articles"); n != 1 {
		t.Fatalf("articles=%d; the corpus must stay clean", n)
	}
	if n := countRows(t, store, "pending_articles"); n != 1 {
		t.Fatalf("pending_articles=%d; the article must be kept", n)
	}
}

// Promotion can still discover an exact duplicate that appeared during review;
// the counters must not claim it was promoted.
func TestPendingReviewCountsPromotionThatTurnsDuplicate(t *testing.T) {
	store, api := manualAPI(t)
	incoming := storage.ArticleInput{
		Source: "本地新闻", Title: "老旧社区更新项目公布", Body: longBody, CollectedAt: time.Now().Unix(),
	}
	// A similar-but-not-identical article makes review call the model.
	if _, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "本地新闻", Title: "老旧社区更新项目的另一阶段", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeferDedup(context.Background(), incoming, storage.AudienceNormal, errors.New("model down")); err != nil {
		t.Fatal(err)
	}
	// While the model answers, an exact copy of the deferred article lands, so
	// promotion finds a duplicate even though the verdict said otherwise.
	inserted := false
	model := modelStub(t, func(*http.Request) (string, error) {
		if !inserted {
			inserted = true
			if _, err := store.Ingest(context.Background(), incoming, storage.AudienceNormal); err != nil {
				t.Errorf("seed exact copy: %v", err)
			}
		}
		return notDuplicate, nil
	})
	api.Model = model

	promoted, discarded, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("review: %v", err)
	}
	if !inserted {
		t.Fatal("the model was never consulted; the fixture did not reach the promote path")
	}
	if promoted != 0 || discarded != 1 {
		t.Fatalf("promoted=%d discarded=%d; want 0/1 because the store found a duplicate", promoted, discarded)
	}
	if n := countRows(t, store, "pending_articles"); n != 0 {
		t.Fatalf("pending_articles=%d; want 0", n)
	}
	if n := countRows(t, store, "articles"); n != 2 {
		t.Fatalf("articles=%d; want the original plus the exact copy", n)
	}
}

// The console listing must not carry article bodies: it refreshes on a timer.
func TestPendingListingOmitsBodies(t *testing.T) {
	store, api := manualAPI(t)
	if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "挂起的资讯", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal, errors.New("model down")); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/dedup/pending", nil)
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), longBody) {
		t.Fatal("the listing leaked an article body")
	}
	for _, field := range []string{"pending_id", "title", "source", "audience", "attempts", "last_error", "created_at"} {
		if !strings.Contains(response.Body.String(), `"`+field+`"`) {
			t.Errorf("listing is missing %q", field)
		}
	}
}

// Promotion writes into the corpus, so it must invalidate in-flight duplicate
// reviews exactly like ingestion does. Otherwise a review that started before
// the promotion commits against a stale candidate window and lets a near
// duplicate through.
func TestPromotionBumpsAudienceRevision(t *testing.T) {
	store, api := manualAPI(t)
	if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "某公司发布新款处理器", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal, errors.New("model down")); err != nil {
		t.Fatal(err)
	}

	before := api.normalRev
	promoted, _, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize)
	if err != nil {
		t.Fatalf("review: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("promoted=%d; want 1", promoted)
	}
	if api.normalRev == before {
		t.Fatal("promotion did not bump the revision; in-flight reviews would miss it")
	}

	// The VIP audience must stay untouched by a normal-audience promotion.
	vipBefore := api.vipRev
	if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
		Source: "财经观察", Title: "央行公布报告", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceVIP, errors.New("model down")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize); err != nil {
		t.Fatalf("vip review: %v", err)
	}
	if api.vipRev == vipBefore {
		t.Fatal("a normal-audience promotion bumped the vip revision")
	}
}

// The sweeper and the console button can both ask for a pass; the second must
// be turned away rather than reviewing the same rows twice.
func TestConcurrentReviewPassesDoNotOverlap(t *testing.T) {
	store, api := manualAPI(t)
	// The deferred rows share a body, so review reaches the model; a stub keeps
	// this test focused on the mutual exclusion.
	model := stubDecision(t, notDuplicate)
	api.Model = model

	for i := 0; i < 3; i++ {
		if _, err := store.DeferDedup(context.Background(), storage.ArticleInput{
			Source: "科技前线", Title: fmt.Sprintf("挂起资讯 %d", i), Body: longBody,
			CollectedAt: time.Now().Unix(),
		}, storage.AudienceNormal, errors.New("model down")); err != nil {
			t.Fatal(err)
		}
	}

	release := make(chan struct{})
	started := make(chan struct{})
	released := make(chan struct{})
	go func() {
		// Hold the pass open so a second caller is turned away.
		api.dedupRun.Lock()
		close(started)
		<-release
		api.dedupRun.Unlock()
		close(released)
	}()
	<-started

	if _, _, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize); !errors.Is(err, ErrReviewInProgress) {
		t.Fatalf("second pass error=%v; want ErrReviewInProgress", err)
	}
	close(release)
	<-released

	// Once the first pass finishes, a new one is admitted again.
	if _, _, err := api.ReviewPendingDedup(context.Background(), PendingDedupBatchSize); err != nil {
		t.Fatalf("pass after release: %v", err)
	}
}

func TestReviewEndpointReportsBusyAsConflict(t *testing.T) {
	_, api := manualAPI(t)
	api.dedupRun.Lock()
	req := httptest.NewRequest(http.MethodPost, "/api/dedup/review", nil)
	response := httptest.NewRecorder()
	api.AdminHandler().ServeHTTP(response, req.WithContext(context.Background()))
	api.dedupRun.Unlock()
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s; want 409", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "review_in_progress") {
		t.Fatalf("body=%s; want the busy error code", response.Body.String())
	}
}

// A collector author has no other signal, so each distinct mistake must come
// back named rather than as one opaque rejection.
func TestIngestRejectionNamesTheOffendingField(t *testing.T) {
	_, a := manualAPI(t)
	cases := []struct {
		name    string
		payload string
		reason  string
	}{
		{"missing source", `{"source":"","title":"标题","body":"正文","collected_at":1791798120}`, storage.RejectMissingSource},
		{"missing title", `{"source":"来源","title":"  ","body":"正文","collected_at":1791798120}`, storage.RejectMissingTitle},
		{"missing body", `{"source":"来源","title":"标题","body":"  ","collected_at":1791798120}`, storage.RejectMissingBody},
		{"missing time", `{"source":"来源","title":"标题","body":"正文","collected_at":0}`, storage.RejectMissingTime},
		{"millisecond time", `{"source":"来源","title":"标题","body":"正文","collected_at":1791798120000}`, storage.RejectTimeNotSecond},
		{"absurd time", `{"source":"来源","title":"标题","body":"正文","collected_at":999999999999999}`, storage.RejectTimeRange},
		{"misspelled field", `{"source":"来源","title":"标题","body":"正文","collected_at":1791798120,"auther":"笔误"}`, storage.UnknownFieldReason("auther")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/ingest/normal",
				strings.NewReader(`{"version":1,"articles":[`+tc.payload+`]}`))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			a.CollectorHandler().ServeHTTP(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var body struct {
				Results []storage.IngestResult `json:"results"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Results) != 1 || body.Results[0].Status != "rejected" {
				t.Fatalf("results=%+v; want one rejection", body.Results)
			}
			if body.Results[0].Reason != tc.reason {
				t.Errorf("reason=%q; want %q", body.Results[0].Reason, tc.reason)
			}
		})
	}
}

// Different mistakes must never collapse into the same answer.
func TestIngestRejectionReasonsAreDistinct(t *testing.T) {
	_, api := manualAPI(t)
	payloads := []string{
		`{"source":"","title":"标题","body":"正文","collected_at":1791798120}`,
		`{"source":"来源","title":"","body":"正文","collected_at":1791798120}`,
		`{"source":"来源","title":"标题","body":"","collected_at":1791798120}`,
		`{"source":"来源","title":"标题","body":"正文","collected_at":0}`,
		`{"source":"来源","title":"标题","body":"正文","collected_at":1791798120000}`,
		`{"source":"来源","title":"标题","body":"正文","collected_at":1791798120,"typo":1}`,
	}
	seen := map[string]string{}
	for _, payload := range payloads {
		req := httptest.NewRequest(http.MethodPost, "/ingest/normal",
			strings.NewReader(`{"version":1,"articles":[`+payload+`]}`))
		response := httptest.NewRecorder()
		api.CollectorHandler().ServeHTTP(response, req)
		var body struct {
			Results []storage.IngestResult `json:"results"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		reason := body.Results[0].Reason
		if reason == "" || reason == "invalid_article" {
			t.Fatalf("payload %s produced an unhelpful reason %q", payload, reason)
		}
		if other, clash := seen[reason]; clash {
			t.Fatalf("reason %q is shared by %s and %s", reason, other, payload)
		}
		seen[reason] = payload
	}
	if len(seen) != len(payloads) {
		t.Fatalf("got %d distinct reasons for %d mistakes", len(seen), len(payloads))
	}
}

// A rejected query must say which parameter was wrong and what was available.
func TestQueryRejectionNamesTheProblem(t *testing.T) {
	store, api := manualAPI(t)
	if _, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "某公司发布新款处理器", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path   string
		reason string
	}{
		{"/news?category=量子&start=0&end=1", "unknown_category"},
		{"/news?category=科技&start=x&end=1", "start_and_end_must_be_integers"},
		{"/news?category=科技&start=-1&end=1", "start_must_not_be_negative"},
		{"/news?category=科技&start=5&end=1", "end_must_be_greater_than_start"},
		{"/news?category=科技&start=0&end=100000", "range_too_large"},
		{"/news?category=科技&start=0&end=99", "end_beyond_max_position"},
		{"/news/range?category=科技&from=5&to=1", "to_must_be_greater_than_from"},
		{"/news/range?category=量子&from=1&to=2", "unknown_category"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		response := httptest.NewRecorder()
		api.QueryHandler(storage.AudienceNormal).ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d body=%s; want 400", tc.path, response.Code, response.Body.String())
			continue
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["reason"] != tc.reason {
			t.Errorf("%s: reason=%v; want %q", tc.path, body["reason"], tc.reason)
		}
		if tc.reason == "unknown_category" {
			available, ok := body["available"].([]any)
			if !ok || len(available) == 0 {
				t.Errorf("%s: unknown_category must list the valid categories", tc.path)
			}
		}
	}
}

// A mistyped path must not answer 200 with unrelated service data.
func TestQueryListenerRejectsUnknownPaths(t *testing.T) {
	_, api := manualAPI(t)
	handler := api.QueryHandler(storage.AudienceNormal)
	for _, path := range []string{"/nonsense", "/news/bogus/extra", "/healthz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code == http.StatusOK {
			t.Errorf("%s: status=%d body=%s; want 404", path, response.Code, response.Body.String())
		}
	}
	// The real root must keep working.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("root status=%d; want 200", response.Code)
	}
}

func analysedCard(t *testing.T, store *storage.Store, title, body, category, summary string, keywords ...string) int64 {
	t.Helper()
	result, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: title, Body: body, CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil || result.Status != "accepted" {
		t.Fatalf("seed: %+v err=%v", result, err)
	}
	words, _ := json.Marshal(keywords)
	if _, err := store.DB.Exec(`INSERT INTO analyses(article_id,category,keywords_json,summary,importance,model,prompt_version,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, result.ArticleID, category, string(words), summary, 3, "m", "v2", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	// Search only returns articles whose analysis finished.
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET status='completed' WHERE article_id=?`, result.ArticleID); err != nil {
		t.Fatal(err)
	}
	return result.ArticleID
}

func TestSearchMatchesTitleSummaryAndKeywords(t *testing.T) {
	store, _ := manualAPI(t)
	analysedCard(t, store, "某公司发布新款处理器", longBody, "科技", "采用新一代工艺提升能效。", "处理器")
	analysedCard(t, store, "央行公布季度报告", longBody, "财经", "报告显示信贷平稳增长。", "货币政策")
	analysedCard(t, store, "新能源车企发布新车", longBody, "产业", "新车续航里程提升。", "续航")

	cases := []struct {
		term  string
		want  int
		field string
	}{
		{"处理器", 1, "title"},
		{"新一代工艺", 1, "summary"},
		{"货币政策", 1, "keyword"},
		{"新能源", 1, "title"},
		{"不存在的词", 0, "none"},
		{"50%", 0, "wildcard treated literally"},
	}
	for _, tc := range cases {
		total, items, err := store.Search(context.Background(), "all", storage.AudienceNormal, tc.term, 20, 0)
		if err != nil {
			t.Fatalf("%s: %v", tc.term, err)
		}
		if total != tc.want || len(items) != tc.want {
			t.Errorf("%q (%s): total=%d items=%d; want %d", tc.term, tc.field, total, len(items), tc.want)
		}
	}
}

// A search must never leak across audiences.
func TestSearchRespectsAudienceIsolation(t *testing.T) {
	store, _ := manualAPI(t)
	result, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "科技前线", Title: "机密项目泄密处理", Body: longBody, CollectedAt: time.Now().Unix(),
	}, storage.AudienceVIP)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`INSERT INTO analyses(article_id,category,keywords_json,summary,importance,model,prompt_version,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, result.ArticleID, "科技", `["机密"]`, "仅限特定受众。", 3, "m", "v2", time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`UPDATE analysis_jobs SET status='completed' WHERE article_id=?`, result.ArticleID); err != nil {
		t.Fatal(err)
	}
	vipTotal, _, err := store.Search(context.Background(), "all", storage.AudienceVIP, "机密", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if vipTotal != 1 {
		t.Fatalf("vip total=%d; want 1", vipTotal)
	}
	normalTotal, _, err := store.Search(context.Background(), "all", storage.AudienceNormal, "机密", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if normalTotal != 0 {
		t.Fatalf("normal total=%d; a VIP article leaked across audiences", normalTotal)
	}
}

func TestSearchPaginatesWithBeforeID(t *testing.T) {
	store, _ := manualAPI(t)
	var ids []int64
	for i := 0; i < 5; i++ {
		ids = append(ids, analysedCard(t, store, fmt.Sprintf("处理器发布第 %d 代", i), longBody, "科技", "采用新一代工艺。", "处理器"))
	}
	total, first, err := store.Search(context.Background(), "all", storage.AudienceNormal, "处理器", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(first) != 2 {
		t.Fatalf("total=%d first=%d; want 5/2", total, len(first))
	}
	_, second, err := store.Search(context.Background(), "all", storage.AudienceNormal, "处理器", 2, first[len(first)-1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 {
		t.Fatalf("second page=%d; want 2", len(second))
	}
	seen := map[int64]bool{}
	for _, item := range append(first, second...) {
		if seen[item.ID] {
			t.Fatalf("article %d appeared on both pages", item.ID)
		}
		seen[item.ID] = true
	}
}

func TestSearchEndpointValidatesInput(t *testing.T) {
	store, api := manualAPI(t)
	analysedCard(t, store, "某公司发布新款处理器", longBody, "科技", "采用新一代工艺。", "处理器")
	handler := api.QueryHandler(storage.AudienceNormal)
	cases := []struct {
		path   string
		reason string
	}{
		{"/news/search?category=科技", "q_is_required"},
		{"/news/search?category=科技&q=%20%20", "q_is_required"},
		{"/news/search?category=量子&q=x", "unknown_category"},
		{"/news/search?category=科技&q=x&limit=0", "limit_must_be_between_1_and_200"},
		{"/news/search?category=科技&q=x&limit=abc", "limit_must_be_between_1_and_200"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d body=%s; want 400", tc.path, response.Code, response.Body.String())
			continue
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["reason"] != tc.reason {
			t.Errorf("%s: reason=%v; want %q", tc.path, body["reason"], tc.reason)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/news/search?category=%E7%A7%91%E6%8A%80&q=%E5%A4%84%E7%90%86%E5%99%A8", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("valid search: status=%d body=%s", response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["total"] != float64(1) || body["q"] != "处理器" {
		t.Fatalf("unexpected body: %+v", body)
	}
}

// Readers type several words and expect articles matching all of them, not one
// long literal phrase.
func TestSearchTreatsWordsAsSeparateTerms(t *testing.T) {
	store, _ := manualAPI(t)
	analysedCard(t, store, "芯片出口新规", longBody, "科技", "多家企业调整出口流程。", "芯片")
	analysedCard(t, store, "汽车出口回暖", longBody, "产业", "整车出口环比增长。", "汽车")
	analysedCard(t, store, "芯片产能扩张", longBody, "科技", "新增产线陆续投产。", "产能")

	cases := []struct {
		query string
		want  int
	}{
		{"芯片", 2},
		{"芯片 出口", 1},    // only the first article has both
		{"出口", 2},       // appears in two titles
		{"调整出口流程", 1},   // a phrase inside one summary
		{"芯片  产能", 1},   // runs of whitespace collapse
		{"芯片 产能 出口", 0}, // no article has all three
		{"芯片 汽车", 0},
	}
	for _, tc := range cases {
		total, items, err := store.Search(context.Background(), "all", storage.AudienceNormal, tc.query, 20, 0)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		if total != tc.want || len(items) != tc.want {
			t.Errorf("%q: total=%d items=%d; want %d", tc.query, total, len(items), tc.want)
		}
	}
}

// Word order must not matter, since no reader expects phrase matching.
func TestSearchIgnoresWordOrder(t *testing.T) {
	store, _ := manualAPI(t)
	analysedCard(t, store, "芯片出口新规", longBody, "科技", "多家企业调整出口流程。", "芯片")
	forward, _, err := store.Search(context.Background(), "all", storage.AudienceNormal, "芯片 出口", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	reverse, _, err := store.Search(context.Background(), "all", storage.AudienceNormal, "出口 芯片", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if forward != 1 || reverse != 1 {
		t.Fatalf("forward=%d reverse=%d; word order must not matter", forward, reverse)
	}
}

// Each roster must render itself when its loader completes. Wiring a loader to
// only refresh the counts leaves the list stuck on "加载中…" until the operator
// happens to touch the filter box.
func TestPanelLoadersRenderTheirOwnRoster(t *testing.T) {
	panel := string(panelFile)
	cases := []struct {
		loader string
		render string
	}{
		{"async function loadFailed(", "renderFailedList()"},
		{"async function loadDedup(", "renderDedupList()"},
	}
	for _, tc := range cases {
		start := strings.Index(panel, tc.loader)
		if start < 0 {
			t.Fatalf("panel no longer defines %s", tc.loader)
		}
		body := panel[start:]
		if end := strings.Index(body, "\n  }"); end > 0 {
			body = body[:end]
		}
		if !strings.Contains(body, tc.render) {
			t.Errorf("%s does not call %s; its roster would never render", tc.loader, tc.render)
		}
	}
	// The list elements must have a writer other than their initial markup.
	for _, id := range []string{"failedList", "dedupList"} {
		if strings.Count(panel, `"`+id+`"`) < 2 {
			t.Errorf("#%s is written only once, so it can never update", id)
		}
	}
}

// A page that is exactly full may still be the last one, so has_more must be
// decided by looking at the row after it, not by the page length alone.
func TestSearchHasMoreIsExact(t *testing.T) {
	store, api := manualAPI(t)
	for i := 0; i < 3; i++ {
		analysedCard(t, store, fmt.Sprintf("处理器发布第 %d 代", i), longBody, "科技", "新一代工艺。", "处理器")
	}
	handler := api.QueryHandler(storage.AudienceNormal)

	// Exactly as many matches as the page holds: there is no second page.
	req := httptest.NewRequest(http.MethodGet, "/news/search?category=%E7%A7%91%E6%8A%80&q=%E5%A4%84%E7%90%86%E5%99%A8&limit=3", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["total"] != float64(3) || body["returned"] != float64(3) {
		t.Fatalf("total=%v returned=%v; want 3/3", body["total"], body["returned"])
	}
	if body["has_more"] != false {
		t.Fatalf("has_more=%v; a full last page must not claim another one", body["has_more"])
	}

	// Fewer matches than the page holds: also no second page.
	req = httptest.NewRequest(http.MethodGet, "/news/search?category=%E7%A7%91%E6%8A%80&q=%E5%A4%84%E7%90%86%E5%99%A8&limit=10", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	body = nil
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["has_more"] != false {
		t.Fatalf("has_more=%v; a short page must not claim another one", body["has_more"])
	}

	// More matches than the page holds: a second page really exists.
	req = httptest.NewRequest(http.MethodGet, "/news/search?category=%E7%A7%91%E6%8A%80&q=%E5%A4%84%E7%90%86%E5%99%A8&limit=2", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	body = nil
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["has_more"] != true || body["returned"] != float64(2) {
		t.Fatalf("has_more=%v returned=%v; want true/2", body["has_more"], body["returned"])
	}
	if body["before_id"] == nil || body["before_id"].(float64) == 0 {
		t.Fatalf("before_id=%v; the client needs a cursor to continue", body["before_id"])
	}
}
