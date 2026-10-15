package server

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/storage"
)

const maxConcurrentIngestRequests = 256

// PendingDedupBatchSize bounds one duplicate-review pass so a large backlog is
// drained over several sweeps instead of one long blocking call.
const PendingDedupBatchSize = 20

// maxDedupRetries bounds how often one submission re-reads its candidate window
// when concurrent inserts keep invalidating the verdict. Past that the article
// is deferred, which is safe and visible, rather than spinning.
const maxDedupRetries = 3

var errBusyCandidateWindow = errors.New("candidate window kept changing under concurrent ingestion")

type API struct {
	Store        *storage.Store
	Config       config.Config
	Logger       *log.Logger
	Model        *analyzer.Client
	normalCommit sync.Mutex
	vipCommit    sync.Mutex
	normalRev    uint64
	vipRev       uint64
	ingestSlots  chan struct{}
	dedupRun     sync.Mutex
}

// ErrReviewInProgress reports that a deferred-review pass is already running,
// either from the background sweeper or from an earlier manual trigger.
var ErrReviewInProgress = errors.New("a deferred duplicate review pass is already running")

// panelHTML is the single-file management console served at the admin root.
var panelHTML []byte

//go:embed panel.html
var panelFile []byte

func init() { panelHTML = panelFile }

type ingestRequest struct {
	Version  int               `json:"version"`
	Articles []json.RawMessage `json:"articles"`
}
type hideRequest struct {
	ID int64 `json:"id"`
}
type retryAnalysisRequest struct {
	ArticleIDs []int64 `json:"article_ids"`
}

func New(st *storage.Store, c config.Config, l *log.Logger, model *analyzer.Client) *API {
	return &API{
		Store: st, Config: c, Logger: l, Model: model,
		ingestSlots: make(chan struct{}, maxConcurrentIngestRequests),
	}
}
func (a *API) CollectorHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest/normal", func(w http.ResponseWriter, r *http.Request) { a.ingest(w, r, storage.AudienceNormal) })
	mux.HandleFunc("POST /ingest/vip", func(w http.ResponseWriter, r *http.Request) { a.ingest(w, r, storage.AudienceVIP) })
	return a.recover(mux)
}
func (a *API) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.panel)
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("POST /api/hide", a.hide)
	mux.HandleFunc("GET /api/analysis/failed", a.failedAnalyses)
	mux.HandleFunc("POST /api/analysis/retry", a.retryAnalyses)
	mux.HandleFunc("GET /api/dedup/pending", a.pendingDedup)
	mux.HandleFunc("POST /api/dedup/review", a.reviewPendingDedup)
	mux.HandleFunc("GET /api/analysis/{id}", a.manualAnalysisSource)
	mux.HandleFunc("POST /api/analysis/manual", a.manualAnalysis)
	return a.recover(mux)
}

// panel serves the single-file management console.
func (a *API) panel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	// The console only ever talks to this origin, so everything else is denied.
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
	w.Header().Set("X-Frame-Options", "DENY")
	_, _ = w.Write(panelHTML)
}

// status is the aggregate view the management console renders.
func (a *API) status(w http.ResponseWriter, r *http.Request) {
	snapshot, err := a.Store.SystemStatus(r.Context())
	if err != nil {
		a.fail(w, err)
		return
	}
	audiences := make([]map[string]any, 0, len(snapshot.Audiences))
	totalArticles, totalFailed := int64(0), int64(0)
	for _, entry := range snapshot.Audiences {
		totalArticles += entry.Articles
		totalFailed += entry.AnalysisFailed
		audiences = append(audiences, map[string]any{
			"audience": entry.Audience, "articles": entry.Articles,
			"analysis_pending": entry.AnalysisPending, "analysis_processing": entry.AnalysisProcessing,
			"analysis_completed": entry.AnalysisCompleted, "analysis_failed": entry.AnalysisFailed,
			"categories": entry.Categories,
		})
	}
	categories := make([]map[string]string, 0, len(a.Config.Categories))
	for _, category := range a.Config.Categories {
		categories = append(categories, map[string]string{"name": category.Name, "description": category.Description})
	}
	queue := snapshot.IngestQueue
	writeJSON(w, http.StatusOK, map[string]any{
		"current_time":      time.Now().Unix(),
		"model":             a.Config.OpenAIModel,
		"summary_max_chars": a.Config.MaxSummaryChars,
		"articles":          totalArticles,
		"analysis_failed":   totalFailed,
		"dedup_pending":     snapshot.DedupPending,
		"ingest_queue": map[string]int64{
			"pending": queue["pending"], "processing": queue["processing"], "completed": queue["completed"],
		},
		"audiences":  audiences,
		"categories": categories,
	})
}

// manualAnalysisSource returns everything needed to write an analysis by hand:
// the full body, the job state, the previous result and the constraints the
// stored analysis has to satisfy.
func (a *API) manualAnalysisSource(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		invalid(w)
		return
	}
	item, err := a.Store.ArticleForManualReview(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		a.fail(w, err)
		return
	}
	categories := make([]map[string]string, 0, len(a.Config.Categories))
	for _, category := range a.Config.Categories {
		categories = append(categories, map[string]string{"name": category.Name, "description": category.Description})
	}
	payload := map[string]any{
		"article_id": id, "source": item.Article.Source, "title": item.Article.Title, "body": item.Article.Body,
		"collected_at": item.Article.CollectedAt, "audience": item.Audience,
		"status": item.JobStatus, "attempts": item.Attempts, "last_error": item.LastError,
		"writable":          item.JobStatus != "pending" && item.JobStatus != "processing",
		"summary_max_chars": a.Config.MaxSummaryChars,
		"categories":        categories,
	}
	if item.Article.OriginalURL != "" {
		payload["original_url"] = item.Article.OriginalURL
	}
	if item.HasAnalysis {
		payload["current"] = item.Existing
	}
	writeJSON(w, http.StatusOK, payload)
}

type manualAnalysisRequest struct {
	ArticleID  int64    `json:"article_id"`
	Category   string   `json:"category"`
	Keywords   []string `json:"keywords"`
	Summary    string   `json:"summary"`
	Importance int      `json:"importance"`
}

// manualAnalysis stores an operator-supplied analysis for one article. It exists
// for the articles the model cannot handle, so it enforces exactly the same
// output rules the model path does.
func (a *API) manualAnalysis(w http.ResponseWriter, r *http.Request) {
	var req manualAnalysisRequest
	if decodeJSON(w, r, 64<<10, &req) != nil || req.ArticleID <= 0 {
		invalid(w)
		return
	}
	category := strings.TrimSpace(req.Category)
	if !validCategory(category, a.Config.Categories) || category == "all" {
		invalid(w)
		return
	}
	keywords := make([]string, 0, len(req.Keywords))
	for _, word := range req.Keywords {
		trimmed := strings.TrimSpace(word)
		if trimmed == "" || utf8.RuneCountInString(trimmed) > 40 {
			invalid(w)
			return
		}
		keywords = append(keywords, trimmed)
	}
	if len(keywords) < 2 || len(keywords) > 6 {
		invalid(w)
		return
	}
	if req.Importance < 1 || req.Importance > 5 {
		invalid(w)
		return
	}
	summary := strings.TrimSpace(req.Summary)
	if summary != "" && utf8.RuneCountInString(summary) > a.Config.MaxSummaryChars {
		invalid(w)
		return
	}
	item, err := a.Store.ArticleForManualReview(r.Context(), req.ArticleID)
	if errors.Is(err, sql.ErrNoRows) {
		invalid(w)
		return
	}
	if err != nil {
		a.fail(w, err)
		return
	}
	// Short bodies are summarised by using the body verbatim, exactly as the
	// model path does, so both sources produce comparable rows.
	if utf8.RuneCountInString(item.Article.Body) <= a.Config.MaxSummaryChars {
		summary = item.Article.Body
	} else if summary == "" {
		invalid(w)
		return
	}
	if err := a.Store.CompleteManualAnalysis(r.Context(), req.ArticleID, category, keywords, summary, req.Importance); err != nil {
		if errors.Is(err, storage.ErrManualNotAllowed) {
			writeJSON(w, 409, map[string]string{"error": "analysis_in_progress"})
			return
		}
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true, "article_id": req.ArticleID, "status": "completed", "mode": "manual",
	})
}

// pendingDedup lists accepted articles whose duplicate review has not finished.
// They live outside the corpus, so this is where they remain visible.
func (a *API) pendingDedup(w http.ResponseWriter, r *http.Request) {
	limit := int64(50)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, ok := parseInt(raw)
		if !ok || parsed < 1 || parsed > 200 {
			invalid(w)
			return
		}
		limit = parsed
	}
	items, err := a.Store.ListPendingDedup(r.Context(), int(limit))
	if err != nil {
		a.fail(w, err)
		return
	}
	total, err := a.Store.CountPendingDedup(r.Context())
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "items": items})
}

// reviewPendingDedup runs a review pass immediately instead of waiting for the
// background sweeper.
func (a *API) reviewPendingDedup(w http.ResponseWriter, r *http.Request) {
	promoted, discarded, err := a.ReviewPendingDedup(r.Context(), PendingDedupBatchSize)
	if errors.Is(err, ErrReviewInProgress) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "review_in_progress"})
		return
	}
	if err != nil {
		// A model outage is an expected state, not a server fault.
		a.Logger.Printf("pending duplicate review pass incomplete: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"promoted": promoted, "discarded": discarded, "completed": false, "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"promoted": promoted, "discarded": discarded, "completed": true})
}
func (a *API) QueryHandler(audience string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { a.info(w, r, audience) })
	mux.HandleFunc("GET /news", func(w http.ResponseWriter, r *http.Request) { a.news(w, r, audience) })
	mux.HandleFunc("GET /news/range", func(w http.ResponseWriter, r *http.Request) { a.rangeNews(w, r, audience) })
	mux.HandleFunc("GET /news/search", func(w http.ResponseWriter, r *http.Request) { a.search(w, r, audience) })
	mux.HandleFunc("GET /news/{id}", func(w http.ResponseWriter, r *http.Request) { a.detail(w, r, audience) })
	// Without this a mistyped path answers 200 with the service listing, which
	// reads as success to a client that then finds no data.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
	})
	return a.recover(mux)
}

func (a *API) info(w http.ResponseWriter, r *http.Request, audience string) {
	names := make([]string, 0, len(a.Config.Categories))
	for _, category := range a.Config.Categories {
		names = append(names, category.Name)
	}
	maxima, err := a.Store.CategoryMaxima(r.Context(), names, audience)
	if err != nil {
		a.fail(w, err)
		return
	}
	descriptions := make(map[string]string, len(a.Config.Categories))
	for _, category := range a.Config.Categories {
		descriptions[category.Name] = category.Description
	}
	categories := make([]map[string]any, 0, len(maxima))
	for _, category := range maxima {
		description := descriptions[category.Name]
		if category.Name == "all" {
			description = "所有分类的资讯"
		}
		categories = append(categories, map[string]any{
			"name": category.Name, "description": description, "max_position": category.MaxPosition,
		})
	}
	writeJSON(w, 200, map[string]any{"current_time": time.Now().Unix(), "page_size": a.Config.MaxQueryCount, "categories": categories})
}
func (a *API) news(w http.ResponseWriter, r *http.Request, audience string) {
	q := r.URL.Query()
	category := q.Get("category")
	start, ok1 := parseInt(q.Get("start"))
	end, ok2 := parseInt(q.Get("end"))
	switch {
	case !validCategory(category, a.Config.Categories):
		invalidBecause(w, "unknown_category", "category", category, "available", a.categoryNames())
		return
	case !ok1 || !ok2:
		invalidBecause(w, "start_and_end_must_be_integers", "start", q.Get("start"), "end", q.Get("end"))
		return
	case start < 0:
		invalidBecause(w, "start_must_not_be_negative", "start", start)
		return
	case end <= start:
		invalidBecause(w, "end_must_be_greater_than_start", "start", start, "end", end)
		return
	case end-start > int64(a.Config.MaxQueryCount):
		invalidBecause(w, "range_too_large", "max_count", a.Config.MaxQueryCount, "requested", end-start)
		return
	}
	max, items, err := a.Store.ListPage(r.Context(), category, audience, start, end)
	if err != nil {
		a.fail(w, err)
		return
	}
	if end > max {
		invalidBecause(w, "end_beyond_max_position", "max_position", max, "end", end)
		return
	}
	writeJSON(w, 200, map[string]any{"category": category, "max_position": max, "start": start, "end": end, "returned": len(items), "has_more": end < max, "items": items})
}

// search runs a substring search over titles, summaries and keywords.
func (a *API) search(w http.ResponseWriter, r *http.Request, audience string) {
	q := r.URL.Query()
	category := q.Get("category")
	term := strings.TrimSpace(q.Get("q"))
	limit := 20
	if raw := q.Get("limit"); raw != "" {
		parsed, ok := parseInt(raw)
		if !ok || parsed < 1 || parsed > 200 {
			invalidBecause(w, "limit_must_be_between_1_and_200", "limit", raw)
			return
		}
		limit = int(parsed)
	}
	switch {
	case term == "":
		invalidBecause(w, "q_is_required")
		return
	case len([]rune(term)) > 100:
		invalidBecause(w, "q_too_long", "max", 100)
		return
	case !validCategory(category, a.Config.Categories):
		invalidBecause(w, "unknown_category", "category", category, "available", a.categoryNames())
		return
	}
	beforeID, _ := parseInt(q.Get("before_id"))
	// Ask for one row beyond the page so has_more is a fact rather than a
	// guess: a page that happens to be exactly full may still be the last one.
	total, items, err := a.Store.Search(r.Context(), category, audience, term, limit+1, beforeID)
	if err != nil {
		a.fail(w, err)
		return
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	nextBefore := int64(0)
	if hasMore {
		nextBefore = items[len(items)-1].ID
	}
	writeJSON(w, 200, map[string]any{
		"category": category, "q": term, "total": total,
		"returned": len(items), "has_more": hasMore, "before_id": nextBefore, "items": items,
	})
}

func (a *API) rangeNews(w http.ResponseWriter, r *http.Request, audience string) {
	q := r.URL.Query()
	category := q.Get("category")
	from, ok1 := parseInt(q.Get("from"))
	to, ok2 := parseInt(q.Get("to"))
	switch {
	case !validCategory(category, a.Config.Categories):
		invalidBecause(w, "unknown_category", "category", category, "available", a.categoryNames())
		return
	case !ok1 || !ok2:
		invalidBecause(w, "from_and_to_must_be_integers", "from", q.Get("from"), "to", q.Get("to"))
		return
	case from >= to:
		invalidBecause(w, "to_must_be_greater_than_from", "from", from, "to", to)
		return
	}
	ranges, err := a.Store.DateRange(r.Context(), category, audience, from, to)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 200, ranges)
}
func (a *API) detail(w http.ResponseWriter, r *http.Request, audience string) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		invalid(w)
		return
	}
	d, err := a.Store.Detail(r.Context(), id, audience)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": []storage.Detail{d}})
}

func (a *API) ingest(w http.ResponseWriter, r *http.Request, audience string) {
	select {
	case a.ingestSlots <- struct{}{}:
		defer func() { <-a.ingestSlots }()
	case <-r.Context().Done():
		return
	}
	var req ingestRequest
	if err := decodeJSON(w, r, 10<<20, &req); err != nil {
		invalid(w)
		return
	}
	if req.Version != 1 || len(req.Articles) == 0 {
		invalid(w)
		return
	}
	if len(req.Articles) > 500 {
		invalid(w)
		return
	}
	articles := make([]storage.ArticleInput, len(req.Articles))
	results := make([]storage.IngestResult, len(req.Articles))
	validIndexes := make([]int, 0, len(req.Articles))
	for index, raw := range req.Articles {
		if err := decodeArticle(raw, &articles[index]); err != nil {
			results[index] = storage.IngestResult{Status: "rejected", Reason: decodeRejectionReason(err)}
			continue
		}
		if _, reason := storage.NormalizeArticle(articles[index]); reason != "" {
			results[index] = storage.IngestResult{Status: "rejected", Reason: reason}
			continue
		}
		validIndexes = append(validIndexes, index)
	}
	validArticles := make([]storage.ArticleInput, 0, len(validIndexes))
	for _, index := range validIndexes {
		validArticles = append(validArticles, articles[index])
	}
	queuedResults, err := a.Store.EnqueueIngestBatch(r.Context(), validArticles, audience)
	if err != nil {
		a.fail(w, err)
		return
	}
	for index, result := range queuedResults {
		results[validIndexes[index]] = result
	}

	// Acceptance means durable queue ownership; duplicate review and analysis
	// continue asynchronously after this response.
	accepted, queued, duplicates, rejected := 0, 0, 0, 0
	for _, res := range results {
		switch res.Status {
		case "accepted":
			accepted++
			queued++
		case "duplicate":
			accepted++
			duplicates++
		default:
			rejected++
		}
	}
	writeJSON(w, 200, map[string]any{"version": 1, "accepted": accepted, "queued": queued, "duplicates": duplicates, "rejected": rejected, "results": results})
}

// ingestArticle performs model review without holding an audience-wide lock.
// An audience revision detects concurrent inserts; if it changes during model
// review, candidates are reloaded before an article is committed.
//
// A model that cannot review does not hold the submission hostage. The article
// is durably stored as deferred and picked up later by ReviewPendingDedup, so
// ingestion stays available through a model outage.
func (a *API) ingestArticle(ctx context.Context, article storage.ArticleInput, audience string) (storage.IngestResult, error) {
	commitLock := &a.normalCommit
	revision := &a.normalRev
	if audience == storage.AudienceVIP {
		commitLock = &a.vipCommit
		revision = &a.vipRev
	}
	for attempt := 0; ; attempt++ {
		if id, duplicate, err := a.Store.FindExactArticle(ctx, article, audience); err != nil {
			return storage.IngestResult{}, err
		} else if duplicate {
			return storage.IngestResult{Status: "duplicate", ArticleID: id}, nil
		}
		commitLock.Lock()
		observedRevision := *revision
		commitLock.Unlock()

		candidates, err := a.Store.FindSimilarCandidates(ctx, article, audience)
		if err != nil {
			return storage.IngestResult{}, err
		}
		if len(candidates) > 0 {
			for _, candidate := range candidates {
				if candidate.Similarity >= 1.0 {
					return storage.IngestResult{Status: "duplicate", ArticleID: candidate.ArticleID}, nil
				}
			}
			decision, reviewErr := a.reviewDuplicate(ctx, article, candidates)
			if reviewErr != nil {
				return a.deferArticle(ctx, article, audience, reviewErr)
			}
			if decision.duplicate {
				return storage.IngestResult{Status: "duplicate", ArticleID: decision.matchedID}, nil
			}
		}

		commitLock.Lock()
		if *revision != observedRevision {
			// Candidates were read before a concurrent insert landed, so the
			// verdict may be stale. Retry against the refreshed window; the
			// article is still stored either way.
			commitLock.Unlock()
			if attempt >= maxDedupRetries {
				return a.deferArticle(ctx, article, audience, errBusyCandidateWindow)
			}
			continue
		}
		result, err := a.Store.Ingest(ctx, article, audience)
		if err == nil && result.Status == "accepted" {
			*revision = *revision + 1
		}
		commitLock.Unlock()
		return result, err
	}
}

// bumpRevision records that the corpus for an audience changed outside the
// ingest path. Deferred-duplicate promotion inserts into articles too, so
// without this a review that started before the promotion would commit against
// a window that is no longer current.
func (a *API) bumpRevision(audience string) {
	lock, revision := &a.normalCommit, &a.normalRev
	if audience == storage.AudienceVIP {
		lock, revision = &a.vipCommit, &a.vipRev
	}
	lock.Lock()
	*revision = *revision + 1
	lock.Unlock()
}

type duplicateDecision struct {
	duplicate bool
	matchedID int64
}

func (a *API) reviewDuplicate(ctx context.Context, article storage.ArticleInput, candidates []storage.SimilarCandidate) (duplicateDecision, error) {
	if a.Model == nil {
		return duplicateDecision{}, errors.New("duplicate review model is not configured")
	}
	modelCandidates := make([]analyzer.DuplicateCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		modelCandidates = append(modelCandidates, analyzer.DuplicateCandidate{
			ArticleID: candidate.ArticleID, Source: candidate.Source, Title: candidate.Title,
			Body: candidate.Body, CollectedAt: candidate.CollectedAt, Similarity: candidate.Similarity,
		})
	}
	matchedID, isDuplicate, err := a.Model.ReviewDuplicate(ctx, article.Source, article.Title, article.Body, modelCandidates)
	if err != nil {
		return duplicateDecision{}, err
	}
	return duplicateDecision{duplicate: isDuplicate, matchedID: matchedID}, nil
}

// deferArticle stores the article for later review instead of losing it.
func (a *API) deferArticle(ctx context.Context, article storage.ArticleInput, audience string, cause error) (storage.IngestResult, error) {
	result, err := a.Store.DeferDedup(ctx, article, audience, cause)
	if err != nil {
		a.Logger.Printf("defer article for duplicate review: %v", err)
		// Storage failed too, so the worker must retry this submission.
		return storage.IngestResult{}, err
	}
	a.Logger.Printf("duplicate review deferred for %s article: %v", audience, cause)
	return result, nil
}

// ReviewPendingDedup re-reviews deferred articles in batches. Articles that
// turn out to be unique enter the corpus; duplicates are dropped.
// IdleReport summarises one idle maintenance pass.
type IdleReport struct {
	Idle      bool   `json:"idle"`
	Promoted  int    `json:"promoted"`
	Discarded int    `json:"discarded"`
	Requeued  int    `json:"requeued"`
	Reason    string `json:"reason,omitempty"`
}

// IdleMaintenance works off what the system owes itself, but only once the
// collector has gone quiet: while fresh articles are arriving the model budget
// belongs to them. Duplicates are settled first, since their verdict decides
// whether an article is worth analysing at all.
func (a *API) IdleMaintenance(ctx context.Context, quietFor time.Duration, batch int) (IdleReport, error) {
	idle, err := a.Store.IdleWindow(ctx, quietFor)
	if err != nil {
		return IdleReport{}, err
	}
	if !idle {
		return IdleReport{Idle: false, Reason: "collector still active"}, nil
	}
	report := IdleReport{Idle: true}
	// Deferred duplicates first: their verdict decides whether the article is
	// worth analysing at all.
	promoted, discarded, err := a.ReviewPendingDedup(ctx, batch)
	report.Promoted, report.Discarded = promoted, discarded
	switch {
	case errors.Is(err, ErrReviewInProgress):
		return report, nil
	case err != nil:
		report.Reason = err.Error()
		return report, err
	}
	requeued, err := a.Store.RequeueOldestFailed(ctx, batch)
	report.Requeued = requeued
	if err != nil {
		report.Reason = err.Error()
		return report, err
	}
	if report.Promoted+report.Discarded+report.Requeued == 0 {
		report.Reason = "nothing outstanding"
	}
	return report, nil
}

func (a *API) ReviewPendingDedup(ctx context.Context, limit int) (promoted, discarded int, err error) {
	// The sweeper and the console button can both ask for a pass. Letting them
	// overlap would review the same rows twice and waste model calls, so the
	// second caller is turned away instead.
	if !a.dedupRun.TryLock() {
		return 0, 0, ErrReviewInProgress
	}
	defer a.dedupRun.Unlock()
	// Only this pass needs the bodies, so they are loaded here rather than on
	// every console refresh.
	items, err := a.Store.DeferredForReview(ctx, limit)
	if err != nil {
		return 0, 0, err
	}
	for _, item := range items {
		if ctx.Err() != nil {
			return promoted, discarded, ctx.Err()
		}
		decision, reviewErr := a.reviewPending(ctx, item)
		if reviewErr != nil {
			if recordErr := a.Store.RecordPendingDedupFailure(ctx, item.ID, reviewErr); recordErr != nil {
				a.Logger.Printf("record deferred review failure for %d: %v", item.ID, recordErr)
			}
			// A model outage fails every item in the batch; stop immediately
			// instead of hammering the endpoint.
			return promoted, discarded, reviewErr
		}
		if decision.duplicate {
			if discardErr := a.Store.DiscardPendingDedup(ctx, item.ID); discardErr != nil {
				return promoted, discarded, discardErr
			}
			discarded++
			continue
		}
		promotedResult, promoteErr := a.Store.PromotePendingDedup(ctx, item)
		if promoteErr != nil {
			return promoted, discarded, promoteErr
		}
		// An identical article can land in the corpus between review and
		// promotion, so the store may still report a duplicate.
		if promotedResult.Status == "duplicate" {
			discarded++
			continue
		}
		promoted++
		a.bumpRevision(item.Audience)
	}
	return promoted, discarded, nil
}

func (a *API) reviewPending(ctx context.Context, item storage.PendingDedup) (duplicateDecision, error) {
	if id, duplicate, err := a.Store.FindExactArticle(ctx, item.Article, item.Audience); err != nil {
		return duplicateDecision{}, err
	} else if duplicate {
		return duplicateDecision{duplicate: true, matchedID: id}, nil
	}
	candidates, err := a.Store.FindSimilarCandidates(ctx, item.Article, item.Audience)
	if err != nil {
		return duplicateDecision{}, err
	}
	for _, candidate := range candidates {
		if candidate.Similarity >= 1.0 {
			return duplicateDecision{duplicate: true, matchedID: candidate.ArticleID}, nil
		}
	}
	if len(candidates) == 0 {
		return duplicateDecision{}, nil
	}
	return a.reviewDuplicate(ctx, item.Article, candidates)
}

// ProcessQueuedArticle runs duplicate review for one already acknowledged job.
func (a *API) ProcessQueuedArticle(ctx context.Context, article storage.ArticleInput, audience string) (storage.IngestResult, error) {
	return a.ingestArticle(ctx, article, audience)
}

func (a *API) hide(w http.ResponseWriter, r *http.Request) {
	var req hideRequest
	if decodeJSON(w, r, 4096, &req) != nil || req.ID <= 0 {
		invalid(w)
		return
	}
	status, err := a.Store.Hide(r.Context(), req.ID)
	if errors.Is(err, sql.ErrNoRows) {
		invalid(w)
		return
	}
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"success": true, "id": req.ID, "status": status, "message": "资讯已隐藏"})
}

func (a *API) failedAnalyses(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := int64(50)
	if raw := q.Get("limit"); raw != "" {
		parsed, ok := parseInt(raw)
		if !ok || parsed < 1 || parsed > 100 {
			invalid(w)
			return
		}
		limit = parsed
	}
	var beforeID int64
	if raw := q.Get("before_id"); raw != "" {
		parsed, ok := parseInt(raw)
		if !ok || parsed <= 0 {
			invalid(w)
			return
		}
		beforeID = parsed
	}
	items, hasMore, err := a.Store.ListFailedAnalysisJobs(r.Context(), beforeID, int(limit))
	if err != nil {
		a.fail(w, err)
		return
	}
	var nextBeforeID int64
	if hasMore && len(items) > 0 {
		nextBeforeID = items[len(items)-1].ArticleID
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "limit": limit, "next_before_id": nextBeforeID, "has_more": hasMore,
	})
}

func (a *API) retryAnalyses(w http.ResponseWriter, r *http.Request) {
	var req retryAnalysisRequest
	if decodeJSON(w, r, 16<<10, &req) != nil || len(req.ArticleIDs) == 0 || len(req.ArticleIDs) > 100 {
		invalid(w)
		return
	}
	ids := make([]int64, 0, len(req.ArticleIDs))
	seen := make(map[int64]struct{}, len(req.ArticleIDs))
	for _, id := range req.ArticleIDs {
		if id <= 0 {
			invalid(w)
			return
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	retried, skipped, err := a.Store.RetryFailedAnalysisJobs(r.Context(), ids)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"retried_ids": retried, "skipped_ids": skipped,
		"retried": len(retried), "skipped": len(skipped),
	})
}

func (a *API) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				a.Logger.Printf("panic handling request: %v", v)
				writeJSON(w, 500, map[string]string{"error": "request_failed"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (a *API) fail(w http.ResponseWriter, err error) {
	a.Logger.Printf("request failed: %v", err)
	writeJSON(w, 500, map[string]string{"error": "request_failed"})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	return decodeStrictJSON(r.Body, dst)
}
func decodeArticle(raw json.RawMessage, dst *storage.ArticleInput) error {
	return decodeStrictJSON(bytes.NewReader(raw), dst)
}

// decodeRejectionReason turns a decoding failure into something a collector
// author can act on. A misspelled key is by far the most common mistake, and
// naming it turns a silent rejection into a one-line fix.
func decodeRejectionReason(err error) string {
	const marker = "unknown field "
	msg := err.Error()
	index := strings.Index(msg, marker)
	if index < 0 {
		return "invalid_article"
	}
	field := strings.TrimSpace(msg[index+len(marker):])
	if len(field) >= 2 && field[0] == '"' {
		if end := strings.Index(field[1:], `"`); end >= 0 {
			field = field[1 : end+1]
		}
	}
	if field == "" {
		return "invalid_article"
	}
	return storage.UnknownFieldReason(field)
}

func decodeStrictJSON(reader io.Reader, dst any) error {
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
func invalid(w http.ResponseWriter) { writeJSON(w, 400, map[string]string{"error": "invalid_request"}) }

// invalidBecause names what the caller got wrong, so a bad request is a one-line
// fix rather than a hunt through the documentation.
func invalidBecause(w http.ResponseWriter, reason string, detail ...any) {
	body := map[string]any{"error": "invalid_request", "reason": reason}
	for i := 0; i+1 < len(detail); i += 2 {
		key, ok := detail[i].(string)
		if !ok {
			continue
		}
		body[key] = detail[i+1]
	}
	writeJSON(w, 400, body)
}

// categoryNames lists the categories a caller may pass, which is the fastest
// way out of a rejected category name.
func (a *API) categoryNames() []string {
	names := make([]string, 0, len(a.Config.Categories))
	for _, category := range a.Config.Categories {
		names = append(names, category.Name)
	}
	sort.Strings(names)
	return names
}
func parseInt(s string) (int64, bool) {
	if strings.TrimSpace(s) == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}
func validCategory(got string, categories []config.Category) bool {
	if got == "all" {
		return true
	}
	for _, c := range categories {
		if got == c.Name {
			return true
		}
	}
	return false
}
