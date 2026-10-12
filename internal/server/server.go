package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/storage"
)

const maxConcurrentIngestRequests = 256

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
}
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
	mux.HandleFunc("POST /hide", a.hide)
	mux.HandleFunc("GET /analysis/failed", a.failedAnalyses)
	mux.HandleFunc("POST /analysis/retry", a.retryAnalyses)
	return a.recover(mux)
}
func (a *API) QueryHandler(audience string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) { a.info(w, r, audience) })
	mux.HandleFunc("GET /news", func(w http.ResponseWriter, r *http.Request) { a.news(w, r, audience) })
	mux.HandleFunc("GET /news/range", func(w http.ResponseWriter, r *http.Request) { a.rangeNews(w, r, audience) })
	mux.HandleFunc("GET /news/{id}", func(w http.ResponseWriter, r *http.Request) { a.detail(w, r, audience) })
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
	if !validCategory(category, a.Config.Categories) || !ok1 || !ok2 || start < 0 || end <= start || end-start > int64(a.Config.MaxQueryCount) {
		invalid(w)
		return
	}
	max, items, err := a.Store.ListPage(r.Context(), category, audience, start, end)
	if err != nil {
		a.fail(w, err)
		return
	}
	if end > max {
		invalid(w)
		return
	}
	writeJSON(w, 200, map[string]any{"category": category, "max_position": max, "start": start, "end": end, "returned": len(items), "has_more": end < max, "items": items})
}
func (a *API) rangeNews(w http.ResponseWriter, r *http.Request, audience string) {
	q := r.URL.Query()
	category := q.Get("category")
	from, ok1 := parseInt(q.Get("from"))
	to, ok2 := parseInt(q.Get("to"))
	if !validCategory(category, a.Config.Categories) || !ok1 || !ok2 || from >= to {
		invalid(w)
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
			results[index] = storage.IngestResult{Status: "rejected", Reason: "invalid_article"}
			continue
		}
		if _, valid := storage.NormalizeArticle(articles[index]); !valid {
			results[index] = storage.IngestResult{Status: "rejected", Reason: "invalid_article"}
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
func (a *API) ingestArticle(ctx context.Context, article storage.ArticleInput, audience string) (storage.IngestResult, error) {
	commitLock := &a.normalCommit
	revision := &a.normalRev
	if audience == storage.AudienceVIP {
		commitLock = &a.vipCommit
		revision = &a.vipRev
	}
	for {
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
			if a.Model == nil {
				return storage.IngestResult{}, errors.New("duplicate review model is not configured")
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
				return storage.IngestResult{}, err
			}
			if isDuplicate {
				return storage.IngestResult{Status: "duplicate", ArticleID: matchedID}, nil
			}
		}

		commitLock.Lock()
		if *revision != observedRevision {
			commitLock.Unlock()
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
