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

const maxConcurrentIngestArticles = 8

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

func New(st *storage.Store, c config.Config, l *log.Logger, model *analyzer.Client) *API {
	return &API{
		Store: st, Config: c, Logger: l, Model: model,
		ingestSlots: make(chan struct{}, maxConcurrentIngestArticles),
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
	maxima, err := a.Store.CategoryMaxima(r.Context(), a.Config.Categories, audience)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"current_time": time.Now().Unix(), "page_size": a.Config.MaxQueryCount, "categories": maxima})
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
	results := make([]storage.IngestResult, len(req.Articles))
	workCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	workerCount := min(len(req.Articles), maxConcurrentIngestArticles)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if workCtx.Err() != nil {
					return
				}
				var article storage.ArticleInput
				var result storage.IngestResult
				if err := decodeArticle(req.Articles[index], &article); err != nil {
					result = storage.IngestResult{Status: "rejected", Reason: "invalid_article"}
				} else {
					var valid bool
					article, valid = storage.NormalizeArticle(article)
					if !valid {
						result = storage.IngestResult{Status: "rejected", Reason: "invalid_article"}
					} else {
						if a.ingestSlots != nil {
							select {
							case a.ingestSlots <- struct{}{}:
							case <-workCtx.Done():
								return
							}
						}
						var err error
						result, err = a.ingestArticle(workCtx, article, audience)
						if a.ingestSlots != nil {
							<-a.ingestSlots
						}
						if err != nil {
							errOnce.Do(func() {
								firstErr = err
								cancel()
							})
							return
						}
					}
				}
				results[index] = result
			}
		}()
	}

	for index := range req.Articles {
		select {
		case jobs <- index:
		case <-workCtx.Done():
			break
		}
		if workCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		a.fail(w, firstErr)
		return
	}
	if r.Context().Err() != nil {
		return
	}

	// accepted counts new records and confirmed duplicates; duplicates is a
	// subset of accepted, while rejected is disjoint.
	accepted, duplicates, rejected := 0, 0, 0
	for _, res := range results {
		switch res.Status {
		case "accepted":
			accepted++
		case "duplicate":
			accepted++
			duplicates++
		default:
			rejected++
		}
	}
	writeJSON(w, 200, map[string]any{"version": 1, "accepted": accepted, "duplicates": duplicates, "rejected": rejected, "results": results})
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
		commitLock.Lock()
		observedRevision := *revision
		commitLock.Unlock()

		candidates, err := a.Store.FindSimilarCandidates(ctx, article, audience)
		if err != nil {
			return storage.IngestResult{}, err
		}
		if len(candidates) > 0 {
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
func validCategory(got string, categories []string) bool {
	if got == "all" {
		return true
	}
	for _, c := range categories {
		if got == c {
			return true
		}
	}
	return false
}
