package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	DB         *sql.DB
	writeMu    sync.Mutex
	jobWake    chan struct{}
	ingestWake chan struct{}
	wal        bool
}

const jobWakeBuffer = 64

const (
	AudienceNormal = "normal"
	AudienceVIP    = "vip"
	// Unix seconds through year 9999; larger contemporary values are commonly
	// millisecond timestamps and do not satisfy the collector contract.
	maxUnixSeconds = int64(253402300799)
	// The maximum configured model timeout is 600 seconds plus a 5-second
	// worker deadline margin. Keep stale-job recovery above that lease.
	processingLeaseSeconds = 10*60 + 10
)

type ArticleInput struct {
	Source      string `json:"source"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	CollectedAt int64  `json:"collected_at"`
	OriginalURL string `json:"original_url,omitempty"`
}
type IngestResult struct {
	Status    string `json:"status"`
	ArticleID int64  `json:"article_id,omitempty"`
	Reason    string `json:"reason,omitempty"`
}
type IngestJob struct {
	ID       int64
	Article  ArticleInput
	Audience string
	Attempts int
}
type Card struct {
	ID          int64    `json:"id"`
	Source      string   `json:"source"`
	CollectedAt int64    `json:"collected_at"`
	Category    string   `json:"category"`
	Importance  int      `json:"importance"`
	Title       string   `json:"title"`
	Summary     string   `json:"summary"`
	Keywords    []string `json:"keywords"`
}
type Detail struct {
	Card
	Body        string `json:"body"`
	OriginalURL string `json:"original_url,omitempty"`
}
type CategoryMax struct {
	Name        string `json:"name"`
	MaxPosition int64  `json:"max_position"`
}

func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := ensureDir(filepath.Dir(path)); err != nil {
			return nil, err
		}
	}
	dsn := path
	maxConnections := 8
	if path == ":memory:" {
		// A plain :memory: database belongs to one connection; keep it pinned
		// for tests and ephemeral stores.
		maxConnections = 1
	} else {
		absolutePath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		fileURL := &url.URL{Scheme: "file", Path: absolutePath}
		query := fileURL.Query()
		query.Set("_busy_timeout", "5000")
		query.Set("_foreign_keys", "1")
		query.Add("_pragma", "synchronous(NORMAL)")
		fileURL.RawQuery = query.Encode()
		dsn = fileURL.String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Initialize WAL before allowing the pool to open parallel connections.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	pragmas := []string{"PRAGMA journal_mode=WAL"}
	if path == ":memory:" {
		pragmas = append(pragmas, "PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON")
	}
	for _, pragma := range pragmas {
		if _, err = db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)
	s := &Store{DB: db, jobWake: make(chan struct{}, jobWakeBuffer), ingestWake: make(chan struct{}, jobWakeBuffer), wal: path != ":memory:"}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	if path != ":memory:" {
		if _, err := db.Exec("PRAGMA optimize=0x10002"); err != nil {
			db.Close()
			return nil, fmt.Errorf("optimize SQLite query planner: %w", err)
		}
	}
	return s, nil
}

// Maintain refreshes planner statistics and passively checkpoints the WAL.
// SQLite's automatic WAL checkpoint remains enabled as a safety net.
func (s *Store) Maintain(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.DB.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		return fmt.Errorf("optimize SQLite query planner: %w", err)
	}
	if !s.wal {
		return nil
	}
	var busy, logFrames, checkpointed int
	if err := s.DB.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logFrames, &checkpointed); err != nil {
		return fmt.Errorf("checkpoint SQLite WAL: %w", err)
	}
	return nil
}

func ensureDir(path string) error {
	if path == "." || path == "" {
		return nil
	}
	return os.MkdirAll(path, 0o755)
}

func (s *Store) initSchema() error {
	schema := `
CREATE TABLE IF NOT EXISTS articles (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL,
 collected_at INTEGER NOT NULL, original_url TEXT, normalized_url TEXT,
 content_hash TEXT NOT NULL, audience TEXT NOT NULL CHECK(audience IN ('normal','vip')),
 hidden INTEGER NOT NULL DEFAULT 0, hidden_at INTEGER, created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_articles_order ON articles(audience,collected_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_articles_recent_ingest ON articles(audience,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_articles_content_hash ON articles(content_hash);
CREATE INDEX IF NOT EXISTS idx_articles_normalized_url ON articles(normalized_url) WHERE normalized_url IS NOT NULL;
CREATE TABLE IF NOT EXISTS ingest_queue (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source TEXT, title TEXT, body TEXT, collected_at INTEGER, original_url TEXT,
 audience TEXT NOT NULL CHECK(audience IN ('normal','vip')),
 content_hash TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','processing','completed')),
 attempts INTEGER NOT NULL DEFAULT 0, next_run_at INTEGER NOT NULL,
 last_error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 UNIQUE(audience,content_hash)
);
CREATE INDEX IF NOT EXISTS idx_ingest_queue_ready ON ingest_queue(status,next_run_at,id);
CREATE INDEX IF NOT EXISTS idx_ingest_queue_processing ON ingest_queue(status,updated_at,id);
CREATE TABLE IF NOT EXISTS analysis_jobs (
 article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
 status TEXT NOT NULL CHECK(status IN ('pending','processing','completed','failed')),
 attempts INTEGER NOT NULL DEFAULT 0, next_run_at INTEGER NOT NULL,
 last_error TEXT, updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_pending_ready ON analysis_jobs(next_run_at,article_id) WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_jobs_processing_stale ON analysis_jobs(updated_at,article_id) WHERE status='processing';
DROP INDEX IF EXISTS idx_jobs_ready;
CREATE TABLE IF NOT EXISTS analyses (
 article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
 category TEXT NOT NULL, keywords_json TEXT NOT NULL, summary TEXT NOT NULL,
 importance INTEGER NOT NULL DEFAULT 3 CHECK(importance BETWEEN 1 AND 5),
 model TEXT NOT NULL, prompt_version TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analyses_category ON analyses(category, article_id);
`
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.DB.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Ingest(ctx context.Context, in ArticleInput, audience string) (IngestResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var valid bool
	in, valid = NormalizeArticle(in)
	if (audience != AudienceNormal && audience != AudienceVIP) || !valid {
		return IngestResult{Status: "rejected", Reason: "invalid_article"}, nil
	}
	normalized := normalizeURL(in.OriginalURL)
	contentHash := articleContentHash(in)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback()
	var id int64
	res, err := tx.ExecContext(ctx, `INSERT INTO articles(source,title,body,collected_at,original_url,normalized_url,content_hash,audience,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, in.Source, in.Title, in.Body, in.CollectedAt, nullString(in.OriginalURL), nullString(normalized), contentHash, audience, time.Now().Unix())
	if err != nil {
		return IngestResult{}, err
	}
	if id, err = res.LastInsertId(); err != nil {
		return IngestResult{}, err
	}
	now := time.Now().Unix()
	if _, err = tx.ExecContext(ctx, `INSERT INTO analysis_jobs(article_id,status,attempts,next_run_at,updated_at) VALUES(?,'pending',0,?,?)`, id, now, now); err != nil {
		return IngestResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return IngestResult{}, err
	}
	s.signalJob()
	return IngestResult{Status: "accepted", ArticleID: id}, nil
}

func articleContentHash(in ArticleInput) string {
	h := sha256.Sum256([]byte(in.Source + "\x00" + in.Title + "\x00" + in.Body))
	return hex.EncodeToString(h[:])
}

// EnqueueIngestBatch durably records valid articles before returning an ACK.
// Existing articles and completed queue entries are reported as duplicates;
// pending or processing entries are safe to acknowledge again on client retry.
func (s *Store) EnqueueIngestBatch(ctx context.Context, articles []ArticleInput, audience string) ([]IngestResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	results := make([]IngestResult, len(articles))
	if audience != AudienceNormal && audience != AudienceVIP {
		return nil, fmt.Errorf("invalid ingestion audience")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	queued := 0
	for index, original := range articles {
		article, valid := NormalizeArticle(original)
		if !valid {
			results[index] = IngestResult{Status: "rejected", Reason: "invalid_article"}
			continue
		}
		hash := articleContentHash(article)
		var exists int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM articles WHERE audience=? AND content_hash=? LIMIT 1`, audience, hash).Scan(&exists)
		if err == nil {
			results[index] = IngestResult{Status: "duplicate"}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO ingest_queue(source,title,body,collected_at,original_url,audience,content_hash,status,attempts,next_run_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'pending',0,?,?,?) ON CONFLICT(audience,content_hash) DO NOTHING`, article.Source, article.Title, article.Body, article.CollectedAt, nullString(article.OriginalURL), audience, hash, now, now, now)
		if err != nil {
			return nil, err
		}
		inserted, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if inserted == 1 {
			queued++
			results[index] = IngestResult{Status: "accepted"}
			continue
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM ingest_queue WHERE audience=? AND content_hash=?`, audience, hash).Scan(&status); err != nil {
			return nil, err
		}
		if status == "completed" {
			results[index] = IngestResult{Status: "duplicate"}
		} else {
			results[index] = IngestResult{Status: "accepted"}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	for range queued {
		s.signalIngest()
	}
	return results, nil
}

func (s *Store) ClaimIngest(ctx context.Context) (IngestJob, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return IngestJob{}, false, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	var job IngestJob
	err = tx.QueryRowContext(ctx, `SELECT id,source,title,body,collected_at,COALESCE(original_url,''),audience,attempts FROM ingest_queue WHERE status='pending' AND next_run_at<=? ORDER BY next_run_at,id LIMIT 1`, now).Scan(&job.ID, &job.Article.Source, &job.Article.Title, &job.Article.Body, &job.Article.CollectedAt, &job.Article.OriginalURL, &job.Audience, &job.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `SELECT id,source,title,body,collected_at,COALESCE(original_url,''),audience,attempts FROM ingest_queue WHERE status='processing' AND updated_at<? ORDER BY updated_at,id LIMIT 1`, now-processingLeaseSeconds).Scan(&job.ID, &job.Article.Source, &job.Article.Title, &job.Article.Body, &job.Article.CollectedAt, &job.Article.OriginalURL, &job.Audience, &job.Attempts)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return IngestJob{}, false, nil
	}
	if err != nil {
		return IngestJob{}, false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE ingest_queue SET status='processing',attempts=attempts+1,updated_at=? WHERE id=?`, now, job.ID); err != nil {
		return IngestJob{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return IngestJob{}, false, err
	}
	job.Attempts++
	return job, true, nil
}

func (s *Store) CompleteIngest(ctx context.Context, job IngestJob) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE ingest_queue SET status='completed',source=NULL,title=NULL,body=NULL,collected_at=NULL,original_url=NULL,last_error=NULL,updated_at=? WHERE id=?`, now, job.ID)
	return err
}

func (s *Store) RetryIngest(ctx context.Context, job IngestJob, cause error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	delay := int64(1 << min(job.Attempts-1, 9))
	if delay > 300 {
		delay = 300
	}
	message := cause.Error()
	if len(message) > 1000 {
		message = message[:1000]
	}
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE ingest_queue SET status='pending',next_run_at=?,last_error=?,updated_at=? WHERE id=? AND status='processing'`, now+delay, message, now, job.ID)
	if err == nil {
		s.signalIngest()
	}
	return err
}

func (s *Store) ReleaseIngest(ctx context.Context, job IngestJob) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE ingest_queue SET status='pending',next_run_at=?,updated_at=? WHERE id=? AND status='processing'`, now, now, job.ID)
	if err == nil {
		s.signalIngest()
	}
	return err
}

func (s *Store) WaitForIngest(ctx context.Context, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-s.ingestWake:
	case <-timer.C:
	}
}

func (s *Store) signalIngest() {
	select {
	case s.ingestWake <- struct{}{}:
	default:
	}
}

func (s *Store) FindExactArticle(ctx context.Context, in ArticleInput, audience string) (int64, bool, error) {
	var id int64
	err := s.DB.QueryRowContext(ctx, `SELECT id FROM articles WHERE audience=? AND content_hash=? LIMIT 1`, audience, articleContentHash(in)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// WaitForJob sleeps until an ingestion wakes a worker or the fallback timer
// expires. The timeout also discovers retries and stale leases without polling
// SQLite once per second while the service is idle.
func (s *Store) WaitForJob(ctx context.Context, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-s.jobWake:
	case <-timer.C:
	}
}

func (s *Store) signalJob() {
	select {
	case s.jobWake <- struct{}{}:
	default:
	}
}

// NormalizeArticle trims identifying metadata while preserving title and body
// exactly as supplied. Body text is compared in its original form for fuzzy
// duplicate detection.
func NormalizeArticle(in ArticleInput) (ArticleInput, bool) {
	in.Source = strings.TrimSpace(in.Source)
	in.OriginalURL = strings.TrimSpace(in.OriginalURL)
	valid := in.Source != "" && strings.TrimSpace(in.Title) != "" && strings.TrimSpace(in.Body) != "" && in.CollectedAt > 0 && in.CollectedAt <= maxUnixSeconds
	return in, valid
}

// MaxPosition returns the upper position bound for the ordered query scope.
// It counts every article occupying a position, including hidden and
// unprocessed articles; List may therefore return fewer visible items.
func (s *Store) MaxPosition(ctx context.Context, category, audience string) (int64, error) {
	return maxPosition(ctx, s.DB, category, audience)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func maxPosition(ctx context.Context, db rowQueryer, category, audience string) (int64, error) {
	query := `SELECT COUNT(*) FROM articles a WHERE a.audience=?`
	args := []any{audience}
	if category != "all" {
		query += ` AND EXISTS (SELECT 1 FROM analyses n WHERE n.article_id=a.id AND n.category=?)`
		args = append(args, category)
	}
	var n int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&n)
	return n, err
}

func (s *Store) CategoryMaxima(ctx context.Context, categories []string, audience string) ([]CategoryMax, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT n.category,COUNT(*) FROM articles a LEFT JOIN analyses n ON n.article_id=a.id WHERE a.audience=? GROUP BY n.category`, audience)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64, len(categories)+1)
	var all int64
	for rows.Next() {
		var category sql.NullString
		var count int64
		if err := rows.Scan(&category, &count); err != nil {
			return nil, err
		}
		all += count
		if category.Valid {
			counts[category.String] = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]CategoryMax, 0, len(categories)+1)
	result = append(result, CategoryMax{Name: "all", MaxPosition: all})
	for _, cat := range categories {
		result = append(result, CategoryMax{Name: cat, MaxPosition: counts[cat]})
	}
	return result, nil
}

func (s *Store) List(ctx context.Context, category, audience string, start, end int64) ([]Card, error) {
	return list(ctx, s.DB, category, audience, start, end)
}

// ListPage reads the current position limit and its requested rows from the
// same SQLite snapshot, so one response cannot mix pre-write metadata with
// post-write page contents.
func (s *Store) ListPage(ctx context.Context, category, audience string, start, end int64) (int64, []Card, error) {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()
	max, err := maxPosition(ctx, tx, category, audience)
	if err != nil {
		return 0, nil, err
	}
	if end > max {
		return max, nil, nil
	}
	items, err := list(ctx, tx, category, audience, start, end)
	if err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}
	return max, items, nil
}

func list(ctx context.Context, db rowsQueryer, category, audience string, start, end int64) ([]Card, error) {
	args := []any{audience}
	query := `WITH page AS (SELECT a.id,a.source,a.collected_at,a.title FROM articles a `
	if category != "all" {
		query += `JOIN analyses n ON n.article_id=a.id `
	}
	query += `WHERE a.audience=?`
	if category != "all" {
		query += ` AND n.category=?`
		args = append(args, category)
	}
	// Select only the requested positional window using the ordering index.
	// The previous ROW_NUMBER plan materialized and numbered the full history
	// on every request, even when the caller asked for just a few rows.
	query += ` ORDER BY a.collected_at DESC,a.id DESC LIMIT ? OFFSET ?) `
	query += `SELECT p.id,p.source,p.collected_at,COALESCE(n.category,''),COALESCE(n.importance,3),p.title,COALESCE(n.summary,''),COALESCE(n.keywords_json,'[]') FROM page p `
	query += `JOIN articles a ON a.id=p.id JOIN analysis_jobs j ON j.article_id=p.id LEFT JOIN analyses n ON n.article_id=p.id `
	query += `WHERE a.hidden=0 AND j.status='completed' ORDER BY p.collected_at DESC,p.id DESC`
	args = append(args, end-start, start)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Card{}
	for rows.Next() {
		var item Card
		var raw string
		if err := rows.Scan(&item.ID, &item.Source, &item.CollectedAt, &item.Category, &item.Importance, &item.Title, &item.Summary, &raw); err != nil {
			return nil, err
		}
		item.Keywords, err = decodeKeywords(raw)
		if err != nil {
			return nil, fmt.Errorf("decode article keywords: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) DateRange(ctx context.Context, category, audience string, from, to int64) ([]map[string]int64, error) {
	// Count every database record in the category, including hidden and
	// unprocessed articles, so the returned positions match /news ordering.
	base := `FROM articles a`
	if category != "all" {
		base += ` JOIN analyses n ON n.article_id=a.id`
	}
	filter := `a.audience=?`
	args := []any{from, to, to, audience}
	if category != "all" {
		filter += ` AND n.category=?`
		args = append(args, category)
	}
	var before, inside int64
	// Both counts come from one SQLite statement, so concurrent ingestion
	// cannot make the returned range internally inconsistent.
	q := `SELECT COALESCE(SUM(CASE WHEN a.collected_at>=? AND a.collected_at<? THEN 1 ELSE 0 END),0), ` +
		`COALESCE(SUM(CASE WHEN a.collected_at>=? THEN 1 ELSE 0 END),0) ` + base + ` WHERE ` + filter
	if err := s.DB.QueryRowContext(ctx, q, args...).Scan(&inside, &before); err != nil {
		return nil, err
	}
	if inside == 0 {
		return []map[string]int64{}, nil
	}
	return []map[string]int64{{"start": before, "end": before + inside}}, nil
}

func (s *Store) Detail(ctx context.Context, id int64, audience string) (Detail, error) {
	var d Detail
	var raw string
	err := s.DB.QueryRowContext(ctx, `SELECT a.id,a.source,a.collected_at,n.category,n.importance,a.title,n.summary,n.keywords_json,a.body,COALESCE(a.original_url,'') FROM articles a JOIN analyses n ON n.article_id=a.id JOIN analysis_jobs j ON j.article_id=a.id WHERE a.id=? AND a.audience=? AND a.hidden=0 AND j.status='completed'`, id, audience).Scan(&d.ID, &d.Source, &d.CollectedAt, &d.Category, &d.Importance, &d.Title, &d.Summary, &raw, &d.Body, &d.OriginalURL)
	if err != nil {
		return Detail{}, err
	}
	d.Keywords, err = decodeKeywords(raw)
	if err != nil {
		return Detail{}, err
	}
	return d, nil
}

func decodeKeywords(raw string) ([]string, error) {
	var keywords []string
	if err := json.Unmarshal([]byte(raw), &keywords); err != nil {
		return nil, err
	}
	if keywords == nil {
		keywords = []string{}
	}
	return keywords, nil
}

func (s *Store) Hide(ctx context.Context, id int64) (string, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var normalizedURL, contentHash string
	var hidden int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(normalized_url,''),content_hash,hidden FROM articles WHERE id=?`, id).Scan(&normalizedURL, &contentHash, &hidden); err != nil {
		return "", err
	}
	query := `UPDATE articles SET hidden=1,hidden_at=? WHERE hidden=0 AND (content_hash=?`
	args := []any{time.Now().Unix(), contentHash}
	if normalizedURL != "" {
		query += ` OR normalized_url=?`
		args = append(args, normalizedURL)
	}
	query += `)`
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	if hidden == 1 {
		return "already_hidden", nil
	}
	return "hidden", nil
}

type Job struct {
	ArticleID           int64
	Source, Title, Body string
	Attempts            int
}

func (s *Store) Claim(ctx context.Context) (Job, bool, error) {
	// The write lock keeps selection and lease update atomic across workers.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	var j Job
	err = tx.QueryRowContext(ctx, `SELECT j.article_id,a.source,a.title,a.body,j.attempts FROM analysis_jobs j JOIN articles a ON a.id=j.article_id WHERE j.status='pending' AND j.next_run_at<=? ORDER BY j.next_run_at,j.article_id LIMIT 1`, now).Scan(&j.ArticleID, &j.Source, &j.Title, &j.Body, &j.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		// Recover abandoned processing jobs only when there is no ready work.
		err = tx.QueryRowContext(ctx, `SELECT j.article_id,a.source,a.title,a.body,j.attempts FROM analysis_jobs j JOIN articles a ON a.id=j.article_id WHERE j.status='processing' AND j.updated_at<? ORDER BY j.updated_at,j.article_id LIMIT 1`, now-processingLeaseSeconds).Scan(&j.ArticleID, &j.Source, &j.Title, &j.Body, &j.Attempts)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE analysis_jobs SET status='processing',attempts=attempts+1,updated_at=? WHERE article_id=?`, now, j.ArticleID); err != nil {
		return Job{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Job{}, false, err
	}
	j.Attempts++
	return j, true, nil
}

func (s *Store) Complete(ctx context.Context, job Job, category string, keywords []string, summary string, importance int, model string) error {
	if importance < 1 || importance > 5 {
		return fmt.Errorf("importance must be between 1 and 5")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	raw, err := json.Marshal(keywords)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	if _, err = tx.ExecContext(ctx, `INSERT INTO analyses(article_id,category,keywords_json,summary,importance,model,prompt_version,created_at) VALUES(?,?,?,?,?,?,'v2',?) ON CONFLICT(article_id) DO UPDATE SET category=excluded.category,keywords_json=excluded.keywords_json,summary=excluded.summary,importance=excluded.importance,model=excluded.model,prompt_version=excluded.prompt_version,created_at=excluded.created_at`, job.ArticleID, category, string(raw), summary, importance, model, now); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE analysis_jobs SET status='completed',last_error=NULL,updated_at=? WHERE article_id=?`, now, job.ArticleID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Fail(ctx context.Context, job Job, maxAttempts int, cause error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	status := "pending"
	delay := int64(1 << min(job.Attempts-1, 8))
	if job.Attempts >= maxAttempts {
		status = "failed"
		delay = 0
	}
	msg := cause.Error()
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE analysis_jobs SET status=?,next_run_at=?,last_error=?,updated_at=? WHERE article_id=?`, status, now+delay, msg, now, job.ArticleID)
	return err
}

// Release returns a claimed job to the ready queue when the process is
// shutting down. Shutdown cancellation is not a model failure and should not
// consume a retry attempt or leave the job leased for several minutes.
func (s *Store) Release(ctx context.Context, job Job) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE analysis_jobs SET status='pending',attempts=?,next_run_at=?,last_error=NULL,updated_at=? WHERE article_id=? AND status='processing'`, max(job.Attempts-1, 0), now, now, job.ArticleID)
	if err == nil {
		s.signalJob()
	}
	return err
}

func normalizeURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	u.RawFragment = ""
	if (u.Scheme == "http" && strings.HasSuffix(u.Host, ":80")) || (u.Scheme == "https" && strings.HasSuffix(u.Host, ":443")) {
		u.Host = strings.TrimSuffix(strings.TrimSuffix(u.Host, ":80"), ":443")
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String()
}
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
