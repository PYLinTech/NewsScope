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
	QueryDB    *sql.DB
	writeMu    sync.Mutex
	jobWake    chan struct{}
	ingestWake chan struct{}
	wal        bool
}

const jobWakeBuffer = 64

const queryConnectionCount = 8

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
	s := &Store{DB: db, QueryDB: db, jobWake: make(chan struct{}, jobWakeBuffer), ingestWake: make(chan struct{}, jobWakeBuffer), wal: path != ":memory:"}
	if path != ":memory:" {
		queryDB, openErr := sql.Open("sqlite", dsn)
		if openErr != nil {
			db.Close()
			return nil, openErr
		}
		queryDB.SetMaxOpenConns(queryConnectionCount)
		queryDB.SetMaxIdleConns(queryConnectionCount)
		s.QueryDB = queryDB
	}
	if err := s.initSchema(); err != nil {
		s.Close()
		return nil, err
	}
	if path != ":memory:" {
		if _, err := db.Exec("PRAGMA optimize=0x10002"); err != nil {
			s.Close()
			return nil, fmt.Errorf("optimize SQLite query planner: %w", err)
		}
	}
	return s, nil
}

// Close releases both the worker/writer pool and the isolated query pool.
func (s *Store) Close() error {
	var queryErr error
	if s.QueryDB != nil && s.QueryDB != s.DB {
		queryErr = s.QueryDB.Close()
	}
	if s.DB != nil {
		return errors.Join(queryErr, s.DB.Close())
	}
	return queryErr
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
CREATE TABLE IF NOT EXISTS pending_articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL,
  collected_at INTEGER NOT NULL, original_url TEXT, normalized_url TEXT,
  content_hash TEXT NOT NULL,
  audience TEXT NOT NULL CHECK(audience IN ('normal','vip')),
  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  UNIQUE(audience,content_hash)
);
CREATE INDEX IF NOT EXISTS idx_pending_articles_ready ON pending_articles(attempts,id);
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
CREATE TABLE IF NOT EXISTS audience_counts (
 audience TEXT PRIMARY KEY,
 article_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS category_counts (
 audience TEXT NOT NULL,
 category TEXT NOT NULL,
 article_count INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(audience,category)
);
CREATE TABLE IF NOT EXISTS query_stats_migrations (
 version INTEGER PRIMARY KEY
);
`
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.DB.Exec(statement); err != nil {
			return err
		}
	}
	triggers := []string{
		`CREATE TRIGGER IF NOT EXISTS articles_count_insert AFTER INSERT ON articles BEGIN
			INSERT INTO audience_counts(audience,article_count) VALUES(NEW.audience,1)
			ON CONFLICT(audience) DO UPDATE SET article_count=article_count+1;
		END`,
		`CREATE TRIGGER IF NOT EXISTS articles_count_delete BEFORE DELETE ON articles BEGIN
			UPDATE audience_counts SET article_count=article_count-1 WHERE audience=OLD.audience;
			UPDATE category_counts SET article_count=article_count-1
			WHERE audience=OLD.audience AND category IN (SELECT category FROM analyses WHERE article_id=OLD.id);
		END`,
		`CREATE TRIGGER IF NOT EXISTS analyses_count_insert AFTER INSERT ON analyses BEGIN
			INSERT INTO category_counts(audience,category,article_count)
			VALUES((SELECT audience FROM articles WHERE id=NEW.article_id),NEW.category,1)
			ON CONFLICT(audience,category) DO UPDATE SET article_count=article_count+1;
		END`,
		`CREATE TRIGGER IF NOT EXISTS analyses_count_category_update AFTER UPDATE OF category ON analyses
		WHEN NEW.category<>OLD.category BEGIN
			UPDATE category_counts SET article_count=article_count-1
			WHERE audience=(SELECT audience FROM articles WHERE id=NEW.article_id) AND category=OLD.category;
			INSERT INTO category_counts(audience,category,article_count)
			VALUES((SELECT audience FROM articles WHERE id=NEW.article_id),NEW.category,1)
			ON CONFLICT(audience,category) DO UPDATE SET article_count=article_count+1;
		END`,
	}
	for _, trigger := range triggers {
		if _, err := s.DB.Exec(trigger); err != nil {
			return err
		}
	}
	return s.initializeQueryStats()
}

// initializeQueryStats backfills counters once. Triggers keep them current in
// the same transactions as article ingestion and analysis completion.
func (s *Store) initializeQueryStats() error {
	var initialized int
	err := s.DB.QueryRow(`SELECT 1 FROM query_stats_migrations WHERE version=1`).Scan(&initialized)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM audience_counts`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM category_counts`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO audience_counts(audience,article_count) SELECT audience,COUNT(*) FROM articles GROUP BY audience`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO category_counts(audience,category,article_count)
		SELECT a.audience,n.category,COUNT(*) FROM analyses n JOIN articles a ON a.id=n.article_id GROUP BY a.audience,n.category`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO audience_counts(audience,article_count) VALUES('normal',0),('vip',0) ON CONFLICT(audience) DO NOTHING`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO query_stats_migrations(version) VALUES(1)`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Ingest(ctx context.Context, in ArticleInput, audience string) (IngestResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var reason string
	in, reason = NormalizeArticle(in)
	if (audience != AudienceNormal && audience != AudienceVIP) || reason != "" {
		if reason == "" {
			reason = "invalid_article"
		}
		return IngestResult{Status: "rejected", Reason: reason}, nil
	}
	normalized := normalizeURL(in.OriginalURL)
	contentHash := articleContentHash(in)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback()
	id, err := insertArticleTx(ctx, tx, in, audience, contentHash, normalized)
	if err != nil {
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

// PendingDedup is an accepted article whose duplicate review could not finish
// because the model was unavailable. It waits in its own table instead of the
// articles corpus.
type PendingDedup struct {
	ID       int64
	Article  ArticleInput
	Audience string
}

// DeferDedup durably stores an accepted article whose duplicate review could
// not complete. Nothing is lost and nothing blocks: the caller already
// acknowledged the submission, and review resumes through ReviewPendingDedup
// once the model is reachable again.
//
// The articles table deliberately stays untouched, so a deferred article never
// occupies a query position and never inflates max_position.
func (s *Store) DeferDedup(ctx context.Context, in ArticleInput, audience string, cause error) (IngestResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if audience != AudienceNormal && audience != AudienceVIP {
		return IngestResult{}, fmt.Errorf("invalid audience")
	}
	article, reason := NormalizeArticle(in)
	if reason != "" {
		return IngestResult{Status: "rejected", Reason: reason}, nil
	}
	hash := articleContentHash(article)
	normalized := normalizeURL(article.OriginalURL)
	now := time.Now().Unix()
	var message string
	if cause != nil {
		message = truncateError(cause.Error())
	}
	res, err := s.DB.ExecContext(ctx, `INSERT INTO pending_articles(source,title,body,collected_at,original_url,normalized_url,content_hash,audience,attempts,last_error,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,0,?,?,?)
		ON CONFLICT(audience,content_hash) DO NOTHING`,
		article.Source, article.Title, article.Body, article.CollectedAt, nullString(article.OriginalURL), nullString(normalized), hash, audience, nullString(message), now, now)
	if err != nil {
		return IngestResult{}, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return IngestResult{}, err
	}
	if inserted == 1 {
		id, err := res.LastInsertId()
		if err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Status: "deferred", ArticleID: id}, nil
	}
	// The same content is already waiting for review; keep the original row and
	// only refresh the recorded cause.
	var id int64
	if err := s.DB.QueryRowContext(ctx, `SELECT id FROM pending_articles WHERE audience=? AND content_hash=?`, audience, hash).Scan(&id); err != nil {
		return IngestResult{}, err
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE pending_articles SET attempts=attempts+1,last_error=?,updated_at=? WHERE id=?`, nullString(message), now, id); err != nil {
		return IngestResult{}, err
	}
	return IngestResult{Status: "deferred", ArticleID: id}, nil
}

// PendingDedupMeta is the listing view of a deferred article. It deliberately
// omits the body: the console refreshes this list on a timer, and bodies are
// large enough that shipping them here would be pure waste.
type PendingDedupMeta struct {
	ID          int64  `json:"pending_id"`
	Source      string `json:"source"`
	Title       string `json:"title"`
	CollectedAt int64  `json:"collected_at"`
	Audience    string `json:"audience"`
	Attempts    int    `json:"attempts"`
	LastError   string `json:"last_error"`
	CreatedAt   int64  `json:"created_at"`
}

// ListPendingDedup returns the oldest deferred articles without their bodies.
func (s *Store) ListPendingDedup(ctx context.Context, limit int) ([]PendingDedupMeta, error) {
	rows, err := s.QueryDB.QueryContext(ctx, `SELECT id,source,title,collected_at,audience,attempts,COALESCE(last_error,''),created_at
		FROM pending_articles ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PendingDedupMeta, 0, limit)
	for rows.Next() {
		var item PendingDedupMeta
		if err := rows.Scan(&item.ID, &item.Source, &item.Title, &item.CollectedAt, &item.Audience, &item.Attempts, &item.LastError, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeferredForReview loads deferred articles together with their bodies. Only
// the review pass needs these, so the bodies stay out of the listing path.
func (s *Store) DeferredForReview(ctx context.Context, limit int) ([]PendingDedup, error) {
	rows, err := s.QueryDB.QueryContext(ctx, `SELECT id,source,title,body,collected_at,COALESCE(original_url,''),audience
		FROM pending_articles ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PendingDedup, 0, limit)
	for rows.Next() {
		var item PendingDedup
		if err := rows.Scan(&item.ID, &item.Article.Source, &item.Article.Title, &item.Article.Body, &item.Article.CollectedAt, &item.Article.OriginalURL, &item.Audience); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CountPendingDedup reports how many deferred articles are still waiting.
func (s *Store) CountPendingDedup(ctx context.Context) (int64, error) {
	var n int64
	err := s.QueryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_articles`).Scan(&n)
	return n, err
}

// PromotePendingDedup moves a reviewed article into the corpus, creating its
// analysis job, and removes it from the pending table. The whole transition is
// one transaction so a crash cannot duplicate or drop the article.
func (s *Store) PromotePendingDedup(ctx context.Context, item PendingDedup) (IngestResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	article, reason := NormalizeArticle(item.Article)
	if reason != "" {
		return IngestResult{}, fmt.Errorf("pending article is no longer valid: %s", reason)
	}
	hash := articleContentHash(article)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback()
	var existing int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM articles WHERE audience=? AND content_hash=? LIMIT 1`, item.Audience, hash).Scan(&existing)
	if err == nil {
		if _, err = tx.ExecContext(ctx, `DELETE FROM pending_articles WHERE id=?`, item.ID); err != nil {
			return IngestResult{}, err
		}
		if err = tx.Commit(); err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Status: "duplicate", ArticleID: existing}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return IngestResult{}, err
	}
	id, err := insertArticleTx(ctx, tx, article, item.Audience, hash, normalizeURL(article.OriginalURL))
	if err != nil {
		return IngestResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM pending_articles WHERE id=?`, item.ID); err != nil {
		return IngestResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return IngestResult{}, err
	}
	s.signalJob()
	return IngestResult{Status: "accepted", ArticleID: id}, nil
}

// DiscardPendingDedup drops a deferred article that review found to be a
// duplicate. The corpus keeps the article it matched.
func (s *Store) DiscardPendingDedup(ctx context.Context, id int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.DB.ExecContext(ctx, `DELETE FROM pending_articles WHERE id=?`, id)
	return err
}

// RecordPendingDedupFailure keeps a deferred article queued and notes why the
// review could not run. It never gives up on the article.
func (s *Store) RecordPendingDedupFailure(ctx context.Context, id int64, cause error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().Unix()
	_, err := s.DB.ExecContext(ctx, `UPDATE pending_articles SET attempts=attempts+1,last_error=?,updated_at=? WHERE id=?`, nullString(truncateError(cause.Error())), now, id)
	return err
}

func truncateError(message string) string {
	if len(message) > 1000 {
		return message[:1000]
	}
	return message
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
		article, reason := NormalizeArticle(original)
		if reason != "" {
			results[index] = IngestResult{Status: "rejected", Reason: reason}
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
	err := s.QueryDB.QueryRowContext(ctx, `SELECT id FROM articles WHERE audience=? AND content_hash=? LIMIT 1`, audience, articleContentHash(in)).Scan(&id)
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

// Rejection reasons naming the field a collector got wrong. Returning the
// specific cause is the difference between a collector author fixing a
// submission and a collector author guessing at it.
const (
	RejectMissingSource = "missing_source"
	RejectMissingTitle  = "missing_title"
	RejectMissingBody   = "missing_body"
	RejectMissingTime   = "missing_collected_at"
	RejectTimeNotSecond = "collected_at_not_seconds"
	RejectTimeRange     = "collected_at_out_of_range"
)

// UnknownFieldReason names a JSON key the collector sent that the schema does
// not define, which is nearly always a typo such as "auther" for "author".
func UnknownFieldReason(field string) string {
	return "unknown_field:" + field
}

// NormalizeArticle trims identifying metadata while preserving title and body
// exactly as supplied. Body text is compared in its original form for fuzzy
// duplicate detection. The returned reason is empty when the article is
// acceptable.
func NormalizeArticle(in ArticleInput) (ArticleInput, string) {
	in.Source = strings.TrimSpace(in.Source)
	in.OriginalURL = strings.TrimSpace(in.OriginalURL)
	switch {
	case in.Source == "":
		return in, RejectMissingSource
	case strings.TrimSpace(in.Title) == "":
		return in, RejectMissingTitle
	case strings.TrimSpace(in.Body) == "":
		return in, RejectMissingBody
	case in.CollectedAt <= 0:
		return in, RejectMissingTime
	case in.CollectedAt > maxUnixSeconds:
		// Many collectors hand over milliseconds without realising it; saying so
		// is far more useful than reporting a bare out-of-range value.
		if in.CollectedAt/1000 <= maxUnixSeconds {
			return in, RejectTimeNotSecond
		}
		return in, RejectTimeRange
	}
	return in, ""
}

// MaxPosition returns the upper position bound for the ordered query scope.
// It counts every article occupying a position, including hidden and
// unprocessed articles; List may therefore return fewer visible items.
// searchTerms splits a query into the words a reader typed, collapsing runs of
// whitespace and dropping empties.
func searchTerms(query string) []string {
	return strings.Fields(query)
}

// likePattern turns a user's term into a substring match, escaping the LIKE
// wildcards so a search for "50%" or "a_b" searches for that text.
func likePattern(term string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(term) + "%"
}

// Search finds analysed articles whose title, summary or keywords contain the
// term. Matching is a plain substring test, which is what Chinese text needs:
// it has no word boundaries to segment on.
//
// This is a scan rather than an index lookup. Article bodies are not searched:
// they are far larger, and a retention pass may already have trimmed them.
func (s *Store) Search(ctx context.Context, category, audience, query string, limit int, beforeID int64) (int, []Card, error) {
	terms := searchTerms(query)
	if len(terms) == 0 {
		return 0, []Card{}, nil
	}
	// Every whitespace-separated term must appear somewhere in the article.
	// Typing "chip export" should find articles mentioning both, not ones
	// containing that exact phrase.
	clauses := make([]string, 0, len(terms))
	args := []any{audience}
	for _, term := range terms {
		pattern := likePattern(term)
		clauses = append(clauses, ` (a.title LIKE ? ESCAPE '\' OR COALESCE(n.summary,'') LIKE ? ESCAPE '\'
		       OR COALESCE(n.keywords_json,'') LIKE ? ESCAPE '\')`)
		args = append(args, pattern, pattern, pattern)
	}
	where := ` FROM articles a
		JOIN analysis_jobs j ON j.article_id=a.id
		LEFT JOIN analyses n ON n.article_id=a.id
		WHERE a.audience=? AND a.hidden=0 AND j.status='completed'
		  AND ` + strings.Join(clauses, " AND")
	if category != "all" {
		where += ` AND n.category=?`
		args = append(args, category)
	}
	cursor := ""
	if beforeID > 0 {
		cursor = ` AND a.id<?`
		args = append(args, beforeID)
	}

	var total int
	if err := s.QueryDB.QueryRowContext(ctx, `SELECT COUNT(*)`+where+cursor, args...).Scan(&total); err != nil {
		return 0, nil, err
	}
	if total == 0 {
		return 0, []Card{}, nil
	}

	pageArgs := append(append([]any{}, args...), limit)
	rows, err := s.QueryDB.QueryContext(ctx, `SELECT a.id,a.source,a.collected_at,
		COALESCE(n.category,''),COALESCE(n.importance,3),a.title,
		COALESCE(n.summary,''),COALESCE(n.keywords_json,'[]')`+where+cursor+
		` ORDER BY a.collected_at DESC,a.id DESC LIMIT ?`, pageArgs...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	items := make([]Card, 0, limit)
	for rows.Next() {
		var item Card
		var raw string
		if err := rows.Scan(&item.ID, &item.Source, &item.CollectedAt, &item.Category, &item.Importance, &item.Title, &item.Summary, &raw); err != nil {
			return 0, nil, err
		}
		item.Keywords, err = decodeKeywords(raw)
		if err != nil {
			return 0, nil, fmt.Errorf("decode article keywords: %w", err)
		}
		items = append(items, item)
	}
	return total, items, rows.Err()
}

func (s *Store) MaxPosition(ctx context.Context, category, audience string) (int64, error) {
	return maxPosition(ctx, s.QueryDB, category, audience)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func maxPosition(ctx context.Context, db rowQueryer, category, audience string) (int64, error) {
	query := `SELECT article_count FROM audience_counts WHERE audience=?`
	args := []any{audience}
	if category != "all" {
		query = `SELECT article_count FROM category_counts WHERE audience=? AND category=?`
		args = append(args, category)
	}
	var n int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

func (s *Store) CategoryMaxima(ctx context.Context, categories []string, audience string) ([]CategoryMax, error) {
	tx, err := s.QueryDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	counts := make(map[string]int64, len(categories)+1)
	var all int64
	if err := tx.QueryRowContext(ctx, `SELECT article_count FROM audience_counts WHERE audience=?`, audience).Scan(&all); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT category,article_count FROM category_counts WHERE audience=?`, audience)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var category string
		var count int64
		if err := rows.Scan(&category, &count); err != nil {
			rows.Close()
			return nil, err
		}
		counts[category] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
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
	return list(ctx, s.QueryDB, category, audience, start, end)
}

// ListPage reads the current position limit and its requested rows from the
// same SQLite snapshot, so one response cannot mix pre-write metadata with
// post-write page contents.
func (s *Store) ListPage(ctx context.Context, category, audience string, start, end int64) (int64, []Card, error) {
	tx, err := s.QueryDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
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
	tx, err := s.QueryDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	base := `FROM articles a`
	filter := `a.audience=? AND a.collected_at>=? AND a.collected_at<?`
	insideArgs := []any{audience, from, to}
	beforeFilter := `a.audience=? AND a.collected_at>=?`
	beforeArgs := []any{audience, to}
	if category != "all" {
		base += ` JOIN analyses n ON n.article_id=a.id`
		filter += ` AND n.category=?`
		insideArgs = append(insideArgs, category)
		beforeFilter += ` AND n.category=?`
		beforeArgs = append(beforeArgs, category)
	}
	var before, inside int64
	// Separate range predicates let SQLite seek through the audience/time index
	// instead of summing over every article in the audience.
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) `+base+` WHERE `+filter, insideArgs...).Scan(&inside); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) `+base+` WHERE `+beforeFilter, beforeArgs...).Scan(&before); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
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
	err := s.QueryDB.QueryRowContext(ctx, `SELECT a.id,a.source,a.collected_at,n.category,n.importance,a.title,n.summary,n.keywords_json,a.body,COALESCE(a.original_url,'') FROM articles a JOIN analyses n ON n.article_id=a.id JOIN analysis_jobs j ON j.article_id=a.id WHERE a.id=? AND a.audience=? AND a.hidden=0 AND j.status='completed'`, id, audience).Scan(&d.ID, &d.Source, &d.CollectedAt, &d.Category, &d.Importance, &d.Title, &d.Summary, &raw, &d.Body, &d.OriginalURL)
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

// AudienceStatus summarises one audience for the management panel.
type AudienceStatus struct {
	Audience           string
	Articles           int64
	AnalysisPending    int64
	AnalysisProcessing int64
	AnalysisCompleted  int64
	AnalysisFailed     int64
	Categories         map[string]int64
}

// SystemStatus is the aggregate view the management panel renders.
type SystemStatus struct {
	Audiences    []AudienceStatus
	IngestQueue  map[string]int64
	DedupPending int64
}

// SystemStatus collects queue and corpus counters in a handful of grouped reads.
// Both audiences are always reported, even before any article exists.
func (s *Store) SystemStatus(ctx context.Context) (SystemStatus, error) {
	var status SystemStatus
	status.IngestQueue = map[string]int64{"pending": 0, "processing": 0, "completed": 0}
	// Collect into a map keyed by audience, then materialise a stable slice.
	entries := map[string]*AudienceStatus{
		AudienceNormal: {Audience: AudienceNormal, Categories: map[string]int64{}},
		AudienceVIP:    {Audience: AudienceVIP, Categories: map[string]int64{}},
	}

	rows, err := s.QueryDB.QueryContext(ctx, `SELECT audience, COUNT(*) FROM articles GROUP BY audience`)
	if err != nil {
		return SystemStatus{}, err
	}
	for rows.Next() {
		var audience string
		var count int64
		if err := rows.Scan(&audience, &count); err != nil {
			rows.Close()
			return SystemStatus{}, err
		}
		if entry, ok := entries[audience]; ok {
			entry.Articles = count
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return SystemStatus{}, err
	}
	rows.Close()

	jobRows, err := s.QueryDB.QueryContext(ctx, `SELECT a.audience, j.status, COUNT(*) FROM analysis_jobs j JOIN articles a ON a.id=j.article_id GROUP BY a.audience, j.status`)
	if err != nil {
		return SystemStatus{}, err
	}
	for jobRows.Next() {
		var audience, jobStatus string
		var count int64
		if err := jobRows.Scan(&audience, &jobStatus, &count); err != nil {
			jobRows.Close()
			return SystemStatus{}, err
		}
		entry, ok := entries[audience]
		if !ok {
			continue
		}
		switch jobStatus {
		case "pending":
			entry.AnalysisPending = count
		case "processing":
			entry.AnalysisProcessing = count
		case "completed":
			entry.AnalysisCompleted = count
		case "failed":
			entry.AnalysisFailed = count
		}
	}
	if err := jobRows.Err(); err != nil {
		jobRows.Close()
		return SystemStatus{}, err
	}
	jobRows.Close()

	categoryRows, err := s.QueryDB.QueryContext(ctx, `SELECT a.audience, n.category, COUNT(*) FROM analyses n JOIN articles a ON a.id=n.article_id GROUP BY a.audience, n.category`)
	if err != nil {
		return SystemStatus{}, err
	}
	for categoryRows.Next() {
		var audience, category string
		var count int64
		if err := categoryRows.Scan(&audience, &category, &count); err != nil {
			categoryRows.Close()
			return SystemStatus{}, err
		}
		if entry, ok := entries[audience]; ok {
			entry.Categories[category] = count
		}
	}
	if err := categoryRows.Err(); err != nil {
		categoryRows.Close()
		return SystemStatus{}, err
	}
	categoryRows.Close()

	queueRows, err := s.QueryDB.QueryContext(ctx, `SELECT status, COUNT(*) FROM ingest_queue GROUP BY status`)
	if err != nil {
		return SystemStatus{}, err
	}
	for queueRows.Next() {
		var queueStatus string
		var count int64
		if err := queueRows.Scan(&queueStatus, &count); err != nil {
			queueRows.Close()
			return SystemStatus{}, err
		}
		status.IngestQueue[queueStatus] = count
	}
	if err := queueRows.Err(); err != nil {
		queueRows.Close()
		return SystemStatus{}, err
	}
	queueRows.Close()

	if err := s.QueryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_articles`).Scan(&status.DedupPending); err != nil {
		return SystemStatus{}, err
	}
	status.Audiences = []AudienceStatus{*entries[AudienceNormal], *entries[AudienceVIP]}
	return status, nil
}

type Job struct {
	ArticleID           int64
	Source, Title, Body string
	Attempts            int
}

type FailedAnalysisJob struct {
	ArticleID   int64  `json:"article_id"`
	Source      string `json:"source"`
	Title       string `json:"title"`
	CollectedAt int64  `json:"collected_at"`
	OriginalURL string `json:"original_url,omitempty"`
	Audience    string `json:"audience"`
	Attempts    int    `json:"attempts"`
	LastError   string `json:"last_error"`
	UpdatedAt   int64  `json:"updated_at"`
}

// ManualAnalysis is the raw material an operator needs to write an analysis by
// hand: the article itself plus the state of its analysis job and whatever the
// model produced so far.
type ManualAnalysis struct {
	Article     ArticleInput
	Audience    string
	JobStatus   string
	Attempts    int
	LastError   string
	HasAnalysis bool
	Existing    Analysis
}

// Analysis is a stored analysis result.
type Analysis struct {
	Category   string
	Keywords   []string
	Summary    string
	Importance int
	Model      string
}

// ErrManualNotAllowed reports that an article's analysis job cannot be written
// by hand right now because a worker owns it.
var ErrManualNotAllowed = errors.New("analysis job is still in progress")

// ArticleForManualReview loads an article for manual analysis. Hidden articles
// are included on purpose: the admin API is how an operator inspects and fixes
// content the query APIs deliberately hide.
func (s *Store) ArticleForManualReview(ctx context.Context, id int64) (ManualAnalysis, error) {
	var item ManualAnalysis
	var rawKeywords string
	var existingID sql.NullInt64
	err := s.QueryDB.QueryRowContext(ctx, `SELECT a.source,a.title,a.body,a.collected_at,COALESCE(a.original_url,''),
		a.audience,j.status,j.attempts,COALESCE(j.last_error,''),
		n.article_id,COALESCE(n.category,''),COALESCE(n.keywords_json,''),
		COALESCE(n.summary,''),COALESCE(n.importance,0),COALESCE(n.model,'')
		FROM articles a JOIN analysis_jobs j ON j.article_id=a.id
		LEFT JOIN analyses n ON n.article_id=a.id
		WHERE a.id=?`, id).Scan(
		&item.Article.Source, &item.Article.Title, &item.Article.Body, &item.Article.CollectedAt, &item.Article.OriginalURL,
		&item.Audience, &item.JobStatus, &item.Attempts, &item.LastError,
		&existingID, &item.Existing.Category, &rawKeywords,
		&item.Existing.Summary, &item.Existing.Importance, &item.Existing.Model,
	)
	if err != nil {
		return ManualAnalysis{}, err
	}
	if existingID.Valid {
		item.HasAnalysis = true
		keywords, err := decodeKeywords(rawKeywords)
		if err != nil {
			return ManualAnalysis{}, err
		}
		item.Existing.Keywords = keywords
	}
	return item, nil
}

// CompleteManualAnalysis stores an operator-supplied analysis and marks the job
// complete, reusing the same write path the model path uses so both produce
// identical rows. Jobs owned by a running worker are rejected.
func (s *Store) CompleteManualAnalysis(ctx context.Context, id int64, category string, keywords []string, summary string, importance int) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := checkImportance(importance); err != nil {
		return err
	}
	raw, err := json.Marshal(keywords)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM analysis_jobs WHERE article_id=?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.ErrNoRows
	}
	if err != nil {
		return err
	}
	if status == "pending" || status == "processing" {
		return ErrManualNotAllowed
	}
	now := time.Now().Unix()
	if _, err = tx.ExecContext(ctx, upsertAnalysisSQL, id, category, string(raw), summary, importance, "manual", now); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE analysis_jobs SET status='completed',attempts=0,last_error=NULL,updated_at=? WHERE article_id=?`, now, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListFailedAnalysisJobs(ctx context.Context, beforeID int64, limit int) ([]FailedAnalysisJob, bool, error) {
	rows, err := s.QueryDB.QueryContext(ctx, `SELECT j.article_id,a.source,a.title,a.collected_at,COALESCE(a.original_url,''),a.audience,j.attempts,COALESCE(j.last_error,''),j.updated_at
		FROM analysis_jobs j JOIN articles a ON a.id=j.article_id
		WHERE j.status='failed' AND (?<=0 OR j.article_id<?)
		ORDER BY j.article_id DESC LIMIT ?`, beforeID, beforeID, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := make([]FailedAnalysisJob, 0, limit+1)
	for rows.Next() {
		var item FailedAnalysisJob
		if err := rows.Scan(&item.ArticleID, &item.Source, &item.Title, &item.CollectedAt, &item.OriginalURL, &item.Audience, &item.Attempts, &item.LastError, &item.UpdatedAt); err != nil {
			return nil, false, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	return items, hasMore, nil
}

// bodyTrimMarker replaces the tail of a trimmed body so a consumer can tell a
// shortened article from a complete one.
const bodyTrimMarker = "…"

// TrimOldBodies shortens the stored body of articles collected before olderThan.
//
// Age is measured by collected_at, the collector's timestamp, so a backfill of
// older news ages out on its own terms rather than on when it happened to be
// submitted.
//
// Articles are never deleted: positions are dense counts over the corpus, so
// removing a row would renumber everything after it and break the incremental
// cursors that every consumer relies on. Bodies dominate storage, so trimming
// them bounds growth while leaving titles, categories, summaries, keywords and
// every position untouched.
func (s *Store) TrimOldBodies(ctx context.Context, olderThan time.Duration, keepChars int) (int64, error) {
	// A non-positive age means retention is off. Without this guard a zero
	// duration would put the cutoff at "now" and trim the entire corpus.
	if olderThan <= 0 || keepChars < 1 {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cutoff := time.Now().Unix() - int64(olderThan/time.Second)
	result, err := s.DB.ExecContext(ctx,
		`UPDATE articles SET body=substr(body,1,?)||? WHERE collected_at<? AND length(body)>?`,
		keepChars, bodyTrimMarker, cutoff, keepChars)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// PruneCompletedIngest drops finished ingest_queue rows. The article already
// lives in articles, so nothing reads a completed queue row again, and keeping
// them would grow the table without bound.
func (s *Store) PruneCompletedIngest(ctx context.Context, completedBefore time.Duration) (int64, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cutoff := time.Now().Unix() - int64(completedBefore/time.Second)
	result, err := s.DB.ExecContext(ctx,
		`DELETE FROM ingest_queue WHERE status='completed' AND updated_at<?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// AutoRetryAttemptCeiling bounds how many times an analysis may be retried by
// the idle sweeper. Past it the article stays failed for a human to look at,
// so content the model will never accept stops consuming retries indefinitely.
const AutoRetryAttemptCeiling = 20

// IdleWindow reports whether the collector stream has gone quiet and no job is
// currently in flight. Work that already failed deliberately does not count as
// activity: retrying it is exactly what idle time is for.
//
// The check is model agnostic on purpose. It asks whether this process has
// anything newer to work on, never what a remote endpoint would allow.
func (s *Store) IdleWindow(ctx context.Context, quietFor time.Duration) (bool, error) {
	var recent, inFlight int
	cutoff := time.Now().Unix() - int64(quietFor/time.Second)
	err := s.QueryDB.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM ingest_queue WHERE created_at>=?),
		       (SELECT COUNT(*) FROM ingest_queue WHERE status='processing')
		     + (SELECT COUNT(*) FROM analysis_jobs WHERE status='processing')`, cutoff).
		Scan(&recent, &inFlight)
	if err != nil {
		return false, err
	}
	return recent == 0 && inFlight == 0, nil
}

// RequeueOldestFailed puts the oldest failed analyses back in the queue so the
// normal workers pick them up. Unlike the console action this keeps the attempt
// counter, so repeated idle retries widen the worker's own backoff as well,
// and it stops once AutoRetryAttemptCeiling is reached.
func (s *Store) RequeueOldestFailed(ctx context.Context, limit int) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().Unix()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE analysis_jobs SET status='pending',attempts=attempts+1,last_error=NULL,next_run_at=?,updated_at=?
		WHERE article_id IN (
			SELECT article_id FROM analysis_jobs
			WHERE status='failed' AND attempts<? AND updated_at<=?
			ORDER BY updated_at,article_id LIMIT ?
		)`, now, now, AutoRetryAttemptCeiling, now-int64(idleRetrySettleSeconds/time.Second), limit)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if affected > 0 {
		s.signalJob()
	}
	return int(affected), nil
}

// idleRetrySettleSeconds keeps a freshly failed analysis from being picked up
// by the very next idle sweep, which would otherwise retry it in a tight loop.
// It must carry the time.Duration type: as an untyped constant the division
// below would silently truncate to zero.
const idleRetrySettleSeconds = 60 * time.Second

func (s *Store) RetryFailedAnalysisJobs(ctx context.Context, articleIDs []int64) (retried, skipped []int64, err error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	retried = make([]int64, 0, len(articleIDs))
	skipped = make([]int64, 0, len(articleIDs))
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	for _, id := range articleIDs {
		result, err := tx.ExecContext(ctx, `UPDATE analysis_jobs SET status='pending',attempts=0,next_run_at=?,last_error=NULL,updated_at=? WHERE article_id=? AND status='failed'`, now, now, id)
		if err != nil {
			return nil, nil, err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, nil, err
		}
		if count == 1 {
			retried = append(retried, id)
		} else {
			skipped = append(skipped, id)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	if len(retried) > 0 {
		s.signalJob()
	}
	return retried, skipped, nil
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

// upsertAnalysisSQL is shared by the model path and the manual path so both
// produce structurally identical rows and only differ in the recorded model.
const upsertAnalysisSQL = `INSERT INTO analyses(article_id,category,keywords_json,summary,importance,model,prompt_version,created_at)
	VALUES(?,?,?,?,?,?,'v2',?)
	ON CONFLICT(article_id) DO UPDATE SET category=excluded.category,keywords_json=excluded.keywords_json,
	summary=excluded.summary,importance=excluded.importance,model=excluded.model,prompt_version=excluded.prompt_version,created_at=excluded.created_at`

// insertArticleTx writes an article row and its pending analysis job, the one
// step shared by every path that admits an article into the corpus.
func insertArticleTx(ctx context.Context, tx *sql.Tx, article ArticleInput, audience, hash, normalizedURL string) (int64, error) {
	now := time.Now().Unix()
	res, err := tx.ExecContext(ctx, `INSERT INTO articles(source,title,body,collected_at,original_url,normalized_url,content_hash,audience,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		article.Source, article.Title, article.Body, article.CollectedAt, nullString(article.OriginalURL), nullString(normalizedURL), hash, audience, now)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO analysis_jobs(article_id,status,attempts,next_run_at,updated_at) VALUES(?,'pending',0,?,?)`, id, now, now); err != nil {
		return 0, err
	}
	return id, nil
}

// checkImportance guards every analysis write, model or manual.
func checkImportance(importance int) error {
	if importance < 1 || importance > 5 {
		return errors.New("importance must be between 1 and 5")
	}
	return nil
}

func (s *Store) Complete(ctx context.Context, job Job, category string, keywords []string, summary string, importance int, model string) error {
	if err := checkImportance(importance); err != nil {
		return err
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
	if _, err = tx.ExecContext(ctx, upsertAnalysisSQL, job.ArticleID, category, string(raw), summary, importance, model, now); err != nil {
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
