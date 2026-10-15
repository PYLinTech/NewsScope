# NewsScope

English | [简体中文](README.zh-CN.md)

NewsScope is a Go service for collecting, analyzing, and querying news articles. It accepts articles from independent collectors, stores them in SQLite, uses an OpenAI-compatible chat completion API to classify and summarize them, and exposes separate query APIs for the `normal` and `vip` audiences.

## Features

- Independent Python collectors send versioned JSON batches to a configured ingest endpoint. NewsScope does not discover, register, or run collectors.
- Separate `normal` and `vip` ingest endpoints use the same payload and processing flow while maintaining independent datasets.
- Model-assisted analysis generates a configured category, keywords, a concise summary, and an importance rating from 1 to 5.
- Similarity-based duplicate candidates are selected from the same audience's previous 24 hours and reviewed by the model before a duplicate is discarded.
- SQLite-backed storage and a persistent analysis queue support retry and recovery of interrupted work.
- Stable position-range queries support incremental lists and date-to-position lookups without page numbers.
- A loopback-only admin endpoint hides articles without deleting their records.

## Architecture

```text
Collectors ──► Ingest API ──► SQLite ingest queue ──► 8 ingestion workers
                                                     │
                   OpenAI-compatible API ◄── analysis workers ◄── SQLite analysis queue
                                                     │
                                      duplicate review + article storage
                                                     │
                                ┌────────────────────┴───────────────────┐
                                ▼                                        ▼
                     normal query listener                    vip query listener
```

| Listener | Default address | Endpoints |
| --- | --- | --- |
| Collector | `127.0.0.1:8886` | `POST /ingest/normal`, `POST /ingest/vip` |
| Normal query | `127.0.0.1:8887` | `GET /`, `GET /news`, `GET /news/range`, `GET /news/{id}` |
| VIP query | `127.0.0.1:8888` | Same query endpoints as normal |
| Admin | `127.0.0.1:8889` | `GET /` console, `GET /api/status`, `POST /api/hide`, `GET /api/analysis/failed`, `POST /api/analysis/retry`, `GET /api/analysis/{id}`, `POST /api/analysis/manual`, `GET /api/dedup/pending`, `POST /api/dedup/review` |

The query listeners can be bound to other interfaces through configuration. The collector and admin listeners bind to loopback by default and can be widened explicitly with `COLLECTOR_ADDR` and `ADMIN_ADDR`; neither the ingest endpoint nor the admin endpoint is authenticated, so only widen them on trusted networks. Protect externally exposed query listeners with your network controls or reverse proxy.

Normal and VIP are independent audiences. Ingesting into one does not copy the article into the other, and duplicate detection is scoped to the selected audience. The two query listeners expose only their corresponding audience.

## Requirements

- Go 1.26 or later
- An OpenAI-compatible API endpoint and model that supports the JSON analysis response used by NewsScope
- Python 3.9 or later only if you use the example collector

## Quick start

1. Clone the repository and create a local environment file:

   ```sh
   cp .env.example .env
   ```

2. Set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL` in `.env`.

3. Start the service:

   ```sh
   go run ./cmd/newsscope
   ```

4. Confirm the pipeline works — submit one article, then read it back:

   ```sh
   curl -s -X POST http://127.0.0.1:8886/ingest/normal \
     -H 'Content-Type: application/json' \
     -d '{"version":1,"articles":[{"source":"demo","title":"A company ships a new processor",
          "body":"The company released a new processor built on a newer process.",
          "collected_at":1791798120}]}'
   ```

   `{"accepted":1,...}` means it is stored; duplicate review and analysis run in
   the background. A few seconds later, query it back using a category from
   `NEWS_CATEGORIES` in `.env`:

   ```sh
   curl -s 'http://127.0.0.1:8887/news?category=科技&start=0&end=1'
   ```

   A `summary` and `keywords` in `items` means the whole path works. If a
   submission is rejected, `reason` names the field at fault, for example
   `missing_body`, `unknown_field:auther` or `collected_at_not_seconds`.

   Without a reachable model endpoint, `scripts/smoke.sh` exercises the whole
   path against a local stub model.

The service loads `.env` from the current working directory. It creates the parent directory for the configured SQLite database when needed. The default database path is `./data/newsscope.db`.

File-backed databases use WAL mode, a 5-second busy timeout, and `synchronous=NORMAL`. This keeps the database consistent while allowing a recent committed transaction to roll back after sudden power loss or a system crash. On startup, NewsScope updates SQLite planner statistics; while running, it runs `PRAGMA optimize` and a passive WAL checkpoint once per day. It relies on SQLite's automatic WAL checkpoint between maintenance runs.

To deploy with Docker, see the [deployment guide](docker/README.zh-CN.md): the core image runs NewsScope alone, and RSSHub, Redis and the collector plugin ship as an optional Compose group. Configuration changes only require `docker compose up -d`, never an image rebuild.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `OPENAI_BASE_URL` | — | Base URL of the OpenAI-compatible API, for example `https://api.example.com/v1`. Required. |
| `OPENAI_API_KEY` | — | API key for the model service. Required. |
| `OPENAI_MODEL` | — | Model identifier sent to the API. Required. |
| `NORMAL_ADDR` | `127.0.0.1:8887` | Bind address for normal query APIs. |
| `VIP_ADDR` | `127.0.0.1:8888` | Bind address for VIP query APIs. |
| `COLLECTOR_ADDR` | `127.0.0.1:8886` | Bind address for the collector listener. Defaults to loopback; set `0.0.0.0:8886` for container deployments so the port can be published directly. |
| `ADMIN_ADDR` | `127.0.0.1:8889` | Bind address for the admin listener. This API has no token, so it is not exposed by default. |
| `COLLECTOR_PORT`, `ADMIN_PORT` | — | Retired in favour of `COLLECTOR_ADDR` and `ADMIN_ADDR`. The service refuses to start while either is set and names the replacement. |
| `MAX_QUERY_COUNT` | `100` | Maximum position-range length accepted by one `GET /news` request and the `page_size` reported by `GET /`. |
| `NEWS_CATEGORIES` | See [`.env.example`](.env.example) | Required JSON array of categories. Each item has a unique `name` and a non-empty `description`; model classification uses both fields. |
| `DATABASE_PATH` | `./data/newsscope.db` | SQLite database file path. |
| `ANALYSIS_WORKERS` | `3` | Number of background analysis workers. |
| `ANALYSIS_MAX_ATTEMPTS` | `5` | Maximum analysis attempts before a job is left failed. |
| `ANALYSIS_TIMEOUT_SECONDS` | `600` | Timeout for one model request, including analysis and duplicate review. |
| `ANALYSIS_MAX_BODY_CHARS` | `300000` | Maximum body characters passed to the analysis model. The stored article body remains the submitted content. |
| `ANALYSIS_MAX_DUPLICATE_DIFF_CHARS` | `30000` | Maximum candidate-only diff characters sent for each article during duplicate review. |
| `ANALYSIS_MAX_SUMMARY_CHARS` | `120` | Maximum summary characters. Bodies at or below this length are used verbatim; longer bodies are summarized and capped at this length. |
| `IDLE_RETRY_INTERVAL_SECONDS` | `300` | How long the collector stream must stay silent before the system counts as idle. |
| `IDLE_RETRY_MAX_BACKOFF_SECONDS` | `1800` | Ceiling for the gap between idle maintenance passes while they make no progress. |
| `IDLE_RETRY_BATCH` | `20` | Maximum items handled per idle maintenance pass. |

## Idle maintenance

A model endpoint can be unavailable for a stretch. While the collector is still
delivering, the model budget goes entirely to fresh articles and **no** extra
retries are attempted. Once the stream has been quiet for
`IDLE_RETRY_INTERVAL_SECONDS`, the system works off what it still owes:

1. deferred duplicate reviews first, because their verdict decides whether an
   article is worth analysing at all;
2. then analyses the model could not finish, re-queued without anyone pressing
   the retry button.

When a pass makes no progress the gap widens `5m → 10m → 20m → …` up to
`IDLE_RETRY_MAX_BACKOFF_SECONDS`, and resets to the short interval as soon as
progress resumes.

The gate is deliberately model agnostic. It asks only whether this process has
newer work and never reads a provider's rate-limit headers, so behaviour is
identical against any OpenAI-compatible endpoint. After 20 total attempts a
single article stays `failed` for a human, so content the model will never
accept stops consuming calls.

## Retention and backup

### Retention

The database grows with the corpus, and article bodies account for well over
80% of it. Retention therefore shortens bodies rather than deleting rows:

| Variable | Default | Effect |
| --- | --- | --- |
| `BODY_RETENTION_DAYS` | `0` | Retention off. Set a day count to shorten bodies of older articles. |
| `BODY_RETENTION_CHARS` | `400` | Characters kept, with a trailing marker. |

Age is measured by the collector's `collected_at`, so a backfill of older news
ages out on its own terms. The pass runs at startup and once every 24 hours.

**Articles are never deleted.** Positions are dense counts over the corpus
(`max_position` is the number of articles), so removing one would renumber
everything after it and break every consumer's cursor. Trimming affects only the
`body` field returned by `GET /news/{id}`; title, category, summary, keywords,
position and timestamps are untouched.

Completed `ingest_queue` rows are pruned after 24 hours, since nothing reads
them again.

### Backup

The database runs in WAL mode, so **do not copy the `.db` file directly** — you
may capture an incomplete snapshot. Two reliable approaches:

**`sqlite3` online backup (preferred, no downtime):**

```sh
sqlite3 /data/newsscope/newsscope.db ".backup '/backup/newsscope-$(date +%F).db'"
```

**Docker volume snapshot:**

```sh
docker run --rm -v newsscope_newsscope-data:/data -v "$PWD/backup:/backup" debian:bookworm-slim \
  tar czf /backup/newsscope-$(date +%F).db.tgz -C /data .
```

To restore, stop the stack and put the file back. Do not restore the WAL
sidecar files (`-wal`, `-shm`):

```sh
docker compose down
cp backup/newsscope-YYYY-MM-DD.db ./data/newsscope.db
docker compose up -d
```

## Collector contract

Collectors are standalone programs maintained and operated independently from NewsScope. Configure each collector with either `http://<collector-host>:8886/ingest/normal` or `http://<collector-host>:8886/ingest/vip`. The example at [`collector/example_collector.py`](collector/example_collector.py) demonstrates only the output method; fetching, parsing, scheduling, source cursors, retries, and logging belong to each collector.

Send a batch using this JSON shape:

```json
{
  "version": 1,
  "articles": [
    {
      "source": "Example News",
      "title": "A sample headline",
      "body": "The complete article text...",
      "collected_at": 1791798120,
      "original_url": "https://example.com/news/12345"
    }
  ]
}
```

`source`, `title`, `body`, and `collected_at` are required. `original_url` is optional and may be omitted or empty. `collected_at` is the time the collector obtained the article, represented as an integer Unix timestamp in seconds. It is not the time an event described in the article occurred. NewsScope does not extract or separately process event dates; such facts remain in the submitted title and body.

The request accepts 1–500 articles and is limited to 10 MiB. Unknown JSON fields are rejected. The collector must choose the audience by selecting the endpoint; there is no audience field in the payload. Submit only source content and collection metadata. NewsScope assigns internal IDs. The collector listener persists valid submissions to SQLite and acknowledges them before duplicate review and analysis run in the background. Eight ingestion workers process the durable queue; analysis then uses the separately configured `ANALYSIS_WORKERS` count.

Example response:

```json
{
  "version": 1,
  "accepted": 1,
  "queued": 1,
  "duplicates": 0,
  "rejected": 0,
  "results": [
    {"status": "accepted"}
  ]
}
```

`accepted` counts valid articles durably queued or confirmed as duplicates; `queued` is the subset accepted for background processing, and `duplicates` is the subset already present. The response is sent after the queue transaction commits, so `accepted` does not mean the article has passed duplicate review or been stored in the article table. A queued item may later be rejected by duplicate review. Each result reports `accepted`, `duplicate`, or `rejected`. A duplicate result can include the existing `article_id` when available.

A rejected entry names the cause so a collector can fix it directly:

| `reason` | Meaning |
| --- | --- |
| `unknown_field:<name>` | The JSON carried a key the schema does not define, usually a typo. |
| `missing_source` / `missing_title` / `missing_body` | A required field is absent or blank. |
| `missing_collected_at` | No collection timestamp was supplied. |
| `collected_at_not_seconds` | The timestamp looks like milliseconds; divide by 1000. |
| `collected_at_out_of_range` | The timestamp is outside the accepted range. |
| `invalid_article` | Anything that does not fall into the cases above. |

Articles in one batch are reported individually, so one bad entry never hides the rest.

## Query API

All timestamps are Unix seconds. The normal and VIP query listeners expose the same API shape but return data only from their own audience.

### Service information — `GET /`

Returns the current server time, the maximum list-query range length, and the name, description, and current maximum position for `all` and each configured category:

```json
{
  "current_time": 1791786600,
  "page_size": 100,
  "categories": [
    {"name": "all", "description": "所有分类的资讯", "max_position": 1300},
    {"name": "科技", "description": "科学研究、技术进展、数字产品及科技产业动态", "max_position": 210}
  ]
}
```

`max_position` is the upper bound of the category's ordered database positions, not the number of visible articles that a query will return. `all` includes every record, including hidden, pending-analysis, failed-analysis, and unclassified articles. A configured category includes records currently assigned to that category, including hidden records. Unclassified articles count only toward `all`. `page_size` is configured by `MAX_QUERY_COUNT`; it is not a page number or a requirement for a frontend to display pagination.

### Position-range list — `GET /news`

```http
GET /news?category=科技&start=0&end=10
```

`category`, `start`, and `end` are required. `category` must be `all` or a configured category. Positions use the left-open, right-closed interval `(start, end]`, so `(0,10]` selects positions 1 through 10. `start` and `end` are record positions, not page numbers. The range length must not exceed `page_size`, and `end` must not exceed the current `max_position`.

Records are ordered by `collected_at` descending, then article ID descending. Position calculation includes hidden and not-yet-analyzed records. `items` filters out hidden articles and articles whose analysis is not complete, so `returned` may be less than the requested range length or zero. `has_more` is true exactly when `end < max_position`.

```json
{
  "category": "科技",
  "max_position": 75,
  "start": 0,
  "end": 10,
  "returned": 1,
  "has_more": true,
  "items": [
    {
      "id": 8123,
      "source": "Example News",
      "collected_at": 1791795360,
      "category": "科技",
      "importance": 4,
      "title": "A sample headline",
      "summary": "A concise model-generated summary.",
      "keywords": ["AI", "industry"]
    }
  ]
}
```

For incremental loading, continue from the previous `end`, for example `(0,10]`, `(10,20]`. Invalid or missing parameters, invalid categories, negative `start`, `end <= start`, an oversized range, or `end > max_position` return HTTP 400 with `{"error":"invalid_request"}`. A valid range with no visible articles returns HTTP 200 and an empty `items` array.

### Date-to-position lookup — `GET /news/range`

```http
GET /news/range?category=科技&from=1791763200&to=1791849600
```

`category`, `from`, and `to` are required. The timestamp interval is half-open: `[from,to)`, filtered by `collected_at`. Require `from < to`. The response contains only position ranges; it does not return article data. Positions use the same ordering as `GET /news` and include hidden and not-yet-analyzed records. The frontend can pass each returned `(start,end]` range to `GET /news`, splitting ranges into consecutive requests when a range exceeds `page_size`.

```json
[{"start": 10, "end": 17}]
```

No matching records returns HTTP 200 and `[]`. Invalid or missing parameters return HTTP 400 with `{"error":"invalid_request"}`. Convert the user's selected local dates to Unix timestamp boundaries in the frontend before making this request.

### Article details — `GET /news/{id}`

```http
GET /news/8123
```

Returns one visible, fully analyzed article in the current audience:

```json
{
  "items": [
    {
      "id": 8123,
      "source": "Example News",
      "collected_at": 1791795360,
      "category": "科技",
      "importance": 4,
      "title": "A sample headline",
      "summary": "A concise model-generated summary.",
      "keywords": ["AI", "industry"],
      "body": "The complete submitted article text...",
      "original_url": "https://example.com/news/12345"
    }
  ]
}
```

`original_url` is omitted when unavailable. A missing, hidden, or not-yet-analyzed article returns HTTP 404 with `{"error":"not_found"}`. An invalid ID returns HTTP 400 with `{"error":"invalid_request"}`. A query failure returns HTTP 500 with `{"error":"request_failed"}`.

### Keyword search — `GET /news/search`

```http
GET /news/search?category=科技&q=processor&limit=20&before_id=0
```

Substring match across titles, summaries and keywords. Chinese has no word
boundaries, so this searches raw substrings rather than segmenting words.
Bodies are excluded: they dominate storage and a retention pass may trim them.

**Whitespace-separated words are combined with AND, in any order.**
`q=chip export` finds articles containing both words, and `q=export chip`
returns the same results; the words need not appear adjacent. LIKE wildcards
(`%`, `_`) are escaped, so searching `50%` looks for that literal text.

| Parameter | Meaning |
| --- | --- |
| `category` | Required. `all` or a configured category. |
| `q` | Required, up to 100 characters. |
| `limit` | Optional, 1–200, default 20. |
| `before_id` | Optional cursor: the last `id` of the previous page. |

`total` is the number of matches, `items` is the page, and `has_more` plus
`before_id` drive paging. Search is a table scan rather than an index lookup.

## Admin API

The admin port also serves a **management console**: open `http://127.0.0.1:8889/` in a browser to watch the corpus and analysis queues live, then resolve anything the model could not handle — retry, write an analysis by hand, run a deferred-review pass, hide an article — without memorising a single endpoint.

Every endpoint behind the console lives under an `/api/` prefix:

| Endpoint | Purpose |
| --- | --- |
| `GET /api/status` | Aggregate state: article totals, analysis queue counts by status, deferred review count, collector queue counts, model name and configured categories |
| `POST /api/hide` | Hide one article |
| `GET /api/analysis/failed` | List failed analysis jobs |
| `POST /api/analysis/retry` | Re-queue failed jobs |
| `GET /api/analysis/{id}` | Manual analysis source (full body plus constraints) |
| `POST /api/analysis/manual` | Submit an operator-written analysis |
| `GET /api/dedup/pending` | List deferred articles |
| `POST /api/dedup/review` | Run a deferred-review pass now |

> Earlier admin paths such as `POST /hide` and `GET /analysis/failed` have moved under `/api/`; the old paths no longer respond.

Every endpoint in this section has no token authentication and binds to loopback by default. Keep it local or restrict access at the host and network level, and do not expose the admin port externally. For routine use, call these from inside the container with `docker compose exec`.

### Hide an article — `POST /api/hide`

The admin listener binds to loopback by default and does not use a token. Keep it local or protect access at the host/network level. Extra safeguards are only needed if you deliberately widen `ADMIN_ADDR`.

```http
POST /api/hide
Content-Type: application/json

{"id": 8123}
```

Success response:

```json
{
  "success": true,
  "id": 8123,
  "status": "hidden",
  "message": "资讯已隐藏"
}
```

Hiding updates the article status and hidden time; it does not delete the record. The article is omitted from list and detail results in the relevant audience, while position maxima remain unchanged. Repeating the operation returns `already_hidden`. An unknown ID returns HTTP 400 with `{"error":"invalid_request"}`.

### List failed analysis jobs — `GET /api/analysis/failed`

```http
GET /api/analysis/failed?limit=50&before_id=8123
```

`limit` is optional, defaults to 50, and must be between 1 and 100. `before_id` is an optional cursor; pass the previous response's `next_before_id` to fetch older records. Results are ordered by descending article ID and include the title, source, audience, attempt count, last error, and update time, but not the article body.

### Retry analysis jobs — `POST /api/analysis/retry`

Submit up to 100 article IDs at a time. Only jobs that are still `failed` are requeued; retrying resets their attempt count and clears the previous error. Other IDs are returned in `skipped_ids`. Workers start processing requeued jobs automatically.

```http
POST /api/analysis/retry
Content-Type: application/json

{"article_ids":[8123,8124]}
```

The success response includes `retried_ids`, `skipped_ids`, and their counts.

### Manual analysis source — `GET /api/analysis/{id}`

```http
GET /api/analysis/8123
```

The raw material for writing an analysis by hand: the full body, the job status, attempt count, last error, and whatever analysis is already stored. The response also carries `summary_max_chars` and every configured category, so the caller does not have to look them up again. `writable` reports whether a manual write is currently allowed, and is `false` while a worker holds the job (`pending` or `processing`).

A missing article returns HTTP 404 with `{"error":"not_found"}`; an invalid ID returns HTTP 400 with `{"error":"invalid_request"}`.

### Submit a manual analysis — `POST /api/analysis/manual`

When the model consistently cannot handle an article — rejected content, for example — an operator can supply the analysis directly. The article becomes visible again immediately and no longer depends on retries.

```http
POST /api/analysis/manual
Content-Type: application/json

{
  "article_id": 8123,
  "category": "科技",
  "keywords": ["处理器", "新一代工艺", "半导体"],
  "summary": "某公司发布采用新一代工艺的处理器，计划下季度上市，售价与合作伙伴名单尚未公布。",
  "importance": 3
}
```

```json
{"success": true, "article_id": 8123, "status": "completed", "mode": "manual"}
```

Validation matches the model path exactly: the category must come from the configured categories and may not be `all`; keywords must number 2 to 6, each non-empty and at most 40 characters; importance must be 1 to 5; the summary may not exceed `ANALYSIS_MAX_SUMMARY_CHARS`. A body at or below the summary limit is still summarised by using it verbatim, as the model path does.

Invalid input or an unknown article returns HTTP 400 with `{"error":"invalid_request"}`; a job owned by a running worker returns HTTP 409 with `{"error":"analysis_in_progress"}`.

Manual and model results share one table, with `model` recorded as `manual` so the two are distinguishable. The endpoint also corrects an existing automatic analysis; re-queuing the article later lets a model result overwrite the manual one.

### List deferred articles — `GET /api/dedup/pending`

```http
GET /api/dedup/pending?limit=50
```

Lists accepted articles whose duplicate review has not finished. Articles deferred while the model was unavailable are held in their own table, so they occupy no query position. `total` is the number still waiting; `items` is ordered by deferral time and includes title, source, audience, attempt count and the last error, but no body.

### Review deferred articles — `POST /api/dedup/review`

Runs one duplicate-review pass over the deferred articles immediately instead of waiting for the background sweeper. No request body.

```json
{"promoted": 3, "discarded": 1, "completed": true}
```

`promoted` counts articles confirmed unique and moved into the article table, `discarded` counts those confirmed duplicate and removed. While the model is still unreachable `completed` is `false` and `error` is returned, and every deferred article is kept. A background pass runs every 30 seconds and backs off to 5 minutes after a failure, so this endpoint is mainly for triggering a pass immediately.

## Analysis and duplicate handling

After an article is accepted, a background worker asks the configured model to select one configured category, produce 2–6 keywords, write a Chinese summary capped by `ANALYSIS_MAX_SUMMARY_CHARS`, and assign an integer importance rating from 1 (lowest) to 5 (highest). Bodies at or below the summary limit are used verbatim; longer bodies are summarized and capped at that limit. Importance is based on supported impact, scope, and urgency rather than dramatic wording. Article bodies are stored as submitted; only `ANALYSIS_MAX_BODY_CHARS` is sent to the analysis model.

For duplicate detection, NewsScope compares each incoming article's title and body with candidate articles collected during the previous 24 hours in the same audience. Fuzzy similarity weights the title at 30% and body at 70%. An exact 100% match is treated as a duplicate without model review; the model reviews all other similar candidates to confirm whether they describe the same specific event. Normal and VIP data are not compared with each other.

**An unavailable model never blocks ingestion.** When an article needs model review and the model cannot be reached, the article is stored in a dedicated pending table and the article table is left untouched: it occupies no query position and does not count toward `max_position`. Once the model recovers, a background worker reviews the backlog automatically — articles confirmed unique enter the article table and queue for analysis, confirmed duplicates are dropped with no residue. `GET /api/dedup/pending` always shows what is still waiting.

So when the model keeps returning errors (rejected content, for example), collection stays healthy: failures never cause a rejection or an endless retry, they only accumulate briefly in the pending table.

## Development

Run the Go test suite with:

```sh
go test ./...
```

The service can be built with:

```sh
go build ./cmd/newsscope
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
