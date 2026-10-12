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
| Admin | `127.0.0.1:8889` | `POST /hide` |

The query listeners can be bound to other interfaces through configuration. The collector and admin listeners always bind to loopback. Protect externally exposed query listeners with your network controls or reverse proxy.

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

The service loads `.env` from the current working directory. It creates the parent directory for the configured SQLite database when needed. The default database path is `./data/newsscope.db`.

File-backed databases use WAL mode, a 5-second busy timeout, and `synchronous=NORMAL`. This keeps the database consistent while allowing a recent committed transaction to roll back after sudden power loss or a system crash. On startup, NewsScope updates SQLite planner statistics; while running, it runs `PRAGMA optimize` and a passive WAL checkpoint once per day. It relies on SQLite's automatic WAL checkpoint between maintenance runs.

To package official RSSHub, NewsScope, and the RSSHub adapter plugin into one Docker container, see the [single-container deployment guide](docker/README.zh-CN.md).

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `OPENAI_BASE_URL` | — | Base URL of the OpenAI-compatible API, for example `https://api.example.com/v1`. Required. |
| `OPENAI_API_KEY` | — | API key for the model service. Required. |
| `OPENAI_MODEL` | — | Model identifier sent to the API. Required. |
| `NORMAL_ADDR` | `127.0.0.1:8887` | Bind address for normal query APIs. |
| `VIP_ADDR` | `127.0.0.1:8888` | Bind address for VIP query APIs. |
| `COLLECTOR_PORT` | `8886` | Port for the collector listener, bound to `127.0.0.1`. |
| `ADMIN_PORT` | `8889` | Port for the admin listener, bound to `127.0.0.1`. |
| `MAX_QUERY_COUNT` | `100` | Maximum position-range length accepted by one `GET /news` request and the `page_size` reported by `GET /`. |
| `NEWS_CATEGORIES` | See [`.env.example`](.env.example) | Required JSON array of categories. Each item has a unique `name` and a non-empty `description`; model classification uses both fields. |
| `DATABASE_PATH` | `./data/newsscope.db` | SQLite database file path. |
| `ANALYSIS_WORKERS` | `3` | Number of background analysis workers. |
| `ANALYSIS_MAX_ATTEMPTS` | `5` | Maximum analysis attempts before a job is left failed. |
| `ANALYSIS_TIMEOUT_SECONDS` | `45` | Timeout for one model analysis request. |
| `ANALYSIS_MAX_BODY_CHARS` | `300000` | Maximum body characters passed to the analysis model. The stored article body remains the submitted content. |
| `ANALYSIS_MAX_DUPLICATE_DIFF_CHARS` | `30000` | Maximum candidate-only diff characters sent for each article during duplicate review. |
| `ANALYSIS_MAX_SUMMARY_CHARS` | `120` | Maximum summary characters. Bodies at or below this length are used verbatim; longer bodies are summarized and capped at this length. |

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

`accepted` counts valid articles durably queued or confirmed as duplicates; `queued` is the subset accepted for background processing, and `duplicates` is the subset already present. The response is sent after the queue transaction commits, so `accepted` does not mean the article has passed duplicate review or been stored in the article table. A queued item may later be rejected by duplicate review. Each result reports `accepted`, `duplicate`, or `rejected`; rejected entries may include a reason. A duplicate result can include the existing `article_id` when available.

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

## Admin API

### Hide an article — `POST /hide`

The admin listener binds to loopback and does not use a token. Keep it local or protect access at the host/network level.

```http
POST /hide
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

## Analysis and duplicate handling

After an article is accepted, a background worker asks the configured model to select one configured category, produce 2–6 keywords, write a Chinese summary capped by `ANALYSIS_MAX_SUMMARY_CHARS`, and assign an integer importance rating from 1 (lowest) to 5 (highest). Bodies at or below the summary limit are used verbatim; longer bodies are summarized and capped at that limit. Importance is based on supported impact, scope, and urgency rather than dramatic wording. Article bodies are stored as submitted; only `ANALYSIS_MAX_BODY_CHARS` is sent to the analysis model.

For duplicate detection, NewsScope compares each incoming article's title and body with candidate articles collected during the previous 24 hours in the same audience. Fuzzy similarity weights the title at 30% and body at 70%. The model then reviews similar candidates and confirms whether they describe the same specific event. Only model-confirmed duplicates are discarded. Normal and VIP data are not compared with each other.

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
