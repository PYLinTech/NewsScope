# NewsScope

[English](README.md) | 简体中文

NewsScope 是一个用于采集、分析和查询资讯的 Go 服务。它接收独立采集器提交的资讯，将数据保存到 SQLite，通过兼容 OpenAI 的聊天补全 API 生成分类、摘要等分析结果，并为 `normal` 和 `vip` 两类受众提供相互独立的查询接口。

## 功能

- 独立 Python 采集器通过配置好的端点提交版本化 JSON 数据。NewsScope 不发现、注册或运行采集器。
- `normal` 和 `vip` 使用独立采集端点；两端数据格式与处理流程相同，但数据集相互独立。
- 模型分析生成预置分类、关键词、简要摘要和 1 至 5 级重要性评分。
- 以相似度筛选过去 24 小时内、同一受众的候选资讯，再交由模型确认是否为同一事件。
- 使用 SQLite 持久化资讯和分析队列，支持失败重试及中断任务恢复。
- 使用稳定的位置范围查询支持增量列表和日期位置查找，不使用页码。
- 仅监听本机的管理端点可隐藏资讯，但不会删除记录。

## 架构

```text
采集器 ──► 采集 API ──► SQLite 采集队列 ──► 8 个采集处理工作器
                                           │
兼容 OpenAI 的 API ◄── 分析工作器 ◄── SQLite 分析队列
                                           │
                                  重复审查与资讯存储
                                           │
                         ┌─────────────────┴─────────────────┐
                         ▼                                   ▼
                normal 查询监听器                    vip 查询监听器
```

| 监听器 | 默认地址 | 端点 |
| --- | --- | --- |
| 采集器 | `127.0.0.1:8886` | `POST /ingest/normal`、`POST /ingest/vip` |
| Normal 查询 | `127.0.0.1:8887` | `GET /`、`GET /news`、`GET /news/range`、`GET /news/{id}` |
| VIP 查询 | `127.0.0.1:8888` | 与 normal 相同的查询端点 |
| 管理 | `127.0.0.1:8889` | `GET /` 管理面板、`GET /api/status`、`POST /api/hide`、`GET /api/analysis/failed`、`POST /api/analysis/retry`、`GET /api/analysis/{id}`、`POST /api/analysis/manual`、`GET /api/dedup/pending`、`POST /api/dedup/review` |

查询监听器可通过配置绑定到其他网络接口。采集器和管理监听器默认绑定本机回环地址，可通过 `COLLECTOR_ADDR` 和 `ADMIN_ADDR` 显式改为其他地址；采集入口没有认证，管理接口也没有认证，请仅在可信网络中这样配置。若将查询监听器暴露到外部网络，请通过网络策略或反向代理保护访问。

Normal 和 VIP 是相互独立的受众。提交到一方的资讯不会自动复制到另一方，重复检测也仅在当前受众的数据中进行。两组查询接口分别只访问自己的数据。

## 环境要求

- Go 1.26 或更高版本
- 一个兼容 OpenAI 的 API 地址及支持 NewsScope 所需 JSON 分析响应的模型
- 仅在使用示例采集器时需要 Python 3.9 或更高版本

## 快速开始

1. 克隆仓库并创建本地环境配置：

   ```sh
   cp .env.example .env
   ```

2. 在 `.env` 中填写 `OPENAI_BASE_URL`、`OPENAI_API_KEY` 和 `OPENAI_MODEL`。

3. 启动服务：

   ```sh
   go run ./cmd/newsscope
   ```

4. 确认链路通了——先投一条资讯，再把它查回来：

   ```sh
   curl -s -X POST http://127.0.0.1:8886/ingest/normal \
     -H 'Content-Type: application/json' \
     -d '{"version":1,"articles":[{"source":"演示来源","title":"某公司发布新款处理器",
          "body":"某公司今日正式发布了新款处理器，采用新一代工艺。",
          "collected_at":1791798120}]}'
   ```

   返回 `{"accepted":1,...}` 即表示已入库，重复审查与分析在后台进行。几秒后查询（分类取自 `.env` 的 `NEWS_CATEGORIES`）：

   ```sh
   curl -s 'http://127.0.0.1:8887/news?category=科技&start=0&end=1'
   ```

   `items` 出现 `summary` 和 `keywords` 就说明整条链路正常。若提交被拒，`reason` 会指明是哪个字段不合法（如 `missing_body`、`unknown_field:auther`、`collected_at_not_seconds`）。

   没有可用的模型端点时，可用 `scripts/smoke.sh` 跑通全流程，它会自建一个本地假模型。

服务从当前工作目录加载 `.env`。若 SQLite 文件所在目录不存在，服务会创建该目录。默认数据库路径为 `./data/newsscope.db`。

文件数据库使用 WAL 模式、5 秒 busy timeout 和 `synchronous=NORMAL`。数据库仍保持一致，但突然断电或系统崩溃后，最近已提交的事务仍可能回滚。NewsScope 启动时会更新 SQLite 查询规划统计信息，运行期间每 24 小时自动执行一次 `PRAGMA optimize` 和被动 WAL 检查点；两次维护之间仍由 SQLite 自动执行 WAL 检查点。

如需使用 Docker 部署，请参阅[部署方案](docker/README.zh-CN.md)：核心镜像只运行 NewsScope，RSSHub、Redis 和采集插件作为可选集合组提供。所有配置改动只需执行 `docker compose up -d`，无需重新构建镜像。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OPENAI_BASE_URL` | — | 兼容 OpenAI 的 API 基础地址，例如 `https://api.example.com/v1`。必填。 |
| `OPENAI_API_KEY` | — | 模型服务的 API 密钥。必填。 |
| `OPENAI_MODEL` | — | 发送给 API 的模型标识。必填。 |
| `NORMAL_ADDR` | `127.0.0.1:8887` | Normal 查询接口的绑定地址。 |
| `VIP_ADDR` | `127.0.0.1:8888` | VIP 查询接口的绑定地址。 |
| `COLLECTOR_ADDR` | `127.0.0.1:8886` | 采集监听地址。默认只绑定本机回环地址；容器部署时设置为 `0.0.0.0:8886` 以直接发布端口。 |
| `ADMIN_ADDR` | `127.0.0.1:8889` | 管理监听地址。该接口无令牌认证，默认不对外暴露。 |
| `COLLECTOR_PORT`、`ADMIN_PORT` | — | 已废弃，分别由 `COLLECTOR_ADDR` 和 `ADMIN_ADDR` 取代。仍然设置时服务会拒绝启动并提示替换写法。 |
| `MAX_QUERY_COUNT` | `100` | 单次 `GET /news` 请求允许的位置范围长度上限，也是 `GET /` 返回的 `page_size`。 |
| `NEWS_CATEGORIES` | 见 [`.env.example`](.env.example) | 必填 JSON 分类数组；每项包含唯一的 `name` 和非空 `description`。模型依据名称和描述分类。 |
| `DATABASE_PATH` | `./data/newsscope.db` | SQLite 数据库文件路径。 |
| `ANALYSIS_WORKERS` | `3` | 后台分析工作器数量。 |
| `ANALYSIS_MAX_ATTEMPTS` | `5` | 分析任务被标记失败前的最大尝试次数。 |
| `ANALYSIS_TIMEOUT_SECONDS` | `600` | 单次模型请求的超时时间，包含资讯分析和重复判断。 |
| `ANALYSIS_MAX_BODY_CHARS` | `300000` | 发送给分析模型的正文最大字符数。数据库仍保存采集器提交的完整正文。 |
| `ANALYSIS_MAX_DUPLICATE_DIFF_CHARS` | `30000` | 查重时每篇候选资讯最多发送的独有差异字符数。 |
| `ANALYSIS_MAX_SUMMARY_CHARS` | `120` | 摘要最大字符数。正文不超过该值时直接将正文作为摘要；超过时由模型总结并限制在该值内。 |
| `IDLE_RETRY_INTERVAL_SECONDS` | `300` | 判定“闲时”的静默时长：采集流连续这么久没有新内容，才开始闲时维护。 |
| `IDLE_RETRY_MAX_BACKOFF_SECONDS` | `1800` | 闲时维护连续无进展时，两次尝试之间的间隔上限。 |
| `IDLE_RETRY_BATCH` | `20` | 每轮闲时维护处理的最大条目数。 |

## 闲时维护

模型可能在一段时间内不可用。采集流活跃时，系统把模型预算全部让给新到的资讯，**完全不做**额外重试；一旦采集流静默超过 `IDLE_RETRY_INTERVAL_SECONDS`，系统自动处理自己欠下的工作：

1. 先补审挂起的重复资讯（其结论决定该资讯是否值得分析）；
2. 再把分析失败的任务重新入队，无需人工点击「重试」。

若某轮没有进展，两次尝试之间的间隔按 `5m → 10m → 20m → …` 逐次拉长，直到 `IDLE_RETRY_MAX_BACKOFF_SECONDS`；一旦重新取得进展即恢复短间隔并继续清空积压。

该判定**与模型无关**：它只询问本进程是否还有更新的工作，不会读取任何服务商的限流响应头，因此对任意 OpenAI 兼容端点行为一致。单条资讯累计重试超过 20 次后保持 `failed`，交由人工处置，避免模型永远无法接受的内容持续消耗调用。

## 数据保留与备份

### 保留策略

数据库会随资讯量持续增长。正文通常占 80% 以上，因此提供按天保留策略：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `BODY_RETENTION_DAYS` | `0` | 关闭保留。设为天数后，采集时间早于该值的资讯正文会被截断。 |
| `BODY_RETENTION_CHARS` | `400` | 截断后保留的字符数，末尾追加 `…` 标记。 |

年龄按采集器提交的 `collected_at` 计算，因此补录的历史资讯按其自身时间归档。策略在启动时和此后每 24 小时各执行一次。

**资讯记录永远不会被删除。** 位置是语料上的稠密计数（`max_position` 等于文章条数），删除任何一条都会让其后所有位置前移，消费方的增量游标随即失效。截断正文只影响 `GET /news/{id}` 返回的 `body` 字段，标题、分类、摘要、关键词、位置与时间戳全部保留。

已完成处理的 `ingest_queue` 记录在 24 小时后自动清理，它们不再被读取。

### 备份

SQLite 处于 WAL 模式，**不要直接复制 `.db` 文件**，否则可能拿到不完整的快照。两种可靠方式：

**方式一：`sqlite3` 在线备份（推荐，无需停服）**

```sh
sqlite3 /data/newsscope/newsscope.db ".backup '/backup/newsscope-$(date +%F).db'"
```

**方式二：Docker 卷快照**

```sh
docker run --rm -v newsscope_newsscope-data:/data -v "$PWD/backup:/backup" debian:bookworm-slim \
  tar czf /backup/newsscope-$(date +%F).db.tgz -C /data .
```

恢复时把文件放回原路径即可，WAL 文件（`-wal`、`-shm`）不应随备份一同恢复。

```sh
docker compose down
cp backup/newsscope-YYYY-MM-DD.db ./data/newsscope.db
docker compose up -d
```

## 采集器接口

采集器是独立编写和运行的程序，由使用者自行维护。NewsScope 不负责发现、注册、调度或运行它们。每个采集器自行配置 `http://<采集服务地址>:8886/ingest/normal` 或 `http://<采集服务地址>:8886/ingest/vip`。[`collector/example_collector.py`](collector/example_collector.py) 只演示输出方法；信息源获取、解析、定时、游标、重试和日志均由采集器自行实现。

提交 JSON 批次的格式如下：

```json
{
  "version": 1,
  "articles": [
    {
      "source": "示例新闻来源",
      "title": "示例资讯标题",
      "body": "完整资讯正文……",
      "collected_at": 1791798120,
      "original_url": "https://example.com/news/12345"
    }
  ]
}
```

`source`、`title`、`body` 和 `collected_at` 为必填字段。`original_url` 可省略或为空。`collected_at` 是采集器取得该资讯的时间，使用秒级 Unix 时间戳整数；它不是资讯正文所述事件的发生时间。NewsScope 不提取或单独处理资讯事实时间，相关信息保留在提交的标题和正文中。

每个请求可包含 1 至 500 条资讯，正文请求大小上限为 10 MiB。JSON 中不接受未定义字段。采集器通过选择端点来决定受众，提交内容中不包含受众字段。采集器只提交来源和资讯内容等原始数据；内部 ID 由 NewsScope 分配。采集监听器先将有效提交持久化到 SQLite，再返回确认；重复审查和后续分析在后台执行。采集队列由 8 个工作器处理，分析工作器数量则由 `ANALYSIS_WORKERS` 单独配置。

响应示例：

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

`accepted` 表示资讯已持久化入队或已确认为重复；`queued` 是已接收入队、等待后台处理的数量，`duplicates` 是已存在资讯的数量。服务会在队列事务提交后响应，因此 `accepted` 不表示资讯已通过重复审查或已写入正式资讯表；后台重复审查仍可能拒绝某条入队资讯。每条结果的状态为 `accepted`、`duplicate` 或 `rejected`。判定为重复时，结果可能包含已有资讯的 `article_id`。

拒收结果的 `reason` 会指明具体原因，便于采集器直接修正：

| `reason` | 含义 |
| --- | --- |
| `unknown_field:<字段名>` | JSON 中出现了未定义字段，通常是拼写错误。 |
| `missing_source` / `missing_title` / `missing_body` | 必填字段缺失或仅含空白。 |
| `missing_collected_at` | 未提供采集时间。 |
| `collected_at_not_seconds` | 时间戳疑似毫秒，请除以 1000。 |
| `collected_at_out_of_range` | 时间戳超出可接受范围。 |
| `invalid_article` | 其他无法归类的情况。 |

一次请求中的多条资讯会**各自**给出原因，互不影响。

## 查询接口

所有时间戳均为秒级 Unix 时间戳。Normal 和 VIP 查询监听器提供相同格式的 API，但只返回各自受众的数据。

### 服务信息 — `GET /`

返回当前服务时间、单次列表查询的最大范围长度，以及 `all` 和每个分类的名称、描述与当前查询最大值：

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

`max_position` 表示该类别有序数据库记录的位置上界，不代表查询实际返回的可见资讯条数。`all` 包含所有资讯记录，包括隐藏、待分析、分析失败和未分类资讯。某个预置分类仅包含当前归入该分类的资讯记录，也包括隐藏资讯。尚未分类的资讯只计入 `all`。`page_size` 由 `MAX_QUERY_COUNT` 配置；它不是页码，也不要求前端显示分页控件。

### 位置范围列表 — `GET /news`

```http
GET /news?category=科技&start=0&end=10
```

`category`、`start` 和 `end` 均为必填参数。`category` 必须为 `all` 或某个预置分类。位置范围使用左开右闭语义 `(start,end]`，因此 `(0,10]` 表示第 1 至第 10 条。`start` 和 `end` 是记录位置，不是页码。范围长度不能超过 `page_size`，且 `end` 不能大于当前 `max_position`。

记录按 `collected_at` 从新到旧排序；采集时间相同时按资讯 ID 从大到小排序。位置计算包含隐藏及尚未完成分析的记录。`items` 会过滤隐藏资讯和分析尚未完成的资讯，因此 `returned` 可能少于请求范围长度，也可能为 0。仅当 `end < max_position` 时，`has_more` 为 `true`。

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
      "source": "示例新闻来源",
      "collected_at": 1791795360,
      "category": "科技",
      "importance": 4,
      "title": "示例资讯标题",
      "summary": "由模型生成的简要摘要。",
      "keywords": ["人工智能", "产业"]
    }
  ]
}
```

增量加载时，下一段使用上一段的 `end` 作为 `start`，例如 `(0,10]`、`(10,20]`。参数缺失或无效、分类无效、`start` 小于 0、`end <= start`、范围过长或 `end > max_position` 时，返回 HTTP 400 和 `{"error":"invalid_request"}`。合法范围内没有可见资讯时，返回 HTTP 200 和空 `items` 数组。

### 日期位置查询 — `GET /news/range`

```http
GET /news/range?category=科技&from=1791763200&to=1791849600
```

`category`、`from` 和 `to` 均为必填参数。按 `collected_at` 使用半开时间区间 `[from,to)` 过滤，且要求 `from < to`。响应只包含位置范围，不返回资讯内容。位置顺序与 `GET /news` 一致，并包含隐藏及尚未完成分析的记录。前端可以将返回的 `(start,end]` 范围传给 `GET /news`；若长度超过 `page_size`，应拆成连续的多次请求。

```json
[{"start": 10, "end": 17}]
```

没有匹配记录时返回 HTTP 200 和 `[]`。参数缺失或无效时返回 HTTP 400 和 `{"error":"invalid_request"}`。前端应先将用户选择的本地日期转换为 Unix 时间戳边界，再调用该接口。

### 资讯详情 — `GET /news/{id}`

```http
GET /news/8123
```

返回当前受众中一条可见且已完成分析的资讯：

```json
{
  "items": [
    {
      "id": 8123,
      "source": "示例新闻来源",
      "collected_at": 1791795360,
      "category": "科技",
      "importance": 4,
      "title": "示例资讯标题",
      "summary": "由模型生成的简要摘要。",
      "keywords": ["人工智能", "产业"],
      "body": "采集器提交的完整资讯正文……",
      "original_url": "https://example.com/news/12345"
    }
  ]
}
```

没有原文地址时省略 `original_url`。资讯不存在、已隐藏或尚未完成分析时，返回 HTTP 404 和 `{"error":"not_found"}`。ID 非法时返回 HTTP 400 和 `{"error":"invalid_request","reason":"..."}`。查询发生错误时返回 HTTP 500 和 `{"error":"request_failed"}`。

参数错误一律带上 `reason` 指明具体问题，例如 `unknown_category`（会同时返回 `available` 列出全部合法分类）、`start_and_end_must_be_integers`、`end_must_be_greater_than_start`、`range_too_large`（含 `max_count`）、`end_beyond_max_position`（含 `max_position`）。未知路径返回 HTTP 404 和 `{"error":"not_found"}`。

### 关键词搜索 — `GET /news/search`

```http
GET /news/search?category=科技&q=处理器&limit=20&before_id=0
```

在标题、摘要和关键词中做**子串匹配**。中文没有词边界，因此这里不做分词，直接按子串查找；正文不参与搜索，因为正文体积大且可能被保留策略截断。

**空格分隔的多个词按「全部命中」处理**，且词序无关：`q=芯片 出口` 会找出同时包含这两个词的文章，而 `q=出口 芯片` 结果相同——不要求它们在文中连续出现。`LIKE` 通配符（`%`、`_`）会按字面量转义，搜索 `50%` 就是找字面的「50%」。

| 参数 | 说明 |
| --- | --- |
| `category` | 必填，`all` 或已配置分类。 |
| `q` | 必填，最长 100 字符。 |
| `limit` | 可选，1–200，默认 20。 |
| `before_id` | 可选，续页游标，取上一页最后一条的 `id`。 |

返回 `total` 为命中总数，`items` 为本页结果，`has_more` 与 `before_id` 用于续页。搜索是全表扫描而非索引查询；正文占存储八成以上且可能被截断，故不参与搜索。

响应示例：

```json
{
  "category": "科技",
  "q": "处理器",
  "total": 12,
  "returned": 2,
  "has_more": true,
  "before_id": 41,
  "items": []
}
```

## 管理接口

管理端口同时提供一个**管理面板**：在浏览器打开 `http://127.0.0.1:8889/` 即可查看语料与分析队列的实时状态，并处置模型无法处理的条目——重试、人工补写分析、一键补审、隐藏资讯，全部在页面上完成，无需记忆接口。

面板之外的所有接口都位于 `/api/` 前缀下：

| 接口 | 说明 |
| --- | --- |
| `GET /api/status` | 汇总状态：资讯总量、分析队列各状态计数、待补审条数、采集队列计数、模型标识与预置分类 |
| `POST /api/hide` | 隐藏指定资讯 |
| `GET /api/analysis/failed` | 查询分析失败名录 |
| `POST /api/analysis/retry` | 将失败任务重新入队 |
| `GET /api/analysis/{id}` | 取人工分析素材（完整正文与约束） |
| `POST /api/analysis/manual` | 人工提交分析结果 |
| `GET /api/dedup/pending` | 查询待补审查重资讯 |
| `POST /api/dedup/review` | 立即执行一轮补审 |

> 旧版本的管理路径（如 `POST /hide`、`GET /analysis/failed`）已全部迁移到 `/api/` 前缀下，旧路径不再响应。

本节所有接口均无令牌认证，默认只绑定本机回环地址。请仅在本机访问，或在主机及网络层限制访问，勿将管理端口暴露到外部网络。日常运维可在容器内用 `docker compose exec` 调用。

### 隐藏资讯 — `POST /api/hide`

管理监听器默认绑定本机回环地址，不使用令牌。请仅在本机访问，或在主机及网络层限制访问。只有在你明确把 `ADMIN_ADDR` 改为其他地址时才需要额外防护。

```http
POST /api/hide
Content-Type: application/json

{"id": 8123}
```

成功响应：

```json
{
  "success": true,
  "id": 8123,
  "status": "hidden",
  "message": "资讯已隐藏"
}
```

隐藏操作只更新资讯状态和隐藏时间，不删除记录。隐藏后，该资讯不再出现在对应受众的列表和详情中，但位置最大值保持不变。重复隐藏返回 `already_hidden`。ID 不存在时返回 HTTP 400 和 `{"error":"invalid_request"}`。

### 查询分析失败记录 — `GET /api/analysis/failed`

```http
GET /api/analysis/failed?limit=50&before_id=8123
```

`limit` 可选，默认 50，范围为 1 至 100。`before_id` 可选，用上一页响应中的 `next_before_id` 查询更早的记录。结果按资讯 ID 倒序排列；失败记录包含资讯标题、来源、受众、失败次数、最后错误和更新时间，不包含正文。

### 重试分析 — `POST /api/analysis/retry`

每次最多提交 100 个资讯 ID。只有当前状态仍为 `failed` 的任务会重新入队；重试会重置失败次数并清除上次错误。其他 ID 会列入 `skipped_ids`。工作器会在提交成功后自动处理。

```http
POST /api/analysis/retry
Content-Type: application/json

{"article_ids":[8123,8124]}
```

成功响应包含 `retried_ids`、`skipped_ids` 及对应数量。

### 人工分析素材 — `GET /api/analysis/{id}`

```http
GET /api/analysis/8123
```

供人工撰写分析使用的原始素材：完整正文、任务状态、尝试次数、最后一次错误，以及当前已保存的分析结果（若有）。响应还带上 `summary_max_chars` 和全部预置分类，省去再查一次配置。`writable` 表示当前是否允许人工写入：任务正由工作器持有（`pending` 或 `processing`）时为 `false`。

资讯不存在时返回 HTTP 404 和 `{"error":"not_found"}`，ID 非法时返回 HTTP 400 和 `{"error":"invalid_request"}`。

### 人工提交分析 — `POST /api/analysis/manual`

当模型持续无法处理某条资讯（例如内容被拒绝）时，可由人工直接补写分析结果，该资讯随即恢复为可见状态，不必再依赖重试。

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

校验规则与模型输出完全一致：分类必须取自预置分类且不能是 `all`；关键词 2 至 6 项、每项非空且不超过 40 个字符；重要性为 1 至 5；摘要不超过 `ANALYSIS_MAX_SUMMARY_CHARS`。正文长度不超过摘要上限时仍以正文作为摘要，与模型路径保持一致。

参数不合法或资讯不存在时返回 HTTP 400 和 `{"error":"invalid_request"}`；任务正由工作器处理时返回 HTTP 409 和 `{"error":"analysis_in_progress"}`。

人工结果与模型结果写入同一张表，`model` 字段记为 `manual` 以区分来源。该接口也可用于修正已有的自动分析结果；若之后再重新入队，模型结果会覆盖人工结果。

### 查询待审资讯 — `GET /api/dedup/pending`

```http
GET /api/dedup/pending?limit=50
```

列出已被接收、但重复审查尚未完成的资讯。模型不可用时被挂起的资讯保存在独立表中，不占用查询位置。`total` 是仍在等待的总条数，`items` 按挂起先后返回，包含标题、来源、受众、尝试次数和最后一次错误，不包含正文。

### 补审挂起资讯 — `POST /api/dedup/review`

立即对挂起的资讯执行一轮重复审查，不必等待后台补审。无需请求体。

```json
{"promoted": 3, "discarded": 1, "completed": true}
```

`promoted` 是确认非重复、已进入正式资讯表的条数，`discarded` 是确认重复、已从待审表移除的条数。模型仍不可用时 `completed` 为 `false` 并返回 `error`，待审资讯原样保留。后台补审每 30 秒自动执行一轮；失败时退避到 5 分钟再试，因此该接口主要用于手动立即触发。

## 分析与重复判断

资讯被接收后，后台工作器会调用配置的模型：从预置分类中选择一个分类、生成 2 至 6 个关键词、生成不超过 `ANALYSIS_MAX_SUMMARY_CHARS` 的中文摘要，并给出 1（最低）至 5（最高）的整数重要性评分。正文长度不超过摘要上限时直接作为摘要；更长的正文由模型总结并限制在上限内。评分依据正文能支持的影响范围、现实影响和紧迫程度，不因夸张措辞而提高。资讯正文按采集器提交内容存储；发送给分析模型的正文长度受 `ANALYSIS_MAX_BODY_CHARS` 限制。

重复判断时，NewsScope 会将每条新资讯的标题和正文与过去 24 小时内、同一受众的候选资讯做模糊相似度比较。标题权重为 30%，正文权重为 70%。相似度达到 100% 时直接判为重复，不再调用模型；其他相似候选由模型复核是否描述同一具体事件。Normal 与 VIP 数据之间不会交叉比对。

**模型不可用不会阻塞采集。** 需要模型复核而模型不可用时，该资讯被存入独立的待审表，正式资讯表保持不变：它不占用查询位置，也不计入 `max_position`。模型恢复后，后台工作器自动补审——确认非重复的进入正式资讯表并排队分析，确认重复的直接丢弃，不留残余记录。用 `GET /api/dedup/pending` 可以随时查看仍在等待的资讯。

因此当模型持续返回错误（例如内容被拒绝）时，采集端始终正常：失败不会导致资讯被拒收或无限重试，只会暂时堆积在待审表中。

## 开发

运行 Go 测试：

```sh
go test ./...
```

构建服务：

```sh
go build ./cmd/newsscope
```

## 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE)。
