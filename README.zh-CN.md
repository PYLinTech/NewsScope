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
独立采集器
        │ POST /ingest/normal 或 /ingest/vip
        ▼
NewsScope 采集监听器 ──► SQLite ──► 分析工作器 ──► 兼容 OpenAI 的 API
                                  │
                 ┌────────────────┴────────────────┐
                 ▼                                 ▼
       normal 查询监听器                  vip 查询监听器
```

| 监听器 | 默认地址 | 端点 |
| --- | --- | --- |
| 采集器 | `127.0.0.1:8886` | `POST /ingest/normal`、`POST /ingest/vip` |
| Normal 查询 | `127.0.0.1:8887` | `GET /`、`GET /news`、`GET /news/range`、`GET /news/{id}` |
| VIP 查询 | `127.0.0.1:8888` | 与 normal 相同的查询端点 |
| 管理 | `127.0.0.1:8889` | `POST /hide` |

查询监听器可通过配置绑定到其他网络接口。采集器和管理监听器始终绑定本机回环地址。若将查询监听器暴露到外部网络，请通过网络策略或反向代理保护访问。

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

服务从当前工作目录加载 `.env`。若 SQLite 文件所在目录不存在，服务会创建该目录。默认数据库路径为 `./data/newsscope.db`。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OPENAI_BASE_URL` | — | 兼容 OpenAI 的 API 基础地址，例如 `https://api.example.com/v1`。必填。 |
| `OPENAI_API_KEY` | — | 模型服务的 API 密钥。必填。 |
| `OPENAI_MODEL` | — | 发送给 API 的模型标识。必填。 |
| `NORMAL_ADDR` | `127.0.0.1:8887` | Normal 查询接口的绑定地址。 |
| `VIP_ADDR` | `127.0.0.1:8888` | VIP 查询接口的绑定地址。 |
| `COLLECTOR_PORT` | `8886` | 采集监听端口，绑定到 `127.0.0.1`。 |
| `ADMIN_PORT` | `8889` | 管理监听端口，绑定到 `127.0.0.1`。 |
| `MAX_QUERY_COUNT` | `100` | 单次 `GET /news` 请求允许的位置范围长度上限，也是 `GET /` 返回的 `page_size`。 |
| `NEWS_CATEGORIES` | `时政,科技,财经,文旅,社会,商业,教育,健康,国际` | 模型分析可用的逗号分隔分类列表；分类名称不能重复。 |
| `DATABASE_PATH` | `./data/newsscope.db` | SQLite 数据库文件路径。 |
| `ANALYSIS_WORKERS` | `3` | 后台分析工作器数量。 |
| `ANALYSIS_MAX_ATTEMPTS` | `5` | 分析任务被标记失败前的最大尝试次数。 |
| `ANALYSIS_TIMEOUT_SECONDS` | `45` | 单次模型分析请求的超时时间。 |
| `ANALYSIS_MAX_BODY_CHARS` | `12000` | 发送给分析模型的正文最大字符数。数据库仍保存采集器提交的完整正文。 |

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

每个请求可包含 1 至 500 条资讯，正文请求大小上限为 10 MiB。JSON 中不接受未定义字段。采集器通过选择端点来决定受众，提交内容中不包含受众字段。采集器只提交来源和资讯内容等原始数据；内部 ID 由 NewsScope 分配，资讯分析在写入后异步进行。

响应示例：

```json
{
  "version": 1,
  "accepted": 1,
  "duplicates": 0,
  "rejected": 0,
  "results": [
    {"status": "accepted", "article_id": 8123}
  ]
}
```

`accepted` 包含新写入的资讯及已确认为重复的资讯；`duplicates` 是 `accepted` 的子集。两者都不表示模型分析已完成。每条结果的状态为 `accepted`、`duplicate` 或 `rejected`；拒收结果可包含原因。判定为重复时，结果可包含已有资讯的 `article_id`。

## 查询接口

所有时间戳均为秒级 Unix 时间戳。Normal 和 VIP 查询监听器提供相同格式的 API，但只返回各自受众的数据。

### 服务信息 — `GET /`

返回当前服务时间、单次列表查询的最大范围长度，以及 `all` 和每个预置分类的当前查询最大值：

```json
{
  "current_time": 1791786600,
  "page_size": 100,
  "categories": [
    {"name": "all", "max_position": 1300},
    {"name": "科技", "max_position": 210}
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

没有原文地址时省略 `original_url`。资讯不存在、已隐藏或尚未完成分析时，返回 HTTP 404 和 `{"error":"not_found"}`。ID 非法时返回 HTTP 400 和 `{"error":"invalid_request"}`。查询发生错误时返回 HTTP 500 和 `{"error":"request_failed"}`。

## 管理接口

### 隐藏资讯 — `POST /hide`

管理监听器绑定本机回环地址，不使用令牌。请仅在本机访问，或在主机及网络层限制访问。

```http
POST /hide
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

## 分析与重复判断

资讯被接收后，后台工作器会调用配置的模型：从预置分类中选择一个分类、生成 2 至 6 个关键词、生成不超过 120 个汉字的中文摘要，并给出 1（最低）至 5（最高）的整数重要性评分。评分依据正文能支持的影响范围、现实影响和紧迫程度，不因夸张措辞而提高。资讯正文按采集器提交内容存储；发送给分析模型的正文长度受配置的字符数限制。

重复判断时，NewsScope 会将每条新资讯的标题和正文与过去 24 小时内、同一受众的候选资讯做模糊相似度比较。标题权重为 30%，正文权重为 70%。随后由模型复核候选是否描述同一具体事件。只有模型确认重复后才丢弃新资讯。Normal 与 VIP 数据之间不会交叉比对。

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
