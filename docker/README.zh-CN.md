# NewsScope 部署方案

本文给出三种部署方式，并说明配置如何生效。

| 方案 | 组成 | 适用场景 |
| --- | --- | --- |
| **A. Docker 核心（推荐）** | 仅 NewsScope | 大多数场景；由任意外部采集器或自建采集器提交资讯 |
| **B. Docker 核心 + RSSHub 集合组（推荐完整版）** | NewsScope + RSSHub + Redis + 采集插件 | 需要直接订阅 RSSHub 路由，不想自己写采集器 |
| **C. 裸机运行** | Go 二进制 | 不使用 Docker 的环境 |

**配置改动一律不需要重新构建镜像。** 见[修改配置](#修改配置)。

## 地址规划

容器内部监听固定为 `8886`/`8887`/`8888`，宿主机通过 `*_BIND` 和 `*_HOST_PORT` 发布端口：

| 用途 | 容器内 | 宿主机默认 | 可改变量 |
| --- | --- | --- | --- |
| 采集入口 | 8886 | `127.0.0.1:8886` | `COLLECTOR_BIND`、`COLLECTOR_HOST_PORT` |
| Normal 查询 | 8887 | `127.0.0.1:8887` | `NORMAL_BIND`、`NORMAL_HOST_PORT` |
| VIP 查询 | 8888 | `127.0.0.1:8888` | `VIP_BIND`、`VIP_HOST_PORT` |
| 管理接口 | 8889 | 不发布 | `ADMIN_ADDR` |
| RSSHub（方案 B） | 1200 | `127.0.0.1:1200` | `RSSHUB_BIND`、`RSSHUB_HOST_PORT` |
| 采集插件面板（方案 B） | 8885 | `127.0.0.1:8885` | `PANEL_BIND`、`PANEL_HOST_PORT` |

管理接口没有令牌认证，默认只绑定容器回环地址，不对宿主机发布。需要时用 `docker compose exec` 访问，或自行在 `compose.yaml` 中增加映射并同时设置 `ADMIN_ADDR=0.0.0.0:8889`：

```sh
docker compose exec newsscope curl -sS -X POST http://127.0.0.1:8889/api/hide \
  -H 'Content-Type: application/json' -d '{"id":8123}'
```

## 方案 A：Docker 核心（推荐）

核心镜像基于 `debian:bookworm-slim`，只包含 NewsScope 二进制和 Python 运行时，没有 RSSHub、Redis 或进程管理器，约 100 MiB。

```sh
git clone https://github.com/PYLinTech/NewsScope.git
cd NewsScope
cp .env.example .env
```

编辑 `.env`，填写 `OPENAI_BASE_URL`、`OPENAI_API_KEY` 和 `OPENAI_MODEL`，然后启动：

```sh
docker compose up -d
```

只有 `newsscope` 一个容器运行。资讯由你自己的采集器提交到
<http://127.0.0.1:8886/ingest/normal> 或 `/ingest/vip`，参考
[`collector/example_collector.py`](../collector/example_collector.py)。

## 方案 B：Docker 核心 + RSSHub 集合组（推荐完整版）

在方案 A 基础上追加一个 `rsshub` profile，会额外启动 `redis`、`rsshub` 和
`rsshub-adapter` 三个容器：

```sh
docker compose --profile rsshub up -d
```

| 容器 | 作用 |
| --- | --- |
| `redis` | RSSHub 缓存，LRU 淘汰，内存上限由 `RSSHUB_REDIS_MAXMEMORY` 控制 |
| `rsshub` | RSSHub 本体，端口 1200 |
| `rsshub-adapter` | 采集插件与管理面板，端口 8885；复用核心镜像，不会与服务端版本不一致 |

首次启动后在浏览器打开 <http://127.0.0.1:8885>，添加订阅。

要点：

- RSSHub 的浏览器路由会额外占用内存，建议为 RSSHub 容器预留至少 2 GiB。不需要
  Chromium 路由时，把 `.env` 中的 `RSSHUB_IMAGE` 换成官方同一提交的普通镜像标签
  `ghcr.io/diygod/rsshub:d55daa06c1340db6fa9cc5f865f627748eb5b346`，可显著减小镜像体积和内存占用。
- 采集插件已预置 NewsScope 地址为 `http://newsscope:8886`（容器网络内的服务名）。
  该值只在配置文件首次创建时写入，之后以面板中保存的值为准。
- **RSSHub 路由地址要填容器网络内的地址**，即 `http://rsshub:1200/<路由>`，而不是
  `http://127.0.0.1:1200/<路由>`。后者指向宿主机，在插件容器内不可达。
- 采集插件的订阅列表、游标和 NewsScope 地址保存在 `newsscope-data` 卷的
  `/data/config/newsscope-rsshub/config.json`，在面板中修改即时生效，无需重启。
- 采集插件自身的监听地址是 `RSSHUB_PANEL_ADDR`（同样是 `host:port` 形式，容器内默认
  `0.0.0.0:8885`）；轮询间隔和并发数由 `RSSHUB_POLL_INTERVAL`、`RSSHUB_POLL_WORKERS`
  控制。裸机运行采集插件时用 `--addr 127.0.0.1:8885`，它只接受回环地址。

## 方案 C：裸机运行

不使用 Docker 时，直接运行 Go 二进制：

```sh
go build -o ~/bin/newsscope ./cmd/newsscope
cd /path/to/config-dir && ~/bin/newsscope
```

服务从**当前工作目录**读取 `.env`，因此应在存放 `.env` 的目录下启动。此时
`NORMAL_ADDR`、`VIP_ADDR`、`COLLECTOR_ADDR` 和 `ADMIN_ADDR` 的默认值都是
`127.0.0.1`，不会对外暴露。

用 launchd 或 systemd 保活并支持重启后自动拉起，例如 launchd 的 `KeepAlive`
配合 `WorkingDirectory` 指向配置目录即可。

## 修改配置

**所有配置都从 `.env` 读取，改完之后执行 `docker compose up -d`。**
该命令会复用现有镜像重建容器，不需要 `docker compose build`。

| 操作 | 命令 | 生效 |
| --- | --- | --- |
| 修改 `.env` | `docker compose up -d` | ✅ 重建容器，几秒完成 |
| 重启但不改配置 | `docker compose restart` | 仅重启进程 |
| 单独重启某个服务 | `docker compose restart newsscope` | 仅重启该容器 |

> ⚠️ **`docker compose restart` 不会重新读取 `.env`。**
> 环境变量在容器创建时就已固定，`restart` 只是把同一个容器停掉再启动，
> 新的 `.env` 不会生效。修改配置后请使用 `docker compose up -d`。

### 常用命令的 profile 行为

`rsshub` profile 下的容器只有带 `--profile` 才会被管理，不带时会留下不一致的状态：

| 命令 | 效果 |
| --- | --- |
| `docker compose up -d` | ✅ 只更新核心容器，**RSSHub 组保持运行**，互不干扰 |
| `docker compose restart newsscope` | ✅ 只重启核心容器 |
| `docker compose down` | ⚠️ 只删除核心容器，**RSSHub 组会被遗留** |
| `docker compose --profile rsshub down` | ✅ 删除全部容器，**卷保留** |
| `docker compose down -v` | ❌ **连同卷一起删除，全部资讯和订阅永久丢失** |

排查问题时记得补上 `--profile rsshub`，否则看不到 RSSHub 和插件的日志与状态。

### 关键配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `COLLECTOR_ADDR` | 裸机 `127.0.0.1:8886`；容器 `0.0.0.0:8886` | 采集监听地址。默认只绑回环，容器内改为全网卡以便直接发布端口，无需端口转发进程 |
| `ADMIN_ADDR` | `127.0.0.1:8889` | 管理监听地址。该接口无令牌认证，默认不对外暴露 |
| `NORMAL_ADDR` / `VIP_ADDR` | 裸机 `127.0.0.1:8887` / `:8888`；容器 `0.0.0.0:8887` / `:8888` | 查询监听地址 |
| `DATABASE_PATH` | 裸机 `./data/newsscope.db`；容器 `/data/newsscope/newsscope.db` | SQLite 文件路径 |
| `NEWS_CATEGORIES` | 见 `.env.example` | 必填 JSON 分类数组 |
| `ANALYSIS_*` | 见 `.env.example` | 分析工作器、超时、正文与摘要长度上限 |

> 这几个变量在 `.env.example` 中默认是**注释状态**。保持注释可以让裸机和容器各自
> 取到正确的默认值；只有当你要自定义时才取消注释。取消注释后该值会同时作用于
> 裸机和容器。

### 从旧的单容器版本迁移

旧版本把 NewsScope、RSSHub、Redis 和采集插件装在同一个镜像里，以 `node` 用户（uid 1000）
运行。新核心镜像改用独立的 `newsscope` 用户（uid 10001），并且只跑 NewsScope 本身。

数据卷的名字没有变（`newsscope_newsscope-data`），SQLite 数据库会直接沿用，不需要迁移。
**但必须先把卷的属主改成新 uid**，否则新容器没有写权限，启动时会报：

```
fatal: unable to open database file (14)
```

升级步骤：

```sh
# 1. 停掉旧容器，保留数据卷
docker compose down

# 2. 把卷交给新镜像的 uid 10001
docker run --rm -u 0 -v newsscope_newsscope-data:/data alpine \
  chown -R 10001:10001 /data

# 3. 按新方式启动（核心，或加上 RSSHub 组）
docker compose up -d
docker compose --profile rsshub up -d
```

> 如果是**全新部署**，跳过这一步：Docker 会用镜像里的属主自动初始化卷。
> 旧版本遗留的 `newsscope-rsshub:local` 镜像不再需要，确认数据无误后可以删除：
> `docker rmi newsscope-rsshub:local`。

### 从旧版本迁移配置变量

`COLLECTOR_PORT` 和 `ADMIN_PORT` 已被 `COLLECTOR_ADDR` 和 `ADMIN_ADDR` 取代。
服务在检测到旧变量时**直接拒绝启动**，并给出替换写法，避免端口被静默改成别的值：

```
COLLECTOR_PORT is no longer supported; remove it or set COLLECTOR_ADDR=127.0.0.1:8886 instead
```

改法是把旧值补上默认主机：

| 旧写法 | 新写法 |
| --- | --- |
| `COLLECTOR_PORT=8886` | 删除，或改为 `COLLECTOR_ADDR=127.0.0.1:8886` |
| `ADMIN_PORT=8889` | 删除，或改为 `ADMIN_ADDR=127.0.0.1:8889` |

容器内的地址保持 `0.0.0.0:<端口>`，由 `compose.yaml` 注入，一般不需要写进 `.env`。

> 宿主机发布端口改用 `*_HOST_PORT`（如 `COLLECTOR_HOST_PORT`），与应用的
> `COLLECTOR_ADDR` 是两套独立的设置：前者只影响 Docker 发布端口，裸机运行会忽略它。

### 什么时候才需要重新构建镜像

只有改动代码或构建参数时：

```sh
docker compose build          # 改了 Go 代码或采集插件
docker compose up -d
```

**更换 `RSSHUB_IMAGE` 不需要重建镜像。** 它现在只是 `rsshub` 服务的镜像标签，
直接拉取并重建该容器即可：

```sh
# 在 .env 里改 RSSHUB_IMAGE 后
docker compose --profile rsshub pull rsshub
docker compose --profile rsshub up -d
```

同理，Redis 换版本也只需 `docker compose --profile rsshub pull redis`。

## 启动状态与日志

```sh
docker compose --profile rsshub ps
docker compose logs -f newsscope
docker compose --profile rsshub logs -f rsshub rsshub-adapter
```

健康检查已内置：核心容器探测两个查询接口，采集插件探测面板状态接口，
Redis 探测 `PING`。采集插件会等待 NewsScope 健康后才启动。

## 持久化

| 卷 | 内容 | 移除后果 |
| --- | --- | --- |
| `newsscope-data` | SQLite 数据库、采集插件订阅与游标 | 永久丢失全部资讯与订阅 |
| `rsshub-config` | RSSHub 自身配置 | 丢失 RSSHub 设置 |
| `rsshub-redis` | Redis 缓存 | 仅丢失缓存，会自动重建 |

重建镜像或容器不会删除数据。SQLite 位于 `newsscope-data` 卷内，升级镜像后原有数据
直接可用。删除资讯请使用管理接口 `POST /api/hide`，不要直接删除数据库文件。

## 网络与安全

所有宿主机端口默认只绑定 `127.0.0.1`。若需从其他机器访问，请在 `.env` 中把对应的
`*_BIND` 改为服务器网卡地址或 `0.0.0.0`，并用防火墙限制来源：

- **采集入口没有认证**，只有可信采集器需要从外部提交时才应暴露 `COLLECTOR_BIND`。
- **管理接口没有认证**，不要暴露 `ADMIN_ADDR`；日常运维用 `docker compose exec` 即可。
- 查询接口可以对外提供，建议放在反向代理之后并启用 TLS。

## 许可证

NewsScope 自身采用 MIT 许可证。核心镜像基于 `debian:bookworm-slim`，不包含 RSSHub
的任何代码，因此镜像内不再附带 RSSHub 的许可证文本。

RSSHub 使用 AGPL-3.0 许可证，其许可证文本由官方镜像自身提供，仓库内也保留了一份副本
[`docker/RSSHUB-LICENSE`](RSSHUB-LICENSE)。启用 `rsshub` 集合组即表示同时运行 AGPL-3.0
许可的 RSSHub，请自行确认合规义务。

官方部署说明和镜像标签见 [RSSHub Docker 部署文档](https://docs.rsshub.app/en/install/)。
