# NewsScope + RSSHub 单容器部署

该镜像将官方 RSSHub Chromium 构建、Redis 缓存、NewsScope Go 服务和“RSSHub 适配插件”打包在同一个容器中。RSSHub 固定到官方构建提交 `d55daa06c1340db6fa9cc5f865f627748eb5b346`；NewsScope 和插件使用当前仓库版本构建。

## 构建和启动

先克隆仓库并进入目录：

```sh
git clone https://github.com/PYLinTech/NewsScope.git
cd NewsScope
```

复制配置模板：

```sh
cp .env.example .env
```

编辑 `.env`，填写 `OPENAI_BASE_URL`、`OPENAI_API_KEY` 和 `OPENAI_MODEL`，然后构建并启动：

```sh
docker compose up -d --build
```

查看启动状态和日志：

```sh
docker compose ps
docker compose logs -f newsscope
```

RSSHub 的浏览器路由会额外占用内存；建议为容器预留至少 2 GiB 内存。若不使用需要 Chromium 的 RSSHub 路由，可通过构建参数把 `RSSHUB_IMAGE` 换成官方同一提交的普通镜像标签 `ghcr.io/diygod/rsshub:d55daa06c1340db6fa9cc5f865f627748eb5b346`，可减小镜像体积和运行内存。

## 地址

| 用途 | 默认地址 |
| --- | --- |
| RSSHub | <http://127.0.0.1:1200> |
| RSSHub 适配插件 | <http://127.0.0.1:8885> |
| 外部采集入口 | <http://127.0.0.1:8886/ingest/normal>、`/ingest/vip` |
| Normal 查询接口 | <http://127.0.0.1:8887> |
| VIP 查询接口 | <http://127.0.0.1:8888> |

插件中的 NewsScope 地址默认是 `http://127.0.0.1:8886`。订阅 URL 由容器内的插件请求；使用 RSSHub 自身路由时填写 `http://127.0.0.1:1200/<路由>`。

所有宿主机端口默认只绑定 `127.0.0.1`。如果需要从其他机器访问查询接口或 RSSHub，请在 `.env` 中将对应的 `*_BIND` 改为服务器指定网卡地址或 `0.0.0.0`，并用防火墙限制访问。采集入口没有认证；只有可信采集器需要从外部提交时才应暴露 `COLLECTOR_BIND`。

## 持久化和配置

Compose 创建 `newsscope-data` 卷，保存 SQLite 数据库、RSSHub 订阅、采集去重游标和 Redis 缓存。重建镜像或容器不会删除数据；移除该卷会永久删除这些数据。

RSSHub 使用 Redis 缓存、5 分钟路由缓存、1 小时内容缓存和 256 MiB Redis 内存上限；Redis 采用 LRU 淘汰，并启用每秒追加文件持久化。可通过 `.env` 中的 `RSSHUB_CACHE_EXPIRE`、`RSSHUB_CONTENT_CACHE_EXPIRE` 和 `RSSHUB_REDIS_MAXMEMORY` 调整。RSSHub 路由依赖的访问令牌等配置也可放进 `.env`，它会传入容器进程环境。

RSSHub 适配插件是本机管理工具，不提供用户认证；Compose 默认只将它映射到宿主机回环接口。RSSHub 自身依赖于第三方网站可用性、站点反爬策略和路由维护状态，镜像配置无法保证每条路由持续可用。官方 RSSHub 使用 AGPL-3.0 许可证，许可证文本随组合镜像安装在 `/usr/share/licenses/RSSHub/AGPL-3.0.txt`。

## 更新 RSSHub

RSSHub 镜像使用官方 commit SHA 标签，确保同一仓库版本每次构建都采用同一 RSSHub 代码。更新时，在 `.env` 中将 `RSSHUB_IMAGE` 改成 RSSHub 官方仓库中已发布的 SHA 标签，再执行：

```sh
docker compose build --no-cache
docker compose up -d
```

官方部署说明和镜像标签见 [RSSHub Docker 部署文档](https://docs.rsshub.app/en/install/)。
