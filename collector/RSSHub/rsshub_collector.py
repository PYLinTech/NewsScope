#!/usr/bin/env python3
"""Local RSSHub feed manager and independent NewsScope collector.

The manager listens on loopback only. Feed subscriptions and the NewsScope
collector base URL are stored in a small JSON file under the user's config
directory; no third-party Python packages are required.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import errno
import fcntl
import hashlib
import html
from html.parser import HTMLParser
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.error import HTTPError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
import uuid
import xml.etree.ElementTree as ET


DEFAULT_PANEL_HOST = "127.0.0.1"
DEFAULT_PANEL_PORT = 8885
DEFAULT_NEWSCOPE_URL = "http://127.0.0.1:8886"
DEFAULT_INTERVAL = 10
DEFAULT_WORKERS = 4
REQUEST_TIMEOUT_SECONDS = 25
MAX_FEED_BYTES = 12 * 1024 * 1024
MAX_BATCH_ITEMS = 500
MAX_BATCH_BYTES = 9 * 1024 * 1024
MAX_ARTICLE_BYTES = 2 * 1024 * 1024
MAX_CONCURRENT_SUBMISSIONS = 2
SUBMISSION_SLOTS = threading.BoundedSemaphore(MAX_CONCURRENT_SUBMISSIONS)
SEEN_LIMIT = 1000
USER_AGENT = "NewsScope-RSSHub-Collector/1.0 (+RSS/Atom reader)"
BLOCK_TAGS = {"br", "p", "div", "li", "tr", "h1", "h2", "h3"}


class ReadWriteLock:
    """Allow concurrent feed submissions while making endpoint changes exclusive."""

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.readers = 0
        self.writer = False
        self.waiting_writers = 0

    @contextmanager
    def read(self):
        with self.condition:
            while self.writer or self.waiting_writers:
                self.condition.wait()
            self.readers += 1
        try:
            yield
        finally:
            with self.condition:
                self.readers -= 1
                if self.readers == 0:
                    self.condition.notify_all()

    @contextmanager
    def write(self):
        with self.condition:
            self.waiting_writers += 1
            try:
                while self.writer or self.readers:
                    self.condition.wait()
                self.writer = True
            finally:
                self.waiting_writers -= 1
                self.condition.notify_all()
        try:
            yield
        finally:
            with self.condition:
                self.writer = False
                self.condition.notify_all()


def default_config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME")
    if base:
        return Path(base).expanduser() / "newsscope-rsshub" / "config.json"
    return Path.home() / ".config" / "newsscope-rsshub" / "config.json"


def validate_http_url(raw: str, *, allow_path: bool = True) -> str:
    value = raw.strip()
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("请输入完整的 http:// 或 https:// 地址")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("地址不能包含账号、密码或片段")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("地址端口无效") from exc
    if not allow_path and (parsed.path not in ("", "/") or parsed.query):
        raise ValueError("NewsScope 地址只填写协议、主机和端口")
    return value.rstrip("/") if not allow_path else value


def validate_loopback_host(host: str) -> str:
    value = host.strip()
    if value == "0.0.0.0" and os.environ.get("RSSHUB_PANEL_CONTAINER_MODE") == "1":
        return value
    if value.lower() == "localhost":
        return value
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError("RSSHub 适配插件只能绑定 IPv4 本机回环地址或容器模式下的 0.0.0.0") from exc
    if address.version != 4 or not address.is_loopback:
        raise ValueError("RSSHub 适配插件只能绑定本机回环地址")
    return value


class TextOnlyHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.ignored_depth = 0

    def handle_data(self, data: str) -> None:
        if data and not self.ignored_depth:
            self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() in {"script", "style"}:
            self.ignored_depth += 1
            return
        if tag.lower() in BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self.ignored_depth:
            self.ignored_depth -= 1
            return
        if tag.lower() in BLOCK_TAGS:
            self.parts.append("\n")


def plain_text(value: str) -> str:
    parser = TextOnlyHTML()
    try:
        parser.feed(value)
        parser.close()
        text = "".join(parser.parts)
    except Exception:
        text = re.sub(r"<[^>]*>", " ", html.unescape(value))
    text = text.replace("\xa0", " ")
    text = re.sub(r"[\t\r\f\v ]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def element_content(element: ET.Element) -> str:
    parts: list[str] = []

    def visit(node: ET.Element) -> None:
        block = local_name(node.tag) in BLOCK_TAGS
        if block:
            parts.append("\n")
        if node.text:
            parts.append(node.text)
        for child in list(node):
            visit(child)
            if child.tail:
                parts.append(child.tail)
        if block:
            parts.append("\n")

    visit(element)
    return "".join(parts)


def first_child_text(element: ET.Element, names: set[str]) -> str:
    for child in list(element):
        if local_name(child.tag) in names:
            return plain_text(element_content(child))
    return ""


def feed_link(item: ET.Element) -> str:
    for child in list(item):
        if local_name(child.tag) == "link":
            href = child.attrib.get("href", "").strip()
            if href:
                rel = child.attrib.get("rel", "alternate").lower()
                if rel == "alternate":
                    return href
                continue
            if child.text and child.text.strip():
                return child.text.strip()
    return ""


def safe_article_url(raw: str, feed_url: str) -> str:
    try:
        value = urljoin(feed_url, raw.strip())
        parsed = urlparse(value)
        parsed.port
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return ""
    return value


def stable_entry_key(item_id: str, link: str, title: str, published: str) -> str:
    value = item_id or link or f"{title}\n{published}"
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def publication_timestamp(value: str, fallback: int) -> int:
    text = value.strip()
    if not text:
        return fallback
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            timestamp = float(text)
            if timestamp > 253402300799:
                timestamp /= 1000
            result = int(timestamp)
            return result if 0 < result <= 253402300799 else fallback
        except (ValueError, OverflowError):
            return fallback
    try:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        result = int(parsed.timestamp())
        return result if 0 < result <= 253402300799 else fallback
    except (TypeError, ValueError, OverflowError, OSError):
        return fallback


def extract_json_feed(raw: bytes) -> tuple[str, list[dict[str, str]]]:
    try:
        document = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"RSSHub JSON 无法解析：{exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("RSSHub JSON 格式无法识别")

    feed_title = plain_text(str(document.get("title") or ""))
    parsed: list[dict[str, str]] = []
    items = document.get("items")
    if isinstance(items, list):
        for item in items[:5000]:
            if not isinstance(item, dict):
                continue
            title = plain_text(str(item.get("title") or ""))
            if not title:
                continue
            link = str(item.get("url") or item.get("external_url") or "").strip()
            published = str(item.get("date_published") or item.get("date_modified") or "")
            body = next(
                (
                    text
                    for field in ("content_text", "content_html", "summary")
                    if isinstance(item.get(field), str)
                    if (text := plain_text(item[field]))
                ),
                title,
            )
            item_id = item.get("id")
            parsed.append({
                "key": stable_entry_key(str(item_id or ""), link, title, published),
                "title": title,
                "body": body,
                "link": link,
                "published": published,
            })
        return feed_title, parsed

    # RSSHub's legacy `format=rss3`/`format=ums` output nests entries in
    # data[] and carries article fields in actions[].metadata.
    data = document.get("data")
    if isinstance(data, list):
        for entry in data[:5000]:
            if not isinstance(entry, dict):
                continue
            actions = entry.get("actions")
            action = actions[0] if isinstance(actions, list) and actions else {}
            metadata = action.get("metadata", {}) if isinstance(action, dict) else {}
            if not isinstance(metadata, dict):
                metadata = {}
            title = plain_text(str(metadata.get("title") or ""))
            if not title:
                continue
            related_urls = action.get("related_urls") if isinstance(action, dict) else None
            link = str((related_urls[0] if isinstance(related_urls, list) and related_urls else "") or entry.get("id") or "").strip()
            published = str(metadata.get("pub_date") or entry.get("timestamp") or "")
            body = plain_text(str(metadata.get("description") or "")) or title
            parsed.append({
                "key": stable_entry_key(str(entry.get("id") or ""), link, title, published),
                "title": title,
                "body": body,
                "link": link,
                "published": published,
            })
        return feed_title, parsed

    raise ValueError("JSON 响应不是 JSON Feed 或 RSSHub RSS3 Feed")


def extract_feed(xml_bytes: bytes) -> tuple[str, list[dict[str, str]]]:
    if xml_bytes.lstrip(b"\xef\xbb\xbf\x00\t\r\n ").startswith((b"{", b"[")):
        return extract_json_feed(xml_bytes)
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"RSS/Atom XML 无法解析：{exc}") from exc

    root_kind = local_name(root.tag)
    if root_kind == "feed":
        container = root
        entries = [node for node in list(root) if local_name(node.tag) == "entry"]
    else:
        container = next((node for node in list(root) if local_name(node.tag) == "channel"), root)
        item_container = root if root_kind == "rdf" else container
        entries = [node for node in list(item_container) if local_name(node.tag) == "item"]

    feed_title = first_child_text(container, {"title"})
    parsed: list[dict[str, str]] = []
    for item in entries[:5000]:
        title = first_child_text(item, {"title"})
        link = feed_link(item)
        item_id = first_child_text(item, {"guid", "id"}).strip()
        published = first_child_text(item, {"pubdate", "published", "updated", "date"}).strip()
        body = next(
            (
                value
                for name in ("encoded", "content", "description", "summary")
                if (value := first_child_text(item, {name}))
            ),
            title,
        )
        if not title:
            continue
        key = stable_entry_key(item_id, link, title, published)
        parsed.append({"key": key, "title": title, "body": body, "link": link, "published": published})
    return feed_title, parsed


class ConfigStore:
    def __init__(self, path: Path, default_poll_interval: int = DEFAULT_INTERVAL) -> None:
        self.path = path
        self.default_poll_interval = default_poll_interval
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.target_lock = ReadWriteLock()
        self.subscription_locks_guard = threading.Lock()
        self.subscription_locks: dict[str, threading.RLock] = {}
        self.instance_lock_file = self.path.with_suffix(self.path.suffix + ".lock").open("a+")
        os.chmod(self.instance_lock_file.name, 0o600)
        try:
            fcntl.flock(self.instance_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.instance_lock_file.close()
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                raise RuntimeError(f"已有采集器正在使用配置文件：{self.path}") from exc
            raise
        self.data = self._load()

    def _default(self) -> dict:
        return {
            "newsscope_url": DEFAULT_NEWSCOPE_URL,
            "poll_interval": self.default_poll_interval,
            "subscriptions": [],
        }

    def _load(self) -> dict:
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            data = self._default()
            self._write(data)
            return data
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"配置文件无法读取：{self.path}：{exc}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"配置文件结构无效：{self.path}")
        subscriptions = data.get("subscriptions", [])
        if not isinstance(subscriptions, list):
            raise RuntimeError(f"配置文件结构无效：{self.path}")
        data["subscriptions"] = subscriptions
        try:
            poll_interval = data.get("poll_interval", self.default_poll_interval)
            if type(poll_interval) is not int or poll_interval < 10:
                raise ValueError("逐行扫描间隔必须是至少 10 秒的整数")
            data["poll_interval"] = poll_interval
            data["newsscope_url"] = validate_http_url(
                str(data.get("newsscope_url", DEFAULT_NEWSCOPE_URL)), allow_path=False
            )
            subscription_ids = set()
            for sub in data["subscriptions"]:
                if not isinstance(sub, dict) or not re.fullmatch(r"[a-f0-9]{32}", str(sub.get("id", ""))):
                    raise ValueError("订阅编号无效")
                if sub["id"] in subscription_ids:
                    raise ValueError("订阅编号重复")
                subscription_ids.add(sub["id"])
                sub["url"] = validate_http_url(str(sub.get("url", "")))
                sub["name"] = str(sub.get("name", ""))[:200]
                sub["audience"] = str(sub.get("audience", "normal")).lower()
                if sub["audience"] not in {"normal", "vip"}:
                    raise ValueError("订阅受众无效")
                seen = sub.get("seen", [])
                if not isinstance(seen, list) or any(not isinstance(key, str) for key in seen):
                    raise ValueError("订阅游标无效")
                sub["seen"] = seen[-SEEN_LIMIT:]
                sub["revision"] = max(1, int(sub.get("revision", 1)))
        except (ValueError, TypeError) as exc:
            raise RuntimeError(f"配置文件结构无效：{self.path}：{exc}") from exc
        return data

    def _write(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(self.path)

    def snapshot(self) -> dict:
        with self.lock:
            return json.loads(json.dumps(self.data, ensure_ascii=False))

    def subscription_lock(self, sub_id: str) -> threading.RLock:
        with self.subscription_locks_guard:
            return self.subscription_locks.setdefault(sub_id, threading.RLock())

    def is_current(self, sub_id: str, revision: int, newsscope_url: str) -> bool:
        with self.lock:
            sub = next((s for s in self.data["subscriptions"] if s.get("id") == sub_id), None)
            return (
                sub is not None
                and int(sub.get("revision", 1)) == revision
                and self.data.get("newsscope_url") == newsscope_url
            )

    def save_settings(self, values: dict) -> dict:
        newsscope_url = None
        poll_interval = None
        if "newsscope_url" in values:
            newsscope_url = validate_http_url(str(values["newsscope_url"]), allow_path=False)
        if "poll_interval" in values:
            poll_interval = values["poll_interval"]
            if type(poll_interval) is not int or poll_interval < 10:
                raise ValueError("逐行扫描间隔必须是至少 10 秒的整数")
        if newsscope_url is None and poll_interval is None:
            raise ValueError("没有可保存的配置")

        target_guard = self.target_lock.write() if newsscope_url is not None else nullcontext()
        with target_guard:
            with self.lock:
                previous_url = self.data["newsscope_url"]
                previous_interval = self.data["poll_interval"]
                if newsscope_url is not None:
                    self.data["newsscope_url"] = newsscope_url
                if poll_interval is not None:
                    self.data["poll_interval"] = poll_interval
                try:
                    self._write(self.data)
                except OSError:
                    self.data["newsscope_url"] = previous_url
                    self.data["poll_interval"] = previous_interval
                    raise
                return {
                    "newsscope_url": self.data["newsscope_url"],
                    "poll_interval": self.data["poll_interval"],
                }

    def poll_interval(self) -> int:
        with self.lock:
            return self.data["poll_interval"]

    def save_subscription(self, values: dict) -> dict:
        name = str(values.get("name", "")).strip()[:200]
        url = validate_http_url(str(values.get("url", "")))
        audience = str(values.get("audience", "normal")).lower()
        if audience not in {"normal", "vip"}:
            raise ValueError("受众请选择 Normal 或 VIP")
        sub_id = str(values.get("id", "")).strip()
        if sub_id:
            with self.lock:
                if not any(s.get("id") == sub_id for s in self.data["subscriptions"]):
                    raise ValueError("订阅不存在，请刷新面板后重试")
        subscription_guard = self.subscription_lock(sub_id) if sub_id else nullcontext()
        with subscription_guard:
            with self.lock:
                existing = next((s for s in self.data["subscriptions"] if s.get("id") == sub_id), None)
                if existing is None:
                    if sub_id:
                        raise ValueError("订阅不存在，请刷新面板后重试")
                    existing = {"id": uuid.uuid4().hex, "seen": [], "revision": 0}
                    self.data["subscriptions"].append(existing)
                changed_feed = existing.get("url") != url
                changed = changed_feed or existing.get("audience") != audience or existing.get("name", "") != name
                existing.update({"name": name, "url": url, "audience": audience})
                if changed_feed:
                    existing["seen"] = []
                if changed:
                    existing["revision"] = int(existing.get("revision", 1)) + 1
                self._write(self.data)
                return {"id": existing["id"], "name": name, "url": url, "audience": audience}

    def delete_subscription(self, sub_id: str) -> bool:
        with self.lock:
            if not any(s.get("id") == sub_id for s in self.data["subscriptions"]):
                return False
        with self.subscription_lock(sub_id):
            with self.lock:
                before = len(self.data["subscriptions"])
                self.data["subscriptions"] = [s for s in self.data["subscriptions"] if s.get("id") != sub_id]
                if len(self.data["subscriptions"]) == before:
                    return False
                self._write(self.data)
                return True

    def mark_seen(self, sub_id: str, keys: list[str], expected_revision: int, expected_newsscope_url: str) -> bool:
        with self.lock:
            sub = next((s for s in self.data["subscriptions"] if s.get("id") == sub_id), None)
            if (
                sub is None
                or int(sub.get("revision", 1)) != expected_revision
                or self.data.get("newsscope_url") != expected_newsscope_url
            ):
                return False
            if not keys:
                return True
            seen = list(sub.get("seen", []))
            seen_set = set(seen)
            for key in keys:
                if key not in seen_set:
                    seen.append(key)
                    seen_set.add(key)
            sub["seen"] = seen[-SEEN_LIMIT:]
            self._write(self.data)
            return True


class RuntimeStatus:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.values: dict[str, dict] = {}

    def set(self, sub_id: str, **values) -> None:
        with self.lock:
            current = self.values.setdefault(sub_id, {})
            current.update(values)

    def snapshot(self) -> dict[str, dict]:
        with self.lock:
            return json.loads(json.dumps(self.values))


def http_request(url: str, *, data: Optional[bytes] = None) -> bytes:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, application/feed+json, application/json, application/xml, text/xml, */*"}
    method = "GET"
    if data is not None:
        method = "POST"
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)
    last_error: Optional[Exception] = None
    if data is not None:
        SUBMISSION_SLOTS.acquire()
    try:
        for attempt in range(4):
            try:
                with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    body = response.read(MAX_FEED_BYTES + 1)
                    if len(body) > MAX_FEED_BYTES:
                        raise ValueError("响应超过 12 MiB 限制")
                    return body
            except HTTPError as exc:
                detail = exc.read(2048).decode("utf-8", errors="replace").strip()
                last_error = RuntimeError(f"HTTP {exc.code}: {detail or exc.reason}")
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 3:
                    break
            except OSError as exc:
                last_error = RuntimeError(str(exc))
                if attempt == 3:
                    break
            time.sleep(min(2 ** attempt, 8))
    finally:
        if data is not None:
            SUBMISSION_SLOTS.release()
    raise RuntimeError(str(last_error or "请求失败"))


def make_batches(articles: list[dict]) -> tuple[list[list[tuple[int, dict]]], int]:
    batches: list[list[tuple[int, dict]]] = []
    current: list[tuple[int, dict]] = []
    size = len(b'{"version":1,"articles":[]}')
    oversized = 0
    for index, article in enumerate(articles):
        encoded = json.dumps(article, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_ARTICLE_BYTES:
            oversized += 1
            continue
        if current and (len(current) >= MAX_BATCH_ITEMS or size + len(encoded) + 1 > MAX_BATCH_BYTES):
            batches.append(current)
            current = []
            size = len(b'{"version":1,"articles":[]}')
        current.append((index, article))
        size += len(encoded) + 1
    if current:
        batches.append(current)
    return batches, oversized


def submit_articles(base_url: str, audience: str, articles: list[dict]) -> tuple[list[int], int, int, int, list[str]]:
    path = "/ingest/vip" if audience == "vip" else "/ingest/normal"
    endpoint = base_url.rstrip("/") + path
    accepted_indices: list[int] = []
    accepted_count = 0
    duplicate_count = 0
    batches, rejected = make_batches(articles)
    errors: list[str] = []
    for indexed_batch in batches:
        batch = [article for _, article in indexed_batch]
        payload = json.dumps({"version": 1, "articles": batch}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        try:
            result = json.loads(http_request(endpoint, data=payload))
            item_results = result.get("results", []) if isinstance(result, dict) else []
            if not isinstance(item_results, list) or len(item_results) != len(batch):
                raise RuntimeError("NewsScope 返回了无法识别的提交结果")
            if any(not isinstance(item, dict) for item in item_results):
                raise RuntimeError("NewsScope 返回了无法识别的提交结果")
        except (RuntimeError, ValueError, TypeError) as exc:
            rejected += len(batch)
            errors.append(str(exc)[:200])
            continue
        for index, item in enumerate(item_results):
            if item.get("status") == "accepted":
                accepted_indices.append(indexed_batch[index][0])
                accepted_count += 1
            elif item.get("status") == "duplicate":
                accepted_indices.append(indexed_batch[index][0])
                duplicate_count += 1
            else:
                rejected += 1
    return accepted_indices, accepted_count, duplicate_count, rejected, errors


def poll_subscription(sub: dict, newscope_url: str, config: Optional[ConfigStore] = None) -> dict:
    sub_id = sub["id"]
    result = {"id": sub_id, "checked_at": int(time.time()), "ok": False}
    try:
        raw = http_request(sub["url"])
        feed_title, entries = extract_feed(raw)
        previous = set(sub.get("seen", []))
        pending = [entry for entry in entries if entry["key"] not in previous]
        source = sub.get("name") or feed_title or urlparse(sub["url"]).hostname or "RSSHub"
        articles: list[dict] = []
        keys: list[str] = []
        for entry in pending:
            published_at = publication_timestamp(entry.get("published", ""), result["checked_at"])
            article = {
                "source": source,
                "title": entry["title"],
                "body": entry["body"],
                "collected_at": published_at,
            }
            original_url = safe_article_url(entry["link"], sub["url"])
            if original_url:
                article["original_url"] = original_url
            articles.append(article)
            keys.append(entry["key"])
        revision = int(sub.get("revision", 1))
        if config is None:
            successful_indices, accepted_count, duplicate_count, rejected, submit_errors = submit_articles(
                newscope_url, sub.get("audience", "normal"), articles
            )
            seen_keys = [keys[index] for index in successful_indices]
        else:
            with config.target_lock.read():
                with config.subscription_lock(sub_id):
                    if not config.is_current(sub_id, revision, newscope_url):
                        result["stale"] = True
                        return result
                    successful_indices, accepted_count, duplicate_count, rejected, submit_errors = submit_articles(
                        newscope_url, sub.get("audience", "normal"), articles
                    )
                    seen_keys = [keys[index] for index in successful_indices]
                    if not config.mark_seen(sub_id, seen_keys, revision, newscope_url):
                        result["stale"] = True
                        return result
        result.update({
            "ok": rejected == 0,
            "feed_title": feed_title,
            "accepted": accepted_count,
            "duplicates": duplicate_count,
            "rejected": rejected,
            "error": "" if rejected == 0 else (
                f"有 {rejected} 条资讯未能确认入队或被服务拒收"
                + (f"；提交错误：{'；'.join(submit_errors)}" if submit_errors else "")
            )[:500],
        })
        if config is None:
            result["seen_keys"] = seen_keys
    except Exception as exc:
        result["error"] = str(exc)[:500]
    return result


class Poller(threading.Thread):
    def __init__(self, config: ConfigStore, status: RuntimeStatus, workers: int) -> None:
        super().__init__(name="rsshub-poller", daemon=True)
        self.config = config
        self.status = status
        self.workers = workers
        self.stop_event = threading.Event()

    def run(self) -> None:
        in_flight = {}
        last_subscription_id = None
        interval = self.config.poll_interval()
        next_dispatch = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="rsshub-feed") as pool:
            while not self.stop_event.is_set():
                completed = [future for future in in_flight if future.done()]
                for future in completed:
                    sub = in_flight.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {"id": sub["id"], "checked_at": int(time.time()), "ok": False, "error": str(exc)[:500]}
                    if result.pop("stale", False):
                        continue
                    result.pop("seen_keys", None)
                    previous = self.status.snapshot().get(sub["id"], {})
                    if result.get("ok"):
                        result["last_success_at"] = result["checked_at"]
                    else:
                        result["last_success_at"] = previous.get("last_success_at")
                    self.status.set(sub["id"], **result)

                now = time.monotonic()
                configured_interval = self.config.poll_interval()
                if configured_interval != interval:
                    interval = configured_interval
                    next_dispatch = now + interval
                if now >= next_dispatch:
                    snapshot = self.config.snapshot()
                    subscriptions = snapshot.get("subscriptions", [])
                    if subscriptions and len(in_flight) < self.workers:
                        ids = [sub["id"] for sub in subscriptions]
                        start = (ids.index(last_subscription_id) + 1) % len(ids) if last_subscription_id in ids else 0
                        in_flight_ids = {sub["id"] for sub in in_flight.values()}
                        for offset in range(len(subscriptions)):
                            sub = subscriptions[(start + offset) % len(subscriptions)]
                            if sub["id"] in in_flight_ids:
                                continue
                            future = pool.submit(poll_subscription, sub, snapshot["newsscope_url"], self.config)
                            in_flight[future] = sub
                            last_subscription_id = sub["id"]
                            break
                    next_dispatch = now + interval

                delay = max(0.0, next_dispatch - time.monotonic())
                if in_flight:
                    wait(in_flight, timeout=min(delay, 0.5), return_when=FIRST_COMPLETED)
                else:
                    self.stop_event.wait(min(delay, 0.5))

    def stop(self) -> None:
        self.stop_event.set()


def json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def make_handler(config: ConfigStore, status: RuntimeStatus, html_path: Path):
    class Handler(BaseHTTPRequestHandler):
        server_version = "NewsScopeRSSHubAdapter/1.0"

        def log_message(self, fmt: str, *args) -> None:
            sys.stderr.write("[rsshub-adapter] " + (fmt % args) + "\n")

        def _send(self, status_code: int, content_type: str, body: bytes) -> None:
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'self' 'unsafe-inline'; script-src 'unsafe-inline'; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status_code: int, value: object) -> None:
            self._send(status_code, "application/json; charset=utf-8", json_bytes(value))

        def _local_request(self) -> bool:
            container_mode = os.environ.get("RSSHUB_PANEL_CONTAINER_MODE") == "1"
            try:
                host_url = urlparse("//" + self.headers.get("Host", ""))
                host = (host_url.hostname or "").lower()
                host_port = host_url.port or 80
            except ValueError:
                return False
            if host not in {"localhost", "127.0.0.1"}:
                return False
            if not container_mode and host_port != self.server.server_port:
                return False
            origin = self.headers.get("Origin")
            if origin:
                try:
                    parsed = urlparse(origin)
                    origin_host = (parsed.hostname or "").lower()
                    origin_port = parsed.port or 80
                except ValueError:
                    return False
                if (
                    parsed.scheme != "http"
                    or origin_host != host
                    or origin_port != host_port
                ):
                    return False
            return True

        def _read_json(self) -> dict:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValueError("请求长度无效") from exc
            if length < 1 or length > 64 * 1024:
                raise ValueError("请求内容为空或超过限制")
            try:
                value = json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError("JSON 格式无效") from exc
            if not isinstance(value, dict):
                raise ValueError("请求内容必须是对象")
            return value

        def do_GET(self) -> None:
            if not self._local_request():
                self._json(403, {"error": "仅允许本机访问"})
                return
            if self.path == "/api/state":
                snapshot = config.snapshot()
                runtime = status.snapshot()
                subscriptions = []
                for sub in snapshot["subscriptions"]:
                    row = {k: sub.get(k) for k in ("id", "name", "url", "audience")}
                    row.update(runtime.get(sub["id"], {}))
                    subscriptions.append(row)
                self._json(200, {
                    "newsscope_url": snapshot["newsscope_url"],
                    "poll_interval": snapshot["poll_interval"],
                    "subscriptions": subscriptions,
                })
                return
            asset_path = urlparse(self.path).path
            if asset_path in {
                "/assets/remixicon/remixicon.css",
                "/assets/remixicon/remixicon.woff2",
            }:
                relative_path = asset_path.lstrip("/")
                content_type = "text/css; charset=utf-8" if asset_path.endswith(".css") else "font/woff2"
                try:
                    body = (html_path.parent / relative_path).read_bytes()
                except OSError as exc:
                    self._json(404, {"error": f"资源无法读取：{exc}"})
                    return
                self._send(200, content_type, body)
                return
            if asset_path not in ("/", "/index.html"):
                self._json(404, {"error": "not_found"})
                return
            try:
                body = html_path.read_bytes()
            except OSError as exc:
                self._json(500, {"error": str(exc)})
                return
            self._send(200, "text/html; charset=utf-8", body)

        def do_POST(self) -> None:
            if not self._local_request():
                self._json(403, {"error": "仅允许本机访问"})
                return
            try:
                payload = self._read_json()
                if self.path == "/api/config":
                    saved = config.save_settings(payload)
                    self._json(200, {"ok": True, **saved})
                elif self.path == "/api/subscriptions":
                    saved = config.save_subscription(payload)
                    self._json(200, {"ok": True, "subscription": saved})
                else:
                    self._json(404, {"error": "not_found"})
            except ValueError as exc:
                self._json(400, {"error": str(exc)})
            except OSError as exc:
                self._json(500, {"error": f"配置保存失败：{exc}"})

        def do_DELETE(self) -> None:
            if not self._local_request():
                self._json(403, {"error": "仅允许本机访问"})
                return
            prefix = "/api/subscriptions/"
            if not self.path.startswith(prefix):
                self._json(404, {"error": "not_found"})
                return
            sub_id = self.path[len(prefix):]
            if not re.fullmatch(r"[a-f0-9]{32}", sub_id):
                self._json(400, {"error": "订阅编号无效"})
                return
            try:
                if config.delete_subscription(sub_id):
                    self._json(200, {"ok": True})
                else:
                    self._json(404, {"error": "订阅不存在"})
            except OSError as exc:
                self._json(500, {"error": f"配置保存失败：{exc}"})

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(description="RSSHub adapter for NewsScope")
    parser.add_argument("--host", default=os.environ.get("RSSHUB_PANEL_HOST", DEFAULT_PANEL_HOST), help="监听地址；容器模式支持 0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("RSSHUB_PANEL_PORT", DEFAULT_PANEL_PORT)), help="适配插件端口，默认 8885")
    parser.add_argument("--interval", type=int, default=int(os.environ.get("RSSHUB_POLL_INTERVAL", DEFAULT_INTERVAL)), help="启动下一个订阅检查的间隔秒数，默认 10")
    parser.add_argument("--workers", type=int, default=int(os.environ.get("RSSHUB_POLL_WORKERS", DEFAULT_WORKERS)), help="并发订阅数，默认 4")
    parser.add_argument("--config", type=Path, default=Path(os.environ.get("RSSHUB_CONFIG", default_config_path())), help="配置文件路径")
    args = parser.parse_args()
    try:
        host = validate_loopback_host(args.host)
    except ValueError as exc:
        parser.error(str(exc))
    if not (1 <= args.port <= 65535) or args.interval < 10 or not (1 <= args.workers <= 32):
        parser.error("端口、轮询间隔或并发数超出有效范围")

    try:
        config = ConfigStore(args.config.expanduser(), args.interval)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 2
    status = RuntimeStatus()
    poller = Poller(config, status, args.workers)
    server = ThreadingHTTPServer((host, args.port), make_handler(config, status, Path(__file__).with_name("index.html")))
    server.daemon_threads = True
    poller.start()
    print(f"RSSHub 适配插件：http://{host}:{args.port}")
    print(f"配置文件：{args.config.expanduser()}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        poller.stop()
        server.shutdown()
        server.server_close()
        poller.join(timeout=3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
