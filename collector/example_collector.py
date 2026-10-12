#!/usr/bin/env python3
"""Standalone output method example for a NewsScope collector."""

import json
import urllib.request


# Configure this independently in each collector.
INGEST_URL = "http://127.0.0.1:8886/ingest/normal"
REQUEST_TIMEOUT_SECONDS = 20


def send_article(source, title, body, collected_at, original_url=None):
    """POST one article using a Unix-seconds collection timestamp.

    `collected_at` is when this collector obtained the article. The collector
    does not inspect, extract, convert, or otherwise process dates in title or
    body; those values are passed to NewsScope as supplied.
    """
    article = {
        "source": source,
        "title": title,
        "body": body,
        "collected_at": int(collected_at),
    }
    if original_url:
        article["original_url"] = original_url

    # The Go endpoint accepts versioned batches; this method sends one item.
    payload = json.dumps(
        {"version": 1, "articles": [article]}, ensure_ascii=False
    ).encode("utf-8")
    request = urllib.request.Request(
        INGEST_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        result = json.loads(response.read().decode("utf-8"))

    if result.get("rejected", 0):
        raise ValueError(f"NewsScope rejected the article: {result!r}")
    return result


# TODO: In your independent collector, implement source fetching and parsing.
# TODO: Provide the source name, title, body, and int(time.time()) to send_article.
# TODO: Manage source cursors, scheduling, retries, and logs in that collector.
