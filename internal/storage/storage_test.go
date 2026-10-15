package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestListUsesCategoryScopedPositions(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	articles := []struct {
		category string
		title    string
		at       int64
	}{
		{"科技", "科技新消息", 300},
		{"财经", "财经新消息", 200},
		{"科技", "科技旧消息", 100},
	}
	for _, item := range articles {
		result, err := store.Ingest(ctx, ArticleInput{Source: "测试", Title: item.title, Body: "正文", CollectedAt: item.at}, AudienceNormal)
		if err != nil || result.Status != "accepted" {
			t.Fatalf("ingest %q: result=%+v err=%v", item.title, result, err)
		}
		job := Job{ArticleID: result.ArticleID}
		if err := store.Complete(ctx, job, item.category, []string{"关键词一", "关键词二"}, "摘要", 3, "test-model"); err != nil {
			t.Fatalf("complete %q: %v", item.title, err)
		}
	}

	max, err := store.MaxPosition(ctx, "科技", AudienceNormal)
	if err != nil || max != 2 {
		t.Fatalf("category max = %d, err=%v; want 2", max, err)
	}
	first, err := store.List(ctx, "科技", AudienceNormal, 0, 1)
	if err != nil || len(first) != 1 || first[0].Title != "科技新消息" {
		t.Fatalf("first category position = %+v, err=%v", first, err)
	}
	second, err := store.List(ctx, "科技", AudienceNormal, 1, 2)
	if err != nil || len(second) != 1 || second[0].Title != "科技旧消息" {
		t.Fatalf("second category position = %+v, err=%v", second, err)
	}
}

func TestHideAlsoHidesMatchingAudienceCopy(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	article := ArticleInput{Source: "测试", Title: "同一资讯", Body: "相同正文", CollectedAt: 100, OriginalURL: "https://example.com/news/1"}
	normal, err := store.Ingest(ctx, article, AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	vip, err := store.Ingest(ctx, article, AudienceVIP)
	if err != nil {
		t.Fatal(err)
	}
	if normal.ArticleID == vip.ArticleID {
		t.Fatal("normal and vip must keep separate article records")
	}
	status, err := store.Hide(ctx, normal.ArticleID)
	if err != nil || status != "hidden" {
		t.Fatalf("hide status=%q err=%v", status, err)
	}
	var hidden int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM articles WHERE hidden=1`).Scan(&hidden); err != nil {
		t.Fatal(err)
	}
	if hidden != 2 {
		t.Fatalf("hidden matching copies = %d, want 2", hidden)
	}
}

func TestIngestRejectsMillisecondCollectionTimestamp(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	result, err := store.Ingest(context.Background(), ArticleInput{
		Source: "测试", Title: "资讯", Body: "正文", CollectedAt: 1791798120000,
	}, AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "rejected" || result.Reason != RejectTimeNotSecond {
		t.Fatalf("result=%+v; want a millisecond timestamp named as such", result)
	}
}

func TestFindSimilarCandidatesUsesRecentRawArticlesInSameAudience(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	first := ArticleInput{
		Source: "城市日报", Title: "市政府公布城市更新计划",
		Body:        "市政府今日公布城市更新计划，将改造老旧社区并新增公共服务设施。相关部门表示，项目将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}
	if result, err := store.Ingest(ctx, first, AudienceNormal); err != nil || result.Status != "accepted" {
		t.Fatalf("ingest candidate: result=%+v err=%v", result, err)
	}

	incoming := ArticleInput{
		Source: "本地新闻", Title: "老旧社区改造与服务设施建设启动",
		Body:        "市政府今日公布城市更新计划，改造老旧社区并新增公共服务设施。有关部门称项目会分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}
	candidates, err := store.FindSimilarCandidates(ctx, incoming, AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Similarity < similarityThreshold {
		t.Fatalf("candidates=%+v; want one recent similar article", candidates)
	}

	vipCandidates, err := store.FindSimilarCandidates(ctx, incoming, AudienceVIP)
	if err != nil {
		t.Fatal(err)
	}
	if len(vipCandidates) != 0 {
		t.Fatalf("VIP candidates=%+v; want audience isolation", vipCandidates)
	}

	if _, err := store.DB.ExecContext(ctx, `UPDATE articles SET created_at=?`, time.Now().Unix()-similarityWindowSeconds-1); err != nil {
		t.Fatal(err)
	}
	candidates, err = store.FindSimilarCandidates(ctx, incoming, AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expired candidates=%+v; want none older than 24 hours", candidates)
	}
}

// Retention is opt-in: with the default zero age nothing may be touched.
func TestTrimOldBodiesIsDisabledByDefault(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	long := strings.Repeat("正文内容", 200)
	for i := 0; i < 3; i++ {
		if _, err := store.Ingest(context.Background(), ArticleInput{
			Source: "来源", Title: "资讯", Body: long, CollectedAt: time.Now().Add(-time.Hour * 48).Unix(),
		}, AudienceNormal); err != nil {
			t.Fatal(err)
		}
	}
	trimmed, err := store.TrimOldBodies(context.Background(), 0, 400)
	if err != nil {
		t.Fatal(err)
	}
	if trimmed != 0 {
		t.Fatalf("trimmed=%d; retention must be off by default", trimmed)
	}
	var bodies []string
	rows, err := store.QueryDB.Query(`SELECT body FROM articles`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	for _, body := range bodies {
		if body != long {
			t.Fatalf("a body was trimmed while retention is off: %d chars", len([]rune(body)))
		}
	}
}

// Trimming must bound growth without renumbering anything.
func TestTrimOldBodiesKeepsPositionsIntact(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	long := strings.Repeat("正文内容", 200)
	for i := 0; i < 3; i++ {
		if _, err := store.Ingest(context.Background(), ArticleInput{
			Source: "来源", Title: "资讯", Body: long,
			CollectedAt: time.Now().Add(-time.Hour * 48).Unix(),
		}, AudienceNormal); err != nil {
			t.Fatal(err)
		}
	}
	before, err := store.MaxPosition(context.Background(), "all", AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	trimmed, err := store.TrimOldBodies(context.Background(), 24*time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if trimmed != 3 {
		t.Fatalf("trimmed=%d; want 3", trimmed)
	}
	after, err := store.MaxPosition(context.Background(), "all", AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("max position moved from %d to %d; cursors would break", before, after)
	}
	var rows int
	if err := store.QueryDB.QueryRow(`SELECT COUNT(*) FROM articles`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("got %d articles after trimming; articles must never be deleted", rows)
	}
	var title, body string
	if err := store.QueryDB.QueryRow(`SELECT title,body FROM articles ORDER BY id LIMIT 1`).Scan(&title, &body); err != nil {
		t.Fatal(err)
	}
	if title != "资讯" {
		t.Fatalf("title changed to %q", title)
	}
	if got := len([]rune(body)); got != 11 {
		t.Fatalf("body is %d runes; want 10 kept plus the marker", got)
	}
}

func TestPruneCompletedIngestKeepsUnfinishedRows(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	if _, err := store.EnqueueIngestBatch(context.Background(), []ArticleInput{
		{Source: "来源", Title: "标题", Body: "正文", CollectedAt: time.Now().Unix()},
	}, AudienceNormal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec(`UPDATE ingest_queue SET status='completed',body=NULL,updated_at=?`, time.Now().Unix()-7200); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueIngestBatch(context.Background(), []ArticleInput{
		{Source: "来源", Title: "第二篇", Body: "正文", CollectedAt: time.Now().Unix()},
	}, AudienceNormal); err != nil {
		t.Fatal(err)
	}
	pruned, err := store.PruneCompletedIngest(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("pruned=%d; want 1", pruned)
	}
	var pending int
	if err := store.QueryDB.QueryRow(`SELECT COUNT(*) FROM ingest_queue WHERE status='pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending rows=%d; unprocessed work must survive", pending)
	}
}
