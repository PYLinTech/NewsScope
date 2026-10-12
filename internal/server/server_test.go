package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"newsscope/internal/analyzer"
	"newsscope/internal/config"
	"newsscope/internal/storage"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestIngestRejectsBadItemsIndividually(t *testing.T) {
	store, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), nil)
	body := `{"version":1,"articles":[{"source":"示例","title":"有效资讯","body":"正文","collected_at":100},{"source":"示例","body":"缺少标题","collected_at":101},{"source":7,"title":"类型错误","body":"正文","collected_at":102},{"source":"示例","title":"毫秒时间","body":"正文","collected_at":1791798120000}]}`
	req := httptest.NewRequest(http.MethodPost, "/ingest/normal", bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	api.CollectorHandler().ServeHTTP(response, req.WithContext(context.Background()))

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got struct {
		Accepted   int `json:"accepted"`
		Duplicates int `json:"duplicates"`
		Rejected   int `json:"rejected"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Accepted != 1 || got.Duplicates != 0 || got.Rejected != 3 {
		t.Fatalf("counts=%+v; want accepted=1 duplicates=0 rejected=3", got)
	}
}

func TestIngestUsesModelToConfirmCodeSimilarity(t *testing.T) {
	store, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	first, err := store.Ingest(context.Background(), storage.ArticleInput{
		Source: "城市日报", Title: "市政府公布城市更新计划",
		Body:        "市政府今日公布城市更新计划，将改造老旧社区并新增公共服务设施。项目将分阶段推进。",
		CollectedAt: time.Now().Unix(),
	}, storage.AudienceNormal)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "accepted" {
		t.Fatalf("first article status=%q", first.Status)
	}

	calls := 0
	model := analyzer.New("http://model.invalid/v1", "test-key", "test-model", nil, 2000, 120, 5)
	model.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		decision := `{"duplicate":false,"matched_id":0}`
		if calls == 1 {
			decision = fmt.Sprintf(`{"duplicate":true,"matched_id":%d}`, first.ArticleID)
		}
		content, _ := json.Marshal(decision)
		responseBody := fmt.Sprintf(`{"choices":[{"message":{"content":%s}}]}`, content)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader([]byte(responseBody))),
			Request:    r,
		}, nil
	})}
	api := New(store, config.Config{}, log.New(io.Discard, "", 0), model)
	type ingestCounts struct {
		Accepted   int `json:"accepted"`
		Duplicates int `json:"duplicates"`
	}

	post := func(title string) ingestCounts {
		t.Helper()
		body := fmt.Sprintf(`{"version":1,"articles":[{"source":"本地新闻","title":%q,"body":%q,"collected_at":%d}]}`,
			title,
			"市政府今日公布城市更新计划，改造老旧社区并新增公共服务设施。有关部门称将分阶段推进。",
			time.Now().Unix(),
		)
		req := httptest.NewRequest(http.MethodPost, "/ingest/normal", bytes.NewBufferString(body))
		response := httptest.NewRecorder()
		api.CollectorHandler().ServeHTTP(response, req)
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		var got ingestCounts
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	duplicate := post("老旧社区更新项目公布")
	if duplicate.Duplicates != 1 || duplicate.Accepted != 1 {
		t.Fatalf("duplicate decision counts=%v", duplicate)
	}
	nonDuplicate := post("另一个城市更新项目的新阶段")
	if nonDuplicate.Accepted != 1 || nonDuplicate.Duplicates != 0 {
		t.Fatalf("non-duplicate decision counts=%v", nonDuplicate)
	}
	if calls != 2 {
		t.Fatalf("model review calls=%d; want 2", calls)
	}
	var count int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM articles`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("stored article count=%d; want existing plus model-approved new article", count)
	}
}
