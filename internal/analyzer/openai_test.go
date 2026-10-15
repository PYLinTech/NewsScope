package analyzer

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"newsscope/internal/config"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newStubbed(t *testing.T, content string) *Client {
	t.Helper()
	c := New("http://model.invalid/v1", "k", "m",
		[]config.Category{{Name: "科技", Description: "技术新闻"}},
		1000, 30000, 40, 5)
	c.HTTP = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		body := `{"choices":[{"message":{"content":` + quote(content) + `}}]}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader([]byte(body))),
			Request:    r,
		}, nil
	})}
	return c
}

func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func TestAnalyzeTrimsKeywordsBeforeStoring(t *testing.T) {
	// A body longer than the summary limit, so the model must supply a summary.
	body := strings.Repeat("正文内容", 60)
	c := newStubbed(t, `{"category":"科技","keywords":[" 处理器 ","  新一代工艺  ","第三代制程"],"summary":"某公司发布新款处理器。","importance":3}`)

	result, err := c.Analyze(context.Background(), "标题", body)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	want := []string{"处理器", "新一代工艺", "第三代制程"}
	if len(result.Keywords) != len(want) {
		t.Fatalf("keywords=%q; want %q", result.Keywords, want)
	}
	for i := range want {
		if result.Keywords[i] != want[i] {
			t.Fatalf("keywords=%q; want %q — padding must not reach the corpus", result.Keywords, want)
		}
	}
}

// The length limit applies to the trimmed value, so padding cannot push a
// keyword over the edge.
func TestAnalyzeAppliesKeywordLimitAfterTrimming(t *testing.T) {
	body := strings.Repeat("正文内容", 60)
	withinLimit := strings.Repeat("词", 40)
	c := newStubbed(t, `{"category":"科技","keywords":["  `+withinLimit+`  ","工艺"],"summary":"摘要。","importance":3}`)
	if _, err := c.Analyze(context.Background(), "标题", body); err != nil {
		t.Fatalf("a 40-rune keyword padded with spaces should be accepted: %v", err)
	}

	overLimit := strings.Repeat("词", 41)
	c = newStubbed(t, `{"category":"科技","keywords":["`+overLimit+`","工艺"],"summary":"摘要。","importance":3}`)
	if _, err := c.Analyze(context.Background(), "标题", body); err == nil {
		t.Fatal("a 41-rune keyword should be rejected")
	}
}

func TestAnalyzeRejectsPaddedBlankKeyword(t *testing.T) {
	body := strings.Repeat("正文内容", 60)
	c := newStubbed(t, `{"category":"科技","keywords":["   ","工艺"],"summary":"摘要。","importance":3}`)
	if _, err := c.Analyze(context.Background(), "标题", body); err == nil {
		t.Fatal("a whitespace-only keyword should be rejected")
	}
}
