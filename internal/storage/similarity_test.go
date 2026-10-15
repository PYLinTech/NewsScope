package storage

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode"
)

// referenceTextShingles is the original tokeniser, kept verbatim as an oracle
// so the optimised version cannot drift from it unnoticed.
func referenceTextShingles(text string) map[string]struct{} {
	text = strings.ToLower(text)
	tokens := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make(map[string]struct{})
	for _, token := range tokens {
		runes := []rune(token)
		containsCJK := false
		for _, r := range runes {
			if unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r) {
				containsCJK = true
				break
			}
		}
		if !containsCJK {
			out[token] = struct{}{}
			continue
		}
		if len(runes) == 1 {
			out[token] = struct{}{}
			continue
		}
		for i := 0; i+1 < len(runes); i++ {
			out[string(runes[i:i+2])] = struct{}{}
		}
	}
	return out
}

func TestTextShinglesMatchesReference(t *testing.T) {
	cases := []string{
		"",
		" ",
		"。，，，",
		"处理器",
		"某公司发布新款处理器",
		"某公司今日发布新款处理器，采用新一代工艺，产品将在下季度上市。",
		"Hello World",
		"Apple announces new chip 2024",
		"Mixed 中英文 content 混排",
		"Processor 处理器 2024年第一季度",
		"株式会社は新製品を発表しました",
		"한국어 텍스트 테스트",
		"12345",
		"a1b2c3",
		"emoji 😀 text 测试",
		"ABCdef GHIjkl",
		"多 空格   分隔   的   文本",
		strings.Repeat("重复内容", 50),
		strings.Repeat("Processor", 30),
		"多字节字符与ASCII混排test文本mixed",
		"ＡＢＣ全角ＡＢＣ",
		"tab\tseparated\tvalues",
		"换行\n分隔\n文本",
	}
	for _, c := range cases {
		want := referenceTextShingles(c)
		got := textShingles(c)
		if !reflect.DeepEqual(want, got) {
			t.Errorf("textShingles(%q):\n  want %v\n  got  %v", c, sortedKeys(want), sortedKeys(got))
		}
	}
}

// Similarity scores must also be unchanged, since the threshold decides which
// candidates ever reach the model.
func TestSimilarityMatchesReference(t *testing.T) {
	pairs := [][2]string{
		{"某公司发布新款处理器", "该公司今日正式发布了新款处理器，采用新一代工艺，售价也未公布。"},
		{"完全相同的标题", "完全相同的正文内容在这里"},
		{"苹果发布新手机", "某公司今日发布新款处理器，采用新一代工艺，产品将在下季度上市。"},
		{"Apple ships new chip", "Apple announced a new processor today for laptops."},
		{"", "空标题"},
		{"短", "短"},
	}
	for _, pair := range pairs {
		wantTitle := referenceTextShingles(pair[0])
		wantBody := referenceTextShingles(pair[1])
		want := titleSimilarityWeight*shingleJaccard(wantTitle, referenceTextShingles(pair[0])) +
			bodySimilarityWeight*shingleJaccard(wantBody, referenceTextShingles(pair[1]))
		got := articleSimilarity(textShingles(pair[0]), textShingles(pair[1]), pair[0], pair[1])
		if want != got {
			t.Errorf("articleSimilarity(%q, %q) = %v; reference gives %v", pair[0], pair[1], got, want)
		}
	}
}

func TestInsertTopCandidateKeepsBestTwo(t *testing.T) {
	// Descending by similarity, ties broken by descending article id.
	top := make([]SimilarCandidate, 0, maxSimilarityCandidates)
	for _, c := range []SimilarCandidate{
		{ArticleID: 7, Similarity: 0.30},
		{ArticleID: 3, Similarity: 0.30},
		{ArticleID: 9, Similarity: 0.50},
		{ArticleID: 1, Similarity: 0.10},
		{ArticleID: 5, Similarity: 0.50},
	} {
		top = insertTopCandidate(top, c)
	}
	if len(top) != maxSimilarityCandidates {
		t.Fatalf("kept %d candidates; want %d", len(top), maxSimilarityCandidates)
	}
	// 9 and 5 share 0.50, and the tie-break is the higher id first.
	if top[0].ArticleID != 9 || top[1].ArticleID != 5 {
		t.Fatalf("top = %d, %d; want 9, 5 (equal similarity, higher id first)", top[0].ArticleID, top[1].ArticleID)
	}
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
