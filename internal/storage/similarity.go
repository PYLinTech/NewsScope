package storage

import (
	"context"
	"strings"
	"time"
	"unicode"
)

const (
	similarityWindowSeconds = int64(24 * time.Hour / time.Second)
	similarityThreshold     = 0.20
	maxSimilarityCandidates = 2
	titleSimilarityWeight   = 0.30
	bodySimilarityWeight    = 0.70
)

// SimilarCandidate is an existing raw article that passed the local content
// similarity filter and can be reviewed by the model.
type SimilarCandidate struct {
	ArticleID   int64
	Source      string
	Title       string
	Body        string
	CollectedAt int64
	Similarity  float64
}

// FindSimilarCandidates compares the supplied raw title and body with raw
// content received in the same audience during the previous 24 hours. The
// caller may treat exact 100% matches as duplicates; other candidates require
// model review.
func (s *Store) FindSimilarCandidates(ctx context.Context, in ArticleInput, audience string) ([]SimilarCandidate, error) {
	cutoff := time.Now().Unix() - similarityWindowSeconds
	// The incoming article is shared across every comparison. Build its shingle
	// sets once instead of repeating tokenization and map allocation per row.
	incomingTitle := textShingles(in.Title)
	incomingBody := textShingles(in.Body)
	rows, err := s.DB.QueryContext(ctx, `SELECT id,source,title,body,collected_at FROM articles WHERE audience=? AND created_at>=? ORDER BY created_at DESC,id DESC`, audience, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]SimilarCandidate, 0, maxSimilarityCandidates)
	for rows.Next() {
		var candidate SimilarCandidate
		if err := rows.Scan(&candidate.ArticleID, &candidate.Source, &candidate.Title, &candidate.Body, &candidate.CollectedAt); err != nil {
			return nil, err
		}
		candidate.Similarity = articleSimilarity(incomingTitle, incomingBody, candidate.Title, candidate.Body)
		if candidate.Similarity < similarityThreshold {
			continue
		}
		candidates = insertTopCandidate(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func insertTopCandidate(top []SimilarCandidate, candidate SimilarCandidate) []SimilarCandidate {
	index := 0
	for index < len(top) && (top[index].Similarity > candidate.Similarity ||
		(top[index].Similarity == candidate.Similarity && top[index].ArticleID > candidate.ArticleID)) {
		index++
	}
	if index >= maxSimilarityCandidates {
		return top
	}
	top = append(top, SimilarCandidate{})
	copy(top[index+1:], top[index:])
	top[index] = candidate
	if len(top) > maxSimilarityCandidates {
		top = top[:maxSimilarityCandidates]
	}
	return top
}

func articleSimilarity(titleSet, bodySet map[string]struct{}, title, body string) float64 {
	titleScore := shingleJaccard(titleSet, textShingles(title))
	bodyScore := shingleJaccard(bodySet, textShingles(body))
	return titleSimilarityWeight*titleScore + bodySimilarityWeight*bodyScore
}

// textShingles tokenises text and returns its shingle set: whole tokens for
// non-CJK runs, overlapping character bigrams for CJK, which is how Chinese
// text has no word boundaries.
//
// The scan works on byte offsets in the original string. CJK bigrams are then
// substrings of that string, so building a set costs no per-bigram allocation —
// which matters because this runs once per candidate article per submission.
func textShingles(text string) map[string]struct{} {
	text = strings.ToLower(text)
	out := make(map[string]struct{})
	start := -1
	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			addTokenShingles(out, text[start:i])
			start = -1
		}
	}
	if start >= 0 {
		addTokenShingles(out, text[start:])
	}
	return out
}

func addTokenShingles(out map[string]struct{}, token string) {
	if !containsCJK(token) {
		out[token] = struct{}{}
		return
	}
	// A two-rune shingle spans from one rune's start to the start of the rune
	// after it, so the loop keeps a two-step lookbehind: `first` is where the
	// shingle begins and `second` is the offset the shingle ends at.
	first, second := -1, -1
	for offset := range token {
		if first >= 0 {
			out[token[first:offset]] = struct{}{}
		}
		first, second = second, offset
	}
	if first >= 0 {
		out[token[first:len(token)]] = struct{}{}
		return
	}
	// A single-rune token has no bigram to emit; keep the character itself.
	out[token] = struct{}{}
}

func containsCJK(token string) bool {
	for _, r := range token {
		if unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r) {
			return true
		}
	}
	return false
}

func shingleJaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	intersection := 0
	for value := range a {
		if _, ok := b[value]; ok {
			intersection++
		}
	}
	union := len(a) + len(b) - intersection
	return float64(intersection) / float64(union)
}
