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
// content received in the same audience during the previous 24 hours. It only
// narrows candidates; the caller must ask the model before classifying a hit
// as a duplicate.
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
		candidate.Similarity = articleSimilarityToSets(incomingTitle, incomingBody, candidate.Title, candidate.Body)
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

func articleSimilarityToSets(titleA, bodyA map[string]struct{}, titleB, bodyB string) float64 {
	titleScore := shingleJaccard(titleA, textShingles(titleB))
	bodyScore := shingleJaccard(bodyA, textShingles(bodyB))
	return titleSimilarityWeight*titleScore + bodySimilarityWeight*bodyScore
}

func textShingles(text string) map[string]struct{} {
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
