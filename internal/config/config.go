package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	OpenAIBaseURL         string
	OpenAIAPIKey          string
	OpenAIModel           string
	NormalAddr            string
	VIPAddr               string
	CollectorPort         int
	AdminPort             int
	MaxQueryCount         int
	Categories            []Category
	DatabasePath          string
	Workers               int
	MaxAttempts           int
	TimeoutSec            int
	MaxBodyChars          int
	MaxDuplicateDiffChars int
	MaxSummaryChars       int
}

type Category struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func Load() (Config, error) {
	loadDotEnv(".env")
	c := Config{
		OpenAIBaseURL:         strings.TrimRight(strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")), "/"),
		OpenAIAPIKey:          strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		OpenAIModel:           strings.TrimSpace(os.Getenv("OPENAI_MODEL")),
		NormalAddr:            env("NORMAL_ADDR", "127.0.0.1:8887"),
		VIPAddr:               env("VIP_ADDR", "127.0.0.1:8888"),
		MaxQueryCount:         intEnv("MAX_QUERY_COUNT", 100),
		DatabasePath:          env("DATABASE_PATH", "./data/newsscope.db"),
		Workers:               intEnv("ANALYSIS_WORKERS", 3),
		MaxAttempts:           intEnv("ANALYSIS_MAX_ATTEMPTS", 5),
		TimeoutSec:            intEnv("ANALYSIS_TIMEOUT_SECONDS", 600),
		MaxBodyChars:          intEnv("ANALYSIS_MAX_BODY_CHARS", 300000),
		MaxDuplicateDiffChars: intEnv("ANALYSIS_MAX_DUPLICATE_DIFF_CHARS", 30000),
		MaxSummaryChars:       intEnv("ANALYSIS_MAX_SUMMARY_CHARS", 120),
	}
	c.CollectorPort = intEnv("COLLECTOR_PORT", 8886)
	c.AdminPort = intEnv("ADMIN_PORT", 8889)
	if c.OpenAIBaseURL == "" || c.OpenAIAPIKey == "" || c.OpenAIModel == "" {
		return c, fmt.Errorf("OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL are required")
	}
	baseURL, err := url.Parse(c.OpenAIBaseURL)
	if err != nil || (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return c, fmt.Errorf("OPENAI_BASE_URL must be an absolute HTTP(S) URL")
	}
	if c.MaxQueryCount < 1 || c.MaxQueryCount > 10000 {
		return c, fmt.Errorf("MAX_QUERY_COUNT must be between 1 and 10000")
	}
	if c.Workers < 1 || c.Workers > 32 || c.MaxAttempts < 1 || c.MaxAttempts > 20 || c.TimeoutSec < 1 || c.MaxDuplicateDiffChars < 1 || c.MaxSummaryChars < 1 || c.MaxSummaryChars > 2000 {
		return c, fmt.Errorf("analysis worker, retry, timeout, duplicate diff or summary limit configuration is invalid")
	}
	for name, addr := range map[string]string{"NORMAL_ADDR": c.NormalAddr, "VIP_ADDR": c.VIPAddr} {
		_, portText, err := net.SplitHostPort(addr)
		port, portErr := strconv.Atoi(portText)
		if err != nil || portErr != nil || port < 1 || port > 65535 {
			return c, fmt.Errorf("%s must be host:port", name)
		}
	}
	for name, port := range map[string]int{"COLLECTOR_PORT": c.CollectorPort, "ADMIN_PORT": c.AdminPort} {
		if port < 1 || port > 65535 {
			return c, fmt.Errorf("%s must be between 1 and 65535", name)
		}
	}
	var categories []Category
	rawCategories := strings.TrimSpace(os.Getenv("NEWS_CATEGORIES"))
	if rawCategories == "" {
		return c, fmt.Errorf("NEWS_CATEGORIES is required and must be a JSON array of category objects")
	}
	if err := json.Unmarshal([]byte(rawCategories), &categories); err != nil {
		return c, fmt.Errorf("NEWS_CATEGORIES must be a JSON array of category objects: %w", err)
	}
	if len(categories) == 0 {
		return c, fmt.Errorf("NEWS_CATEGORIES must not be empty")
	}
	seen := map[string]bool{"all": true}
	for i := range categories {
		categories[i].Name = strings.TrimSpace(categories[i].Name)
		categories[i].Description = strings.TrimSpace(categories[i].Description)
		if categories[i].Name == "" || categories[i].Description == "" || seen[categories[i].Name] {
			return c, fmt.Errorf("NEWS_CATEGORIES contains an empty name, empty description or duplicate category")
		}
		seen[categories[i].Name] = true
	}
	c.Categories = categories
	if c.DatabasePath == "" {
		return c, fmt.Errorf("DATABASE_PATH must not be empty")
	}
	return c, nil
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
func intEnv(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}

func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var multilineKey string
	var multilineValue strings.Builder
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if multilineKey != "" {
			if strings.HasSuffix(line, "'") {
				multilineValue.WriteString(strings.TrimSuffix(line, "'"))
				setEnvIfUnset(multilineKey, strings.TrimSpace(multilineValue.String()))
				multilineKey = ""
				multilineValue.Reset()
			} else {
				multilineValue.WriteString(line)
				multilineValue.WriteByte('\n')
			}
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key != "" {
			if strings.HasPrefix(value, "'") {
				if len(value) > 1 && strings.HasSuffix(value, "'") {
					value = value[1 : len(value)-1]
				} else {
					multilineKey = key
					multilineValue.WriteString(strings.TrimPrefix(value, "'"))
					multilineValue.WriteByte('\n')
					continue
				}
			} else {
				value = strings.Trim(value, "\"")
			}
			setEnvIfUnset(key, value)
		}
	}
}

func setEnvIfUnset(key, value string) {
	if _, exists := os.LookupEnv(key); !exists {
		_ = os.Setenv(key, value)
	}
}
