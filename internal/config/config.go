package config

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	OpenAIBaseURL string
	OpenAIAPIKey  string
	OpenAIModel   string
	NormalAddr    string
	VIPAddr       string
	CollectorPort int
	AdminPort     int
	MaxQueryCount int
	Categories    []string
	DatabasePath  string
	Workers       int
	MaxAttempts   int
	TimeoutSec    int
	MaxBodyChars  int
}

func Load() (Config, error) {
	loadDotEnv(".env")
	c := Config{
		OpenAIBaseURL: strings.TrimRight(strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")), "/"),
		OpenAIAPIKey:  strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		OpenAIModel:   strings.TrimSpace(os.Getenv("OPENAI_MODEL")),
		NormalAddr:    env("NORMAL_ADDR", "127.0.0.1:8887"),
		VIPAddr:       env("VIP_ADDR", "127.0.0.1:8888"),
		MaxQueryCount: intEnv("MAX_QUERY_COUNT", 100),
		DatabasePath:  env("DATABASE_PATH", "./data/newsscope.db"),
		Workers:       intEnv("ANALYSIS_WORKERS", 3),
		MaxAttempts:   intEnv("ANALYSIS_MAX_ATTEMPTS", 5),
		TimeoutSec:    intEnv("ANALYSIS_TIMEOUT_SECONDS", 45),
		MaxBodyChars:  intEnv("ANALYSIS_MAX_BODY_CHARS", 12000),
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
	if c.Workers < 1 || c.Workers > 32 || c.MaxAttempts < 1 || c.MaxAttempts > 20 || c.TimeoutSec < 1 || c.TimeoutSec > 600 || c.MaxBodyChars < 100 || c.MaxBodyChars > 200000 {
		return c, fmt.Errorf("analysis worker, retry, timeout or body limit configuration is invalid")
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
	raw := strings.Split(env("NEWS_CATEGORIES", "时政,科技,财经,文旅,社会,商业,教育,健康,国际"), ",")
	seen := map[string]bool{"all": true}
	for _, item := range raw {
		name := strings.TrimSpace(item)
		if name == "" || seen[name] {
			return c, fmt.Errorf("NEWS_CATEGORIES contains an empty or duplicate category")
		}
		seen[name] = true
		c.Categories = append(c.Categories, name)
	}
	if len(c.Categories) == 0 {
		return c, fmt.Errorf("NEWS_CATEGORIES must not be empty")
	}
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
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		value = strings.Trim(value, "\"'")
		if key != "" {
			if _, exists := os.LookupEnv(key); !exists {
				_ = os.Setenv(key, value)
			}
		}
	}
}
