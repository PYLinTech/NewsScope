package config

import (
	"strings"
	"testing"
)

func withEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for key, value := range values {
		t.Setenv(key, value)
	}
	t.Setenv("OPENAI_BASE_URL", "https://api.example.com/v1")
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_MODEL", "test-model")
	t.Setenv("NEWS_CATEGORIES", `[{"name":"科技","description":"技术新闻"}]`)
}

func TestListenerAddresses(t *testing.T) {
	t.Run("every listener defaults to loopback", func(t *testing.T) {
		withEnv(t, nil)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		for name, got := range map[string]string{
			"NORMAL_ADDR":    cfg.NormalAddr,
			"VIP_ADDR":       cfg.VIPAddr,
			"COLLECTOR_ADDR": cfg.CollectorAddr,
			"ADMIN_ADDR":     cfg.AdminAddr,
		} {
			if want := "127.0.0.1:"; got[:len(want)] != want {
				t.Errorf("%s = %q, want a loopback address", name, got)
			}
		}
	})

	t.Run("explicit addresses win for container deployment", func(t *testing.T) {
		withEnv(t, map[string]string{
			"NORMAL_ADDR":    "0.0.0.0:8887",
			"VIP_ADDR":       "0.0.0.0:8888",
			"COLLECTOR_ADDR": "0.0.0.0:8886",
			"ADMIN_ADDR":     "0.0.0.0:8889",
		})
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		for name, got := range map[string]string{
			"NORMAL_ADDR":    cfg.NormalAddr,
			"VIP_ADDR":       cfg.VIPAddr,
			"COLLECTOR_ADDR": cfg.CollectorAddr,
			"ADMIN_ADDR":     cfg.AdminAddr,
		} {
			if want := "0.0.0.0:"; got[:len(want)] != want {
				t.Errorf("%s = %q, want %q", name, got, want)
			}
		}
	})

	t.Run("address without a port is rejected", func(t *testing.T) {
		for _, key := range []string{"NORMAL_ADDR", "VIP_ADDR", "COLLECTOR_ADDR", "ADMIN_ADDR"} {
			withEnv(t, map[string]string{key: "0.0.0.0"})
			if _, err := Load(); err == nil {
				t.Errorf("Load accepted %s without a port", key)
			}
		}
	})
}

// The port-only settings were replaced by the host:port addresses. A stale
// value has to fail loudly instead of silently pinning a different port.
func TestRetiredPortOnlySettings(t *testing.T) {
	t.Run("COLLECTOR_PORT is reported with its replacement", func(t *testing.T) {
		withEnv(t, map[string]string{"COLLECTOR_PORT": "9999"})
		_, err := Load()
		if err == nil {
			t.Fatal("Load accepted the retired COLLECTOR_PORT setting")
		}
		for _, want := range []string{"COLLECTOR_PORT", "COLLECTOR_ADDR=127.0.0.1:9999"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("ADMIN_PORT is reported with its replacement", func(t *testing.T) {
		withEnv(t, map[string]string{"ADMIN_PORT": "9998"})
		_, err := Load()
		if err == nil {
			t.Fatal("Load accepted the retired ADMIN_PORT setting")
		}
		for _, want := range []string{"ADMIN_PORT", "ADMIN_ADDR=127.0.0.1:9998"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("the replacement host is kept when the address is widened", func(t *testing.T) {
		withEnv(t, map[string]string{"ADMIN_ADDR": "0.0.0.0:8889", "ADMIN_PORT": "9998"})
		_, err := Load()
		if err == nil {
			t.Fatal("Load accepted the retired ADMIN_PORT setting")
		}
		if !strings.Contains(err.Error(), "ADMIN_ADDR=0.0.0.0:9998") {
			t.Errorf("error %q does not reuse the configured host", err)
		}
	})
}
