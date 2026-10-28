package main

import (
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
)

func TestOriginAllowlist(t *testing.T) {
	prod := NewOriginAllowlist(DefaultAllowedOrigins, false)
	dev := NewOriginAllowlist(DefaultAllowedOrigins, true)

	cases := []struct {
		origin string
		prod   bool
		dev    bool
	}{
		{"https://app.truelist.io", true, true},
		{"https://APP.truelist.io", true, true},
		{"https://truelist.io", true, true},
		{"https://www.truelist.io", true, true},
		{"http://app.truelist.io", false, false},
		{"https://app.truelist.io.evil.com", false, false},
		{"https://evil.com", false, false},
		{"null", false, false},
		{"https://staging.truelist.io", false, true},
		{"http://localhost:5173", false, true},
		{"http://127.0.0.1:8000", false, true},
		{"http://localhost.evil.com", false, false},
	}

	for _, c := range cases {
		if got := prod.Allows(c.origin); got != c.prod {
			t.Errorf("prod.Allows(%q) = %v, want %v", c.origin, got, c.prod)
		}
		if got := dev.Allows(c.origin); got != c.dev {
			t.Errorf("dev.Allows(%q) = %v, want %v", c.origin, got, c.dev)
		}
	}
}

func TestOriginAllowlistFromEnv(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://a.example.com, https://b.example.com")
	t.Setenv("ALLOW_DEV_ORIGINS", "")

	a := OriginAllowlistFromEnv()
	if !a.Allows("https://b.example.com") {
		t.Error("expected ALLOWED_ORIGINS entry to be allowed")
	}
	if a.Allows("https://app.truelist.io") {
		t.Error("expected ALLOWED_ORIGINS to replace the defaults")
	}
	if a.Allows("http://localhost:5173") {
		t.Error("expected localhost to need ALLOW_DEV_ORIGINS")
	}

	t.Setenv("ALLOWED_ORIGINS", "")
	t.Setenv("ALLOW_DEV_ORIGINS", "true")

	a = OriginAllowlistFromEnv()
	if !a.Allows("https://app.truelist.io") || !a.Allows("http://localhost:5173") || !a.Allows("https://staging.truelist.io") {
		t.Errorf("expected defaults plus dev origins, got %s", a)
	}
}

func TestUpgradeChecksOrigin(t *testing.T) {
	_, url := newTestServer(t, DefaultTimeouts)

	_, resp, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"https://evil.com"}})
	if err == nil {
		t.Fatal("expected a foreign origin to be rejected")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %v", resp)
	}

	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"https://app.truelist.io"}})
	if err != nil {
		t.Fatalf("expected app origin to be allowed: %v", err)
	}
	conn.Close()
}
