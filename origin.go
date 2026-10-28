package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
)

// DefaultAllowedOrigins are the production pages that open the socket: the app
// and the marketing site's free tools.
var DefaultAllowedOrigins = []string{
	"https://app.truelist.io",
	"https://truelist.io",
	"https://www.truelist.io",
}

// DevAllowedOrigins are added when ALLOW_DEV_ORIGINS is set, along with any
// localhost port.
var DevAllowedOrigins = []string{
	"https://staging.truelist.io",
}

// OriginAllowlist decides which browser origins may open a socket. It only
// stops pages on other sites from connecting; it is not authentication.
type OriginAllowlist struct {
	origins        map[string]struct{}
	allowLocalhost bool
}

func NewOriginAllowlist(origins []string, allowDev bool) *OriginAllowlist {
	a := &OriginAllowlist{origins: make(map[string]struct{}), allowLocalhost: allowDev}

	if allowDev {
		origins = append(append([]string{}, origins...), DevAllowedOrigins...)
	}
	for _, origin := range origins {
		origin = normalizeOrigin(origin)
		if origin != "" {
			a.origins[origin] = struct{}{}
		}
	}

	return a
}

// OriginAllowlistFromEnv reads ALLOWED_ORIGINS (comma-separated, replaces the
// defaults) and ALLOW_DEV_ORIGINS (adds staging and localhost).
func OriginAllowlistFromEnv() *OriginAllowlist {
	origins := DefaultAllowedOrigins
	if env := os.Getenv("ALLOWED_ORIGINS"); strings.TrimSpace(env) != "" {
		origins = strings.Split(env, ",")
	}

	allowDev := false
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ALLOW_DEV_ORIGINS"))) {
	case "1", "true", "yes":
		allowDev = true
	}

	return NewOriginAllowlist(origins, allowDev)
}

// CheckOrigin is a websocket.Upgrader CheckOrigin func. Requests without an
// Origin header come from non-browser clients, which could send any origin
// anyway, so they are allowed.
func (a *OriginAllowlist) CheckOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	if a.Allows(origin) {
		return true
	}

	log.Println("Rejected websocket from origin: ", origin)
	return false
}

func (a *OriginAllowlist) Allows(origin string) bool {
	normalized := normalizeOrigin(origin)
	if normalized == "" {
		return false
	}

	if _, ok := a.origins[normalized]; ok {
		return true
	}

	if a.allowLocalhost {
		u, _ := url.Parse(normalized)
		switch u.Hostname() {
		case "localhost", "127.0.0.1", "::1":
			return true
		}
	}

	return false
}

func (a *OriginAllowlist) String() string {
	origins := make([]string, 0, len(a.origins))
	for origin := range a.origins {
		origins = append(origins, origin)
	}
	sort.Strings(origins)
	s := strings.Join(origins, ", ")
	if a.allowLocalhost {
		s += ", localhost"
	}
	return s
}

// normalizeOrigin returns scheme://host[:port] in lower case, or "" if origin
// isn't an http(s) origin.
func normalizeOrigin(origin string) string {
	u, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || u.Host == "" {
		return ""
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return ""
	}

	return scheme + "://" + strings.ToLower(u.Host)
}
