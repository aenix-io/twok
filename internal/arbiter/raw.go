package arbiter

import (
	"fmt"
	"strings"
)

// RawFactory builds an Arbiter from the URL exactly as the operator wrote
// it. It exists for backends whose URLs net/url does not parse, or parses
// differently between Go releases: an etcd URL with a comma-separated list
// of endpoints in the host part is the case in point. Credentials come from
// files or the environment, never from the URL, as for Factory.
type RawFactory func(rawURL string) (Arbiter, error)

var rawFactories = make(map[string]RawFactory)

// RegisterRaw adds a backend for a URL scheme that receives the raw URL.
// Open hands such a URL to the factory before net/url sees it. It panics
// when the scheme is already registered in either form.
func RegisterRaw(scheme string, f RawFactory) {
	mu.Lock()
	defer mu.Unlock()
	if registered(scheme) {
		panic(fmt.Sprintf("arbiter: scheme %q registered twice", scheme))
	}
	rawFactories[scheme] = f
}

// registered reports whether a scheme has a factory of either form. The
// caller holds mu.
func registered(scheme string) bool {
	_, plain := factories[scheme]
	_, raw := rawFactories[scheme]
	return plain || raw
}

// rawFactory returns the raw factory for the scheme of rawURL, if any. The
// scheme is everything before the first colon, compared case-insensitively
// as RFC 3986 asks.
func rawFactory(rawURL string) (RawFactory, bool) {
	scheme, _, ok := strings.Cut(rawURL, ":")
	if !ok {
		return nil, false
	}
	mu.RLock()
	defer mu.RUnlock()
	f, ok := rawFactories[strings.ToLower(scheme)]
	return f, ok
}
