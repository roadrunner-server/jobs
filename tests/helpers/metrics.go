package helpers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// RequireMetricsEventually polls url until the exposition contains every want
// snippet and returns that snapshot, so further assertions read one scrape.
func RequireMetricsEventually(t *testing.T, url string, want ...string) string {
	t.Helper()

	var (
		mu   sync.Mutex
		last string
	)

	require.Eventually(t, func() bool {
		body, err := scrape(t.Context(), url)
		if err != nil {
			return false
		}

		mu.Lock()
		last = body
		mu.Unlock()

		for _, w := range want {
			if !strings.Contains(body, w) {
				return false
			}
		}

		return true
	}, callTimeout, callTick, "metrics did not contain %v", want)

	mu.Lock()
	defer mu.Unlock()

	return last
}

func scrape(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
