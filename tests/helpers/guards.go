package helpers

import (
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// strictEnv turns a missing piece of the test environment from a skip into a
// failure. CI sets it so that a test can never silently disappear from a green
// run because a broker or a PHP extension was absent.
const strictEnv = "RR_JOBS_STRICT_ENV"

const brokerDialTimeout = time.Second

// RequireBrokers skips the test when any of the addresses does not accept a
// connection, which is the state of a machine without the docker compose stack.
func RequireBrokers(t *testing.T, addrs ...string) {
	t.Helper()

	dialer := net.Dialer{Timeout: brokerDialTimeout}

	var unreachable []string
	for _, addr := range addrs {
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		if err != nil {
			unreachable = append(unreachable, addr)
			continue
		}
		_ = conn.Close()
	}

	if len(unreachable) == 0 {
		return
	}

	skipOrFailf(t, "brokers are not reachable: %v, bring up tests/env/docker-compose-jobs.yaml", unreachable)
}

// RequirePHPExtension skips the test when the PHP binary on PATH was built
// without the extension. The workers talk over pipes, but a script that opens an
// rpc connection needs the sockets extension.
func RequirePHPExtension(t *testing.T, name string) {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "php", "-r", `exit(extension_loaded($argv[1]) ? 0 : 1);`, "--", name)
	if err := cmd.Run(); err == nil {
		return
	}

	skipOrFailf(t, "php was built without the %s extension", name)
}

func skipOrFailf(t *testing.T, format string, args ...any) {
	t.Helper()

	if os.Getenv(strictEnv) == "1" {
		require.Failf(t, "the test environment is incomplete", format, args...)
		return
	}

	t.Skipf(format+" (set %s=1 to fail instead of skipping)", append(args, strictEnv)...)
}
