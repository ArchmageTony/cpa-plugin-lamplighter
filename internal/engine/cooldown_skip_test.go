package engine

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PosvdM/cpa-plugin-lamplighter/internal/ignite"
	"github.com/PosvdM/cpa-plugin-lamplighter/internal/providers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// A host whose Claude credential is reported by CPA as cooling until.
func coolingClaudeHost(c *clock, until time.Time) *fakeHost {
	h := standardHost(c)
	h.auths[3].Unavailable = true
	h.auths[3].NextRetryAfter = until
	return h
}

func TestSkipCPACooldownSkipsActiveQueries(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := coolingClaudeHost(c, c.t.Add(time.Hour))
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	cfg := e.config()
	cfg.SkipCPACooldown = true
	e.Configure(cfg, nil)

	// A startup or scheduled round leaves the cooling credential out.
	e.poll(ctx, cfg, c.t, "", false)
	if got := h.requestsTo(providers.ClaudeUsageURL); got != 0 {
		t.Fatalf("a cooling credential must not be queried, got %d requests", got)
	}
	if _, ok := e.activeAt["3"]; ok {
		t.Fatal("a skipped credential must not count as a successful active query")
	}
	if got := h.requestsTo(providers.CodexUsageURL); got != 2 {
		t.Fatalf("the other credentials must still be queried, got %d Codex requests", got)
	}

	// The account stays in the status list with its cooldown.
	status := e.Status()
	var claude *AccountStatus
	for i := range status.Accounts {
		if status.Accounts[i].AuthIndex == "3" {
			claude = &status.Accounts[i]
		}
	}
	if claude == nil || claude.CooldownUntil.IsZero() {
		t.Fatal("the skipped credential must stay in the status list with its cooldown")
	}

	// The manual refresh button skips it as well.
	cmd := command{kind: "refresh", arg: "", reply: make(chan commandReply, 1)}
	e.runCommand(ctx, cmd)
	<-cmd.reply
	if got := h.requestsTo(providers.ClaudeUsageURL); got != 0 {
		t.Fatalf("the manual refresh must skip a cooling credential, got %d requests", got)
	}

	// Once the cooldown has ended, the query runs again.
	h.auths[3].NextRetryAfter = c.t.Add(-time.Minute)
	e.poll(ctx, cfg, c.t, "", false)
	if got := h.requestsTo(providers.ClaudeUsageURL); got != 1 {
		t.Fatalf("an ended cooldown must be queried again, got %d Claude requests", got)
	}
}

func TestSkipCPACooldownOffKeepsUpstreamBehavior(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := coolingClaudeHost(c, c.t.Add(time.Hour))
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()

	// Without the switch the cooling credential is queried as upstream does.
	e.poll(ctx, e.config(), c.t, "", false)
	if got := h.requestsTo(providers.ClaudeUsageURL); got != 1 {
		t.Fatalf("the default config must keep the upstream behavior, got %d requests", got)
	}
	if _, ok := e.activeAt["3"]; !ok {
		t.Fatal("the queried credential must count as a successful active query")
	}
}

func TestSkipCPACooldownKeepsTheLastError(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := standardHost(c)
	base := h.http
	h.http = func(req pluginapi.HTTPRequest) pluginapi.HTTPResponse {
		if req.URL == providers.ClaudeUsageURL {
			return pluginapi.HTTPResponse{StatusCode: 500, Body: []byte("boom")}
		}
		return base(req)
	}
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	e.poll(ctx, e.config(), c.t, "", false)
	saved := e.pollErrs["3"]
	if saved == "" {
		t.Fatal("the failed query must leave an error first")
	}

	// A skipped round keeps the error of the last query.
	h.http = base
	h.auths[3].Unavailable = true
	h.auths[3].NextRetryAfter = c.t.Add(time.Hour)
	cfg := e.config()
	cfg.SkipCPACooldown = true
	e.Configure(cfg, nil)
	e.poll(ctx, cfg, c.t, "", false)
	if e.pollErrs["3"] != saved {
		t.Fatalf("a skipped credential keeps the error of its last query, got %q", e.pollErrs["3"])
	}

	// The single-credential path, which probes and the manual refresh use,
	// keeps the error too.
	e.poll(ctx, cfg, c.t, "3", true)
	if e.pollErrs["3"] != saved {
		t.Fatalf("a skipped single credential keeps its old error, got %q", e.pollErrs["3"])
	}

	// Upstream behavior: a successful query clears the error again.
	h.auths[3].Unavailable = false
	cfg.SkipCPACooldown = false
	e.Configure(cfg, nil)
	e.poll(ctx, cfg, c.t, "", false)
	if e.pollErrs["3"] != "" {
		t.Fatalf("a successful query clears the error, got %q", e.pollErrs["3"])
	}
}

func TestSkipCPACooldownSkipsResetProbes(t *testing.T) {
	start := time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)
	c := &clock{t: start}
	// The reset times follow a separate clock, so they stay fixed.
	h := standardHost(&clock{t: start})
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	e.poll(ctx, e.config(), c.t, "", false)
	reset := start.Add(3 * time.Hour)

	// From now on CPA holds the Claude credential in a cooldown.
	h.auths[3].Unavailable = true
	h.auths[3].NextRetryAfter = start.Add(4 * time.Hour)
	cfg := e.config()
	cfg.SkipCPACooldown = true
	e.Configure(cfg, nil)
	e.poll(ctx, cfg, c.t, "", false)
	beforeClaude := h.requestsTo(providers.ClaudeUsageURL)
	beforeCodex := h.requestsTo(providers.CodexUsageURL)

	// The probes run with force, and must not bypass the filter.
	c.t = reset.Add(-20 * time.Second)
	e.runProbes(ctx, cfg, c.t)
	if got := h.requestsTo(providers.ClaudeUsageURL); got != beforeClaude {
		t.Fatalf("the reset probe of a cooling credential must be skipped, got %d requests", got-beforeClaude)
	}
	if got := h.requestsTo(providers.CodexUsageURL); got == beforeCodex {
		t.Fatal("the reset probes of the other credentials must still run")
	}
}

func TestSkipCPACooldownSkipsIgnitionConfirmRefresh(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := standardHost(c)
	// The ignition response carries no quota headers, so the confirm
	// refreshes the credential actively.
	h.execute = func(req pluginapi.HostModelExecutionRequest) (pluginapi.HostModelExecutionResponse, error) {
		return pluginapi.HostModelExecutionResponse{}, nil
	}
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	e.poll(ctx, e.config(), c.t, "", false)

	// CPA now holds the Claude credential in a cooldown.
	h.auths[3].Unavailable = true
	h.auths[3].NextRetryAfter = c.t.Add(2 * time.Hour)
	cfg := e.config()
	cfg.SkipCPACooldown = true
	e.Configure(cfg, nil)
	e.poll(ctx, cfg, c.t, "", false)

	// The confirm waits shrink to nothing for the test.
	delays := confirmDelays
	confirmDelays = []time.Duration{time.Millisecond}
	defer func() { confirmDelays = delays }()

	target := target{group: *e.groups["claude:3:claude:main"], cred: e.creds["3"]}
	start := c.t.Add(time.Minute)
	c.t = start
	before := h.requestsTo(providers.ClaudeUsageURL)
	err := e.confirm(ctx, cfg, target, start, pluginapi.HostModelExecutionResponse{})
	if !errors.Is(err, ignite.ErrNotConfirmed) {
		t.Fatalf("without the active refresh the ignition stays unconfirmed, got %v", err)
	}
	if got := h.requestsTo(providers.ClaudeUsageURL); got != before {
		t.Fatalf("the confirm must not query a cooling credential, got %d requests", got-before)
	}

	// Upstream behavior: the confirm refreshes the credential and confirms.
	cfg.SkipCPACooldown = false
	e.Configure(cfg, nil)
	if err := e.confirm(ctx, cfg, target, start, pluginapi.HostModelExecutionResponse{}); err != nil {
		t.Fatalf("the upstream confirm must succeed, got %v", err)
	}
	if got := h.requestsTo(providers.ClaudeUsageURL); got != before+1 {
		t.Fatalf("the upstream confirm must query the credential once, got %d requests", got-before)
	}
}

func TestSkipCPACooldownKeepsPassiveUpdates(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := coolingClaudeHost(c, c.t.Add(time.Hour))
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	cfg := e.config()
	cfg.SkipCPACooldown = true
	e.Configure(cfg, nil)
	e.poll(ctx, cfg, c.t, "", false)

	// Passive updates still reach the skipped credential.
	e.applyUsage(ctx, usageEvent{provider: "claude", authIndex: "3", header: http.Header{
		"Anthropic-Ratelimit-Unified-5h-Utilization": {"0.35"},
		"Anthropic-Ratelimit-Unified-5h-Reset":       {strconv.FormatInt(c.t.Add(3*time.Hour).Unix(), 10)},
	}})
	if window := e.Status().Accounts[2].Groups[0].Windows[0]; window.Source != "passive" || window.Remaining != 65 {
		t.Fatalf("the passive window of the skipped credential %+v", window)
	}

	// Further rounds still skip the active query.
	e.poll(ctx, cfg, c.t, "", false)
	if got := h.requestsTo(providers.ClaudeUsageURL); got != 0 {
		t.Fatalf("the cooling credential must stay out of the active queries, got %d requests", got)
	}
}

func TestStatusSinksCoolingCredentials(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 4, 10, 0, 0, 0, shanghai)}
	h := standardHost(c)
	e, _ := newTestEngine(t, h, c)
	ctx := context.Background()
	e.poll(ctx, e.config(), c.t, "", false)

	titles := func() string {
		var out []string
		for _, account := range e.Status().Accounts {
			out = append(out, account.Title)
		}
		return strings.Join(out, ",")
	}
	codexPair := func() []string {
		var out []string
		for _, account := range e.Status().Accounts {
			if account.Provider == "codex" {
				out = append(out, account.Title)
			}
		}
		return out
	}
	base := titles()
	pair := codexPair()
	if len(pair) != 2 || !strings.HasSuffix(base, ",Claude,Antigravity") {
		t.Fatalf("baseline order %s", base)
	}

	// A cooling credential sinks to the end of its provider group.
	e.mu.Lock()
	e.creds["2"].CooldownUntil = c.t.Add(time.Hour)
	e.mu.Unlock()
	if got := codexPair(); got[0] != pair[1] || got[1] != pair[0] {
		t.Fatalf("the cooling credential must sink, got %v, want %v", got, []string{pair[1], pair[0]})
	}

	// A cooldown that has ended restores the suffix order.
	e.mu.Lock()
	e.creds["2"].CooldownUntil = c.t.Add(-time.Minute)
	e.mu.Unlock()
	if got := codexPair(); got[0] != pair[0] || got[1] != pair[1] {
		t.Fatalf("an ended cooldown restores the order, got %v, want %v", got, pair)
	}

	// A cooling credential stays within its provider group: the provider
	// rank decides before the cooldown does.
	e.mu.Lock()
	e.creds["3"].CooldownUntil = c.t.Add(time.Hour)
	e.mu.Unlock()
	if got := titles(); got != base {
		t.Fatalf("the provider rank must keep its order, got %s, want %s", got, base)
	}
}
