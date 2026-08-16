package middleware

import (
	"context"
	"fmt"
	"my-spider-demo/downloader"
	"my-spider-demo/spider"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

var fastRetry = RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}

// statusThenOK：前 n 次返回 code，之后 200 "ok"。
func statusThenOK(n int32, code int) http.HandlerFunc {
	var calls atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= n {
			w.WriteHeader(code)
			return
		}
		fmt.Fprint(w, "ok")
	}
}

func alwaysStatus(code int, calls *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(code)
	}
}

func TestRetryRecoversAfterServerErrors(t *testing.T) {
	srv := httptest.NewServer(statusThenOK(2, http.StatusServiceUnavailable))
	defer srv.Close()

	r, _ := spider.NewRequest("", srv.URL, nil)
	h := RetryMiddleware(fastRetry, testLogger)(downloader.NewHTTPDownloader(5 * time.Second).Download)

	resp, err := h(r)
	if err != nil {
		t.Fatalf("重试后应成功: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(resp.Body) != "ok" {
		t.Errorf("resp = %d %q", resp.StatusCode, resp.Body)
	}
	if n := r.Meta["attempts"].(int); n != 3 {
		t.Errorf("attempts = %d, want 3", n)
	}
}

func TestRetryExhaustsOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(alwaysStatus(http.StatusBadGateway, &calls))
	defer srv.Close()

	r, _ := spider.NewRequest("", srv.URL, nil)
	h := RetryMiddleware(fastRetry, testLogger)(downloader.NewHTTPDownloader(5 * time.Second).Download)
	resp, err := h(r)
	if err != nil {
		t.Fatalf("耗尽重试不应返回 error（拿到 5xx 响应即传输成功）: %v", err)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("应返回最后一次响应, got %d", resp.StatusCode)
	}
	if calls.Load() != 3 {
		t.Errorf("实际请求 %d 次, want 3（MaxAttempts）", calls.Load())
	}
}

func TestNoRetryOn4xx(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound} {
		var calls atomic.Int32
		srv := httptest.NewServer(alwaysStatus(code, &calls))

		r, _ := spider.NewRequest("", srv.URL, nil)
		h := RetryMiddleware(fastRetry, testLogger)(downloader.NewHTTPDownloader(5 * time.Second).Download)
		h(r)
		srv.Close()

		if calls.Load() != 1 {
			t.Errorf("status %d: 实际请求 %d 次, want 1（4xx 不重试）", code, calls.Load())
		}
	}
}

func TestNoRetryWhenRequestContextDead(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(alwaysStatus(http.StatusOK, &calls))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预先取消：传输必然失败，且不应触发重试

	r, _ := spider.NewRequest("", srv.URL, nil)
	r = r.WithContext(ctx)
	h := RetryMiddleware(fastRetry, testLogger)(downloader.NewHTTPDownloader(5 * time.Second).Download)

	_, err := h(r)
	if err == nil {
		t.Fatal("ctx 已取消应返回 error")
	}
	if calls.Load() != 0 {
		t.Errorf("死 ctx 下真实发包 %d 次, want 0", calls.Load())
	}
	if n := r.Meta["attempts"].(int); n != 1 {
		t.Errorf("attempts = %d, want 1（ctx 已死不重试）", n)
	}
}

func TestBackoffDelayBounds(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 100 * time.Millisecond, MaxDelay: 350 * time.Millisecond}
	for attempt := 1; attempt <= 3; attempt++ {
		base := cfg.BaseDelay << (attempt - 1)
		if base > cfg.MaxDelay || base <= 0 {
			base = cfg.MaxDelay
		}
		upper := base + base/4
		if upper > cfg.MaxDelay {
			upper = cfg.MaxDelay
		}
		for i := 0; i < 200; i++ {
			d := backoffDelay(cfg, attempt)
			if d < base || d > upper {
				t.Fatalf("attempt %d: delay = %v, want [%v, %v]", attempt, d, base, upper)
			}
		}
	}
	// 深层重试必须封顶在 MaxDelay（指数溢出也不能突破）
	for i := 0; i < 50; i++ {
		if d := backoffDelay(cfg, 40); d != cfg.MaxDelay {
			t.Fatalf("attempt 40: delay = %v, want %v", d, cfg.MaxDelay)
		}
	}
}
