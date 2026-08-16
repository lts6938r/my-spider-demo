package middleware

import (
	"context"
	"errors"
	"testing"

	"my-spider-demo/spider"
	"time"
)

func TestTokenBucketBurstThenRefill(t *testing.T) {
	b := newTokenBucket(10) // burst = 10

	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := b.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("突发 10 个不应等待, elapsed %v", elapsed)
	}

	// 第 11 个要等约 100ms 才补充出 1 个令牌
	start = time.Now()
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Errorf("桶空后应等待约 100ms, elapsed %v", elapsed)
	}
}

func TestTokenBucketContextCancel(t *testing.T) {
	b := newTokenBucket(1)
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	} // 耗尽令牌

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := b.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("want DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Error("等待应被 ctx 及时打断")
	}
}

func TestRateLimitMiddlewareDomainLimited(t *testing.T) {
	// 127.0.0.1 限 2 req/s：前两个立即放行，第三个要等约 500ms。
	cfg := RateLimitConfig{DomainQPS: map[string]float64{"127.0.0.1": 2}}
	h := RateLimitMiddleware(cfg)(okHandle)

	start := time.Now()
	h(&spider.Request{URL: "https://127.0.0.1/a"})
	h(&spider.Request{URL: "https://127.0.0.1/b"})
	if fast := time.Since(start); fast > 200*time.Millisecond {
		t.Errorf("burst 内的两个请求不应等待, elapsed %v", fast)
	}

	h(&spider.Request{URL: "https://127.0.0.1/c"})
	if total := time.Since(start); total < 350*time.Millisecond {
		t.Errorf("第三个请求应被限流, elapsed %v", total)
	}
}

func TestRateLimitMiddlewareUnlimitedDomain(t *testing.T) {
	// 只限制了 example.com；本机请求不受影响，也不应有全局限流。
	cfg := RateLimitConfig{DomainQPS: map[string]float64{"example.com": 1}}
	h := RateLimitMiddleware(cfg)(okHandle)

	start := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := h(&spider.Request{URL: "https://127.0.0.1/x"}); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("未限域名不应等待, elapsed %v", elapsed)
	}
}
