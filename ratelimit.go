package spider

import (
	"context"
	"math"
	"net/url"
	"sync"
	"time"
)

// RateLimitConfig：GlobalQPS <= 0 表示不限；
// DomainQPS 只对列出的域名生效（example.com → 2 表示该域名 2 req/s）。
type RateLimitConfig struct {
	GlobalQPS float64
	DomainQPS map[string]float64
}

// tokenBucket 标准令牌桶：按速率连续补充令牌，容量为 burst；
// Wait 拿不到令牌就睡到攒够为止，可被 ctx 打断。
// 自写而非直接用 golang.org/x/time/rate：令牌桶核心只有几十行，
// 亲手实现一次（补充、突发容量、ctx 打断）比引依赖更值得；
// V4 若需要分布式限流再评估引入。
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // 每秒补充的令牌数
	burst  float64 // 桶容量：允许的瞬时突发量
	tokens float64
	last   time.Time
}

func newTokenBucket(qps float64) *tokenBucket {
	burst := math.Ceil(qps) // 容量取 QPS 向上取整：限 5/s 允许瞬间打 5 发
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{rate: qps, burst: burst, tokens: burst, last: time.Now()}
}

func (t *tokenBucket) Wait(ctx context.Context) error {
	for {
		t.mu.Lock()
		now := time.Now()
		t.tokens = min(t.burst, t.tokens+now.Sub(t.last).Seconds()*t.rate)
		t.last = now
		if t.tokens >= 1 {
			t.tokens--
			t.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - t.tokens) / t.rate * float64(time.Second))
		t.mu.Unlock()

		select {
		case <-time.After(wait):
			// 定时器可能早醒、或有并发者抢走刚补充的令牌，回循环重新核算
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RateLimitMiddleware 在真实发包前获取令牌（全局 + 域名两级，依次获取）。
// 放在 Retry 内层：每次真实发包（含重试）都要重新过限流，
// 否则重试会绕过限流把压力打回服务器。
func RateLimitMiddleware(cfg RateLimitConfig) Middleware {
	var (
		mu      sync.Mutex
		domains = make(map[string]*tokenBucket)
	)
	var global *tokenBucket
	if cfg.GlobalQPS > 0 {
		global = newTokenBucket(cfg.GlobalQPS)
	}
	return func(next Handle) Handle {
		return func(r *Request) (*Response, error) {
			ctx := r.Context()
			if global != nil {
				if err := global.Wait(ctx); err != nil {
					return nil, err
				}
			}
			host := hostOf(r)
			if _, limited := cfg.DomainQPS[host]; limited {
				mu.Lock()
				b := domains[host]
				if b == nil {
					b = newTokenBucket(cfg.DomainQPS[host])
					domains[host] = b
				}
				mu.Unlock()
				if err := b.Wait(ctx); err != nil {
					return nil, err
				}
			}
			return next(r)
		}
	}
}

func hostOf(r *Request) string {
	u, err := url.Parse(r.URL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
