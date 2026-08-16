package middleware

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"my-spider-demo/spider"
	"net/http"
	"time"
)

// RetryConfig 控制重试行为。
type RetryConfig struct {
	MaxAttempts int           // 总尝试次数（含首次），<1 视为 1
	BaseDelay   time.Duration // 第一次重试的等待基数
	MaxDelay    time.Duration // 单次等待上限（含抖动）
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   500 * time.Millisecond,
		MaxDelay:    10 * time.Second,
	}
}

// RetryMiddleware 按错误类型决定是否重试：
//
//   - 传输错误（连接拒绝/重置、超时等）→ 重试
//   - 429 / 500 / 502 / 503 / 504 → 重试
//   - 其余 4xx（400/404…）→ 不重试：服务器明确拒绝，重试无意义
//   - 请求 ctx 已取消/超时 → 不重试：ctx 已死，再试只会立即失败
//
// 等待采用指数退避 + 抖动（见 backoffDelay）。
// 建议放在 RateLimit 外层：每次真实发包（含重试）都重新过限流。
func RetryMiddleware(cfg RetryConfig, lg *slog.Logger) Middleware {
	if lg == nil {
		lg = slog.Default()
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return func(next Handle) Handle {
		return func(r *spider.Request) (*spider.Response, error) {
			var (
				resp *spider.Response
				err  error
			)
			attempt := 0
			for attempt < cfg.MaxAttempts {
				attempt++
				resp, err = next(r)

				reason := retryReason(r, resp, err)
				if reason == "" || attempt >= cfg.MaxAttempts {
					break
				}
				delay := backoffDelay(cfg, attempt)
				lg.Warn("retry scheduled",
					"id", r.ID, "url", r.BuildURL(),
					"attempt", attempt, "delay", delay, "reason", reason)
				select {
				case <-time.After(delay):
				case <-r.Context().Done():
					return nil, fmt.Errorf("spider: cancelled during retry backoff: %w", r.Context().Err())
				}
			}
			if r.Meta == nil {
				r.Meta = make(map[string]any)
			}
			r.Meta["attempts"] = attempt
			return resp, err
		}
	}
}

// retryReason 返回是否重试及原因描述，空串表示不重试。
// 用 r.Context().Err() 判断"请求自身预算已耗尽"：
// 用户级取消/超时不重试；而 Client.Timeout 这类网络超时
// 不会置位请求 ctx，仍会按传输错误重试——两者必须区分。
func retryReason(r *spider.Request, resp *spider.Response, err error) string {
	if err != nil {
		if r.Context().Err() != nil {
			return ""
		}
		return "transport error"
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return "http 429"
	case 500, 502, 503, 504:
		return fmt.Sprintf("http %d", resp.StatusCode)
	}
	return ""
}

// backoffDelay 指数退避：BaseDelay * 2^(attempt-1)，
// 叠加 [0, 25%) 抖动后以 MaxDelay 封顶。
// 抖动防止多个客户端在同一时刻集中重试（thundering herd）。
func backoffDelay(cfg RetryConfig, attempt int) time.Duration {
	d := cfg.BaseDelay << (attempt - 1) // attempt 从 1 计
	if d < cfg.BaseDelay {              // int64 左移溢出变负，视为到达上限
		d = cfg.MaxDelay
	}
	if q := d / 4; q > 0 {
		d += rand.N(q)
	}
	if d > cfg.MaxDelay {
		d = cfg.MaxDelay
	}
	return d
}
