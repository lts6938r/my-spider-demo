package middleware

import (
	"log/slog"
	"my-spider-demo/spider"
	"net/http"
	"time"
)

// Handle 是"执行一次下载"的函数形态，与 Downloader.Download 同构。
type Handle func(r *spider.Request) (*spider.Response, error)

// Middleware 把一个 Handle 包装成另一个 Handle。
//
// next(r) 之前的代码是请求阶段（改 Header、限流、记日志），
// next(r) 之后（或 defer）是响应阶段——天然的洋葱模型：
//
//	Logging(最外) → Retry → RateLimit → UserAgent(最内) → Downloader
//
// 为什么是函数包装、而不是 ProcessRequest/ProcessResponse 双钩子接口：
// Retry 必须能"重新发起"一次下载，只有包装调用本身才做得到，
// 观察式的钩子表达不了这种控制流。
type Middleware func(next Handle) Handle

// Chain 依序包装：mws[0] 最外层（最先处理请求、最后处理响应）。
// 返回值可直接当下载函数调用。
func Chain(h Handle, mws ...Middleware) Handle {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// UserAgentMiddleware 为未显式设置 UA 的请求补默认值。
func UserAgentMiddleware(ua string) Middleware {
	return func(next Handle) Handle {
		return func(r *spider.Request) (*spider.Response, error) {
			if r.Header == nil {
				r.Header = make(http.Header)
			}
			if r.Header.Get("User-Agent") == "" {
				r.Header.Set("User-Agent", ua)
			}
			return next(r)
		}
	}
}

// LoggingMiddleware 记录一次（逻辑）下载的结果：
// id / method / url / status / cost / attempts / err。
// 放在链最外层时，cost 包含限流等待与重试耗时，
// 配合 Retry 的逐次日志可回答"这个 URL 为什么慢 / 为什么最终失败"。
func LoggingMiddleware(lg *slog.Logger) Middleware {
	if lg == nil {
		lg = slog.Default()
	}
	return func(next Handle) Handle {
		return func(r *spider.Request) (*spider.Response, error) {
			start := time.Now()
			resp, err := next(r)

			attrs := []any{
				"id", r.ID,
				"method", r.Method,
				"url", r.BuildURL(),
				"cost_ms", time.Since(start).Milliseconds(),
			}
			if resp != nil {
				attrs = append(attrs, "status", resp.StatusCode)
			}
			if n, ok := r.Meta["attempts"].(int); ok && n > 1 {
				attrs = append(attrs, "attempts", n)
			}
			if err != nil {
				attrs = append(attrs, "err", err)
				lg.Error("download failed", attrs...)
				return resp, err
			}
			lg.Info("download", attrs...)
			return resp, err
		}
	}
}
