package engine

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"my-spider-demo/dedup"
	"my-spider-demo/downloader"
	"my-spider-demo/middleware"
	"my-spider-demo/pipeline"
	"my-spider-demo/scheduler"
	"time"
)

// Config 是 V2 的全部可配置项。
// 只收录有真实消费者的键——Worker（V3）、Proxy（V4）、
// Output（文件管道出现时）再进配置，避免死配置。
type Config struct {
	Worker         int                `json:"worker"`       // 并发 worker 数，<=0 → DefaultWorkers
	Scheduler      string             `json:"scheduler"`    // "fifo"(默认) | "priority"
	Downloader     string             `json:"downloader"`   // "http"(默认) | "cdp"（真实浏览器）
	RenderWait     Duration           `json:"render_wait"`  // CDP：load 后额外渲染等待
	SaveDir        string             `json:"save_dir"`     // 非空则把响应原文保存到该目录
	Output         string             `json:"output"`       // 非空则追加 JSONL 文件管道
	SQLite         string             `json:"sqlite"`       // 非空则追加 SQLite 管道（数据库文件路径）
	SQLiteTable    string             `json:"sqlite_table"` // SQLite 表名，默认 items
	Checkpoint     string             `json:"checkpoint"`   // 断点续爬状态文件；非空时覆盖 duper 参数
	UserAgent      string             `json:"user_agent"`
	Timeout        Duration           `json:"timeout"`          // 单请求整体超时
	MaxAttempts    int                `json:"max_attempts"`     // 重试总次数（含首次）
	RetryBaseDelay Duration           `json:"retry_base_delay"` // 重试退避基数
	RetryMaxDelay  Duration           `json:"retry_max_delay"`  // 重试等待上限
	GlobalQPS      float64            `json:"global_qps"`       // <=0 不限
	DomainQPS      map[string]float64 `json:"domain_qps"`       // 只对列出的域名生效
	LogLevel       string             `json:"log_level"`        // debug/info/warn/error
}

func DefaultConfig() *Config {
	return &Config{
		Worker:         DefaultWorkers,
		Scheduler:      "fifo",
		UserAgent:      downloader.DefaultUserAgent,
		Timeout:        Duration{30 * time.Second},
		MaxAttempts:    3,
		RetryBaseDelay: Duration{500 * time.Millisecond},
		RetryMaxDelay:  Duration{10 * time.Second},
		LogLevel:       "info",
	}
}

// Duration 支持 JSON 字符串写法（"5s"、"200ms"）。
// time.Duration 原生只能从纳秒数字解析，对人不可读，这里收窄为字符串。
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf(`spider: duration 必须是字符串（如 "5s"）: %w`, err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("spider: 解析 duration %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

// LoadConfig 读入 JSON 配置文件，缺失的键保留默认值；path 为空返回默认配置。
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("spider: read config: %w", err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("spider: parse config %s: %w", path, err)
	}
	return cfg, nil
}

// NewEngineFromConfig 用一份配置组装整个引擎（含中间件链的固定顺序）：
//
//	Logging(外) → Retry → RateLimit → UserAgent(内) → Downloader
//
// 顺序即语义：Logging 在最外看全程耗时；Retry 在 RateLimit 之外，
// 每次真实发包（含重试）都重新过限流。
// duper 传 nil 则不去重。
// 返回 error：CDP 模式启动浏览器、SQLite 打开数据库都可能失败，
// 组装失败应在运行前暴露而非运行中崩溃。
func NewEngineFromConfig(cfg *Config, duper dedup.Duper, pipes ...pipeline.Pipeline) (*Engine, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	lg := newLogger(cfg.LogLevel)

	// 断点续爬：配置了状态文件即启用 Checkpoint 去重器（覆盖传入的 duper）
	if cfg.Checkpoint != "" {
		duper = dedup.NewCheckpoint(cfg.Checkpoint)
	}

	var mws []middleware.Middleware
	mws = append(mws, middleware.LoggingMiddleware(lg))
	// SaveResponse 放在 Retry 外层：只保存最终成功的响应，
	// 重试途中的 429/5xx 中间产物不落盘
	if cfg.SaveDir != "" {
		mws = append(mws, middleware.SaveResponseMiddleware(cfg.SaveDir, lg))
	}
	if cfg.MaxAttempts > 1 {
		mws = append(mws, middleware.RetryMiddleware(middleware.RetryConfig{
			MaxAttempts: cfg.MaxAttempts,
			BaseDelay:   cfg.RetryBaseDelay.Duration,
			MaxDelay:    cfg.RetryMaxDelay.Duration,
		}, lg))
	}
	if cfg.GlobalQPS > 0 || len(cfg.DomainQPS) > 0 {
		mws = append(mws, middleware.RateLimitMiddleware(middleware.RateLimitConfig{
			GlobalQPS: cfg.GlobalQPS,
			DomainQPS: cfg.DomainQPS,
		}))
	}
	if cfg.UserAgent != "" {
		mws = append(mws, middleware.UserAgentMiddleware(cfg.UserAgent))
	}

	// 下载器可替换：HTTP（net/http）、CDP（真实浏览器）或
	// fallback（HTTP 优先，失败/被拦截时惰性降级 CDP），引擎无感知
	var down downloader.Downloader = downloader.NewHTTPDownloader(cfg.Timeout.Duration)
	switch strings.ToLower(cfg.Downloader) {
	case "cdp":
		d, err := downloader.NewCDPDownloader(downloader.CDPConfig{
			Headless: true,
			Timeout:  cfg.Timeout.Duration,
			Wait:     cfg.RenderWait.Duration,
		})
		if err != nil {
			return nil, err
		}
		down = d
	case "fallback":
		down = downloader.NewFallbackDownloader(
			downloader.NewHTTPDownloader(cfg.Timeout.Duration),
			downloader.NewLazyCDP(downloader.CDPConfig{
				Headless: true,
				Timeout:  cfg.Timeout.Duration,
				Wait:     cfg.RenderWait.Duration,
			}))
	}

	// 调度器可替换：同一 Scheduler 接口的两种实现，引擎无感知
	var sched scheduler.Scheduler = scheduler.NewFIFOScheduler()
	if strings.EqualFold(cfg.Scheduler, "priority") {
		sched = scheduler.NewPriorityQueueScheduler()
	}

	// Output 非空：追加 jsonl 文件管道（引擎退出时自动 Close）
	if cfg.Output != "" {
		pipes = append(pipes, pipeline.NewJSONLFilePipeline(cfg.Output))
	}
	// SQLite 非空：追加 SQLite 管道（打开失败立即报错）
	if cfg.SQLite != "" {
		sqlp, err := pipeline.NewSQLitePipeline(cfg.SQLite, cfg.SQLiteTable)
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, sqlp)
	}

	return &Engine{
		Sched:   sched,
		Down:    down,
		Mws:     mws,
		Pipes:   pipes,
		Duper:   duper,
		Logger:  lg,
		Workers: cfg.Worker,
	}, nil
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lv}))
}
