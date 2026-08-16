package spider

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.UserAgent != DefaultUserAgent || cfg.Timeout.Duration != 30*time.Second {
		t.Errorf("defaults = %+v", cfg)
	}
	if cfg.MaxAttempts != 3 || cfg.RetryBaseDelay.Duration != 500*time.Millisecond {
		t.Errorf("retry defaults = %+v", cfg)
	}
}

func TestLoadConfigOverridesAndKeepsDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"worker": 4,
		"user_agent": "ua-x",
		"timeout": "5s",
		"domain_qps": {"a.com": 2}
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker != 4 {
		t.Errorf("worker = %d, want 4", cfg.Worker)
	}
	if cfg.UserAgent != "ua-x" {
		t.Errorf("user_agent = %q", cfg.UserAgent)
	}
	if cfg.Timeout.Duration != 5*time.Second {
		t.Errorf("timeout = %v, want 5s（字符串解析）", cfg.Timeout.Duration)
	}
	if q, ok := cfg.DomainQPS["a.com"]; !ok || q != 2 {
		t.Errorf("domain_qps = %v", cfg.DomainQPS)
	}
	// 文件里没写的键保留默认值
	if cfg.MaxAttempts != 3 || cfg.LogLevel != "info" {
		t.Errorf("缺失键应保留默认: %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	if _, err := LoadConfig(""); err != nil {
		t.Errorf("空路径应直接返回默认配置: %v", err)
	}
	if _, err := LoadConfig("no/such/file.json"); err == nil {
		t.Error("文件不存在应报错")
	}

	path := filepath.Join(t.TempDir(), "bad.json")
	os.WriteFile(path, []byte(`{"timeout": "5x"}`), 0o644)
	if _, err := LoadConfig(path); err == nil {
		t.Error(`"5x" 不是合法 duration，应报错`)
	}
}

func TestNewEngineFromConfigAssemblesChain(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GlobalQPS = 5
	cfg.Worker = 3
	cfg.Scheduler = "priority"
	cfg.Output = filepath.Join(t.TempDir(), "out.jsonl")
	cfg.SQLite = filepath.Join(t.TempDir(), "out.db")
	eng, err := NewEngineFromConfig(cfg, NewHashSetDuper(), ConsolePipeline{})
	if err != nil {
		t.Fatal(err)
	}
	// 引擎未 Run 就不会走 closeResources；Windows 下未关闭的
	// SQLite 句柄会锁住 TempDir 导致清理失败，测试里显式收尾。
	defer eng.closeResources()

	if len(eng.Mws) != 4 { // Logging + Retry + RateLimit + UA
		t.Errorf("Mws 数量 = %d, want 4", len(eng.Mws))
	}
	if eng.Workers != 3 {
		t.Errorf("Workers = %d, want 3（配置传导）", eng.Workers)
	}
	if _, ok := eng.Sched.(*PriorityQueueScheduler); !ok {
		t.Errorf("Sched 类型 = %T, want *PriorityQueueScheduler（配置选择）", eng.Sched)
	}
	if _, ok := eng.Down.(*HTTPDownloader); !ok {
		t.Errorf("Down 类型 = %T, want *HTTPDownloader（默认）", eng.Down)
	}
	if len(eng.Pipes) != 3 { // Console + JSONL + SQLite
		t.Errorf("Pipes 数量 = %d, want 3（Output/SQLite 追加管道）", len(eng.Pipes))
	}
	if eng.Duper == nil || eng.Logger == nil || eng.Down == nil || eng.Sched == nil {
		t.Error("引擎组件未装配完整")
	}

	// 未开启限流时链条应少一层
	cfg2 := DefaultConfig()
	cfg2.GlobalQPS = 0
	eng2, err := NewEngineFromConfig(cfg2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(eng2.Mws) != 3 {
		t.Errorf("无限流配置时 Mws 数量 = %d, want 3", len(eng2.Mws))
	}
}
