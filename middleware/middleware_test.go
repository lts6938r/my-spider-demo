package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"my-spider-demo/spider"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// testLogger 把日志全部丢弃，让测试输出干净。
var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func okHandle(r *spider.Request) (*spider.Response, error) {
	return &spider.Response{StatusCode: http.StatusOK, Request: r}, nil
}

func failHandle(*spider.Request) (*spider.Response, error) {
	return nil, io.ErrUnexpectedEOF
}

func TestChainOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mk := func(name string) Middleware {
		return func(next Handle) Handle {
			return func(r *spider.Request) (*spider.Response, error) {
				mu.Lock()
				order = append(order, name+".req")
				mu.Unlock()
				resp, err := next(r)
				mu.Lock()
				order = append(order, name+".resp")
				mu.Unlock()
				return resp, err
			}
		}
	}

	h := Chain(okHandle, mk("a"), mk("b"))
	if _, err := h(&spider.Request{}); err != nil {
		t.Fatal(err)
	}

	want := []string{"a.req", "b.req", "b.resp", "a.resp"}
	if !reflect.DeepEqual(order, want) {
		t.Errorf("洋葱顺序 = %v, want %v（Mws[0] 应最外层）", order, want)
	}
}

func TestUserAgentMiddleware(t *testing.T) {
	h := UserAgentMiddleware("test-ua")(okHandle)

	r, _ := spider.NewRequest("", "https://example.com/a", nil)
	h(r)
	if got := r.Header.Get("User-Agent"); got != "test-ua" {
		t.Errorf("未设置 UA 时应补默认, got %q", got)
	}

	r2, _ := spider.NewRequest("", "https://example.com/b", nil)
	r2.Header.Set("User-Agent", "mine")
	h(r2)
	if got := r2.Header.Get("User-Agent"); got != "mine" {
		t.Errorf("已有 UA 不应被覆盖, got %q", got)
	}

	// 手工构造、Header 为 nil 的请求不应 panic
	r3 := &spider.Request{URL: "https://example.com/c"}
	if _, err := h(r3); err != nil {
		t.Fatal(err)
	}
	if got := r3.Header.Get("User-Agent"); got != "test-ua" {
		t.Errorf("nil Header 应初始化, got %q", got)
	}
}

func TestLoggingMiddleware(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, nil))
	h := LoggingMiddleware(lg)(okHandle)

	r, _ := spider.NewRequest("", "https://example.com/x", nil)
	r.ID = 7
	h(r)

	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("日志不是合法 JSON: %v, got %q", err, buf.String())
	}
	if rec["level"] != "INFO" || rec["msg"] != "download" {
		t.Errorf("level/msg = %v/%v", rec["level"], rec["msg"])
	}
	if rec["url"] != "https://example.com/x" || rec["id"] != float64(7) {
		t.Errorf("url/id = %v/%v", rec["url"], rec["id"])
	}
	if rec["status"] != float64(http.StatusOK) {
		t.Errorf("status = %v", rec["status"])
	}

	buf.Reset()
	hf := LoggingMiddleware(lg)(failHandle)
	hf(r)
	rec = nil
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec)
	if rec["level"] != "ERROR" || rec["err"] == nil {
		t.Errorf("失败日志应为 ERROR 且带 err: %v", rec)
	}
}
