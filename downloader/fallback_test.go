package downloader

import (
	"errors"
	"fmt"
	"my-spider-demo/spider"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type stubDownloader struct {
	calls atomic.Int32
	resp  *spider.Response
	err   error
}

func (s *stubDownloader) Download(r *spider.Request) (*spider.Response, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return &spider.Response{StatusCode: s.resp.StatusCode, Body: s.resp.Body, Request: r}, nil
}

func stubWith(status int) *stubDownloader {
	return &stubDownloader{resp: &spider.Response{StatusCode: status, Body: []byte("from-stub")}}
}

func TestFallbackOnTransportError(t *testing.T) {
	primary := &stubDownloader{err: errors.New("conn refused")}
	secondary := stubWith(200)
	f := NewFallbackDownloader(primary, secondary)

	r, _ := spider.NewRequest("", "https://example.com/a", nil)
	resp, err := f.Download(r)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || string(resp.Body) != "from-stub" {
		t.Errorf("应返回降级侧响应, got %d %q", resp.StatusCode, resp.Body)
	}
	if secondary.calls.Load() != 1 {
		t.Errorf("降级侧应被调用 1 次, got %d", secondary.calls.Load())
	}
}

func TestFallbackOnBlockedStatus(t *testing.T) {
	for _, code := range []int{403, 429} {
		primary := stubWith(code)
		secondary := stubWith(200)
		f := NewFallbackDownloader(primary, secondary)

		r, _ := spider.NewRequest("", "https://example.com/a", nil)
		resp, err := f.Download(r)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Errorf("status %d 应触发降级, got 最终 %d", code, resp.StatusCode)
		}
	}
}

func TestNoFallbackWhenPrimaryOK(t *testing.T) {
	primary := stubWith(200)
	secondary := stubWith(200)
	f := NewFallbackDownloader(primary, secondary)

	r, _ := spider.NewRequest("", "https://example.com/a", nil)
	resp, err := f.Download(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "from-stub" { /* 两 stub 内容一致，改为看调用数 */
		_ = resp
	}
	if secondary.calls.Load() != 0 {
		t.Errorf("首选成功不应触发降级, 降级侧被调用 %d 次", secondary.calls.Load())
	}
	if primary.calls.Load() != 1 {
		t.Errorf("首选应被调用 1 次, got %d", primary.calls.Load())
	}
}

func TestFallbackOther4xxNotBlocked(t *testing.T) {
	// 404/500 不属于反爬拦截，不降级（语义由上层重试/解析处理）
	primary := stubWith(404)
	secondary := stubWith(200)
	f := NewFallbackDownloader(primary, secondary)

	r, _ := spider.NewRequest("", "https://example.com/a", nil)
	resp, err := f.Download(r)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 404 || secondary.calls.Load() != 0 {
		t.Errorf("404 不应降级: got %d, 降级调用 %d 次", resp.StatusCode, secondary.calls.Load())
	}
}

func TestFallbackSecondaryErrorWins(t *testing.T) {
	primary := &stubDownloader{err: errors.New("primary down")}
	secondary := &stubDownloader{err: errors.New("cdp blocked")}
	f := NewFallbackDownloader(primary, secondary)

	r, _ := spider.NewRequest("", "https://example.com/a", nil)
	_, err := f.Download(r)
	if err == nil || err.Error() != "cdp blocked" {
		t.Errorf("双侧失败应返回降级侧错误, got %v", err)
	}
}

type closerStub struct {
	stubDownloader
	closed atomic.Int32
}

func (c *closerStub) Close() error { c.closed.Add(1); return nil }

func TestFallbackCloseBoth(t *testing.T) {
	primary := &closerStub{stubDownloader: stubDownloader{resp: &spider.Response{StatusCode: 200}}}
	secondary := &closerStub{stubDownloader: stubDownloader{resp: &spider.Response{StatusCode: 200}}}
	f := NewFallbackDownloader(primary, secondary)

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if primary.closed.Load() != 1 || secondary.closed.Load() != 1 {
		t.Error("两侧 io.Closer 都应被关闭")
	}
}

// e2e：首选恒 403（反爬拦截），降级侧用真实 Chrome 抓本地 JS 渲染页——
// 验证降级链路在真实 CDP 下端到端可用。机器无 Chrome 时跳过。
func TestFallbackE2ERealCDP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, jsPage)
	}))
	defer srv.Close()

	cdp, err := NewCDPDownloader(CDPConfig{
		Headless: true,
		Timeout:  15 * time.Second,
		Wait:     600 * time.Millisecond,
	})
	if err != nil {
		t.Skipf("本机无法启动 Chrome，跳过: %v", err)
	}
	defer cdp.Close()

	blocked := stubWith(403)
	f := NewFallbackDownloader(blocked, cdp)

	r, _ := spider.NewRequest("", srv.URL, nil)
	resp, err := f.Download(r)
	if err != nil {
		t.Fatalf("降级抓取失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200（来自 CDP）", resp.StatusCode)
	}
	if !strings.Contains(resp.Text(), "rendered-by-js") {
		t.Errorf("降级响应应为 JS 渲染后的 DOM: %.200s", resp.Text())
	}
	if blocked.calls.Load() != 1 {
		t.Errorf("首选应先被尝试, got %d 次", blocked.calls.Load())
	}
}
