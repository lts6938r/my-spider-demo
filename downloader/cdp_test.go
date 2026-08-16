package downloader

import (
	"context"
	"fmt"
	"my-spider-demo/spider"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// jsPage 返回一个"源码里没有、必须执行 JS 才出现"的页面：
// 这是 CDP 下载器区别于 HTTP 下载器的根本证明。
const jsPage = `<!doctype html>
<html><body>
<div id="x">loading</div>
<script>setTimeout(function(){ document.getElementById("x").textContent = "rendered-by-js"; }, 100);</script>
</body></html>`

// newCDPorSkip 启动 CDP 下载器；机器上没有 Chrome 时跳过测试。
func newCDPorSkip(t *testing.T) *CDPDownloader {
	t.Helper()
	d, err := NewCDPDownloader(CDPConfig{
		Headless: true,
		Timeout:  15 * time.Second,
		Wait:     600 * time.Millisecond, // 等 setTimeout(100ms) 上屏
	})
	if err != nil {
		t.Skipf("本机无法启动 Chrome，跳过 CDP 测试: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestCDPDownloaderRendersJS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, jsPage)
	}))
	defer srv.Close()

	d := newCDPorSkip(t)
	req, err := spider.NewRequest("", srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.Download(req)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
	if resp.Request != req {
		t.Error("Response 应回溯引用 Request")
	}
	if !strings.Contains(resp.Text(), "rendered-by-js") {
		t.Errorf("渲染后的 DOM 未包含 JS 写入的内容（JS 未执行?）: %.200s", resp.Text())
	}
	if resp.Cost <= 0 {
		t.Error("Cost 应大于 0")
	}
}

func TestCDPDownloaderHTTPStatusIsNotError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	d := newCDPorSkip(t)
	req, _ := spider.NewRequest("", srv.URL+"/missing", nil)
	resp, err := d.Download(req)
	if err != nil {
		t.Fatalf("404 不应视为传输错误: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want 404", resp.StatusCode)
	}
}

func TestCDPDownloaderHonorsRequestContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, jsPage) // 有 600ms 渲染等待，足够窗口触发取消
	}))
	defer srv.Close()

	d := newCDPorSkip(t)
	req, _ := spider.NewRequest("", srv.URL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	start := time.Now()
	_, err := d.Download(req)
	if err == nil {
		t.Fatal("请求 ctx 到期应返回 error（经桥接 goroutine 取消 tab）")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("取消耗时 %v，应及时返回", elapsed)
	}
}
