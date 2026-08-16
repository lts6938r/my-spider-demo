package middleware

import (
	"my-spider-demo/downloader"
	"my-spider-demo/spider"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSaveResponseMiddleware(t *testing.T) {
	dir := t.TempDir()
	h := SaveResponseMiddleware(dir, testLogger)(func(r *spider.Request) (*spider.Response, error) {
		return &spider.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
			Body:       []byte("<html>page body</html>"),
			Request:    r,
		}, nil
	})

	r, _ := spider.NewRequest("", "https://example.com/news/article-one", nil)
	if _, err := h(r); err != nil {
		t.Fatal(err)
	}

	saved, _ := r.Meta["saved_path"].(string)
	if saved == "" {
		t.Fatal("保存成功应写入 Meta[saved_path]")
	}
	if !strings.HasSuffix(saved, ".html") {
		t.Errorf("扩展名 = %q, want .html（按 Content-Type 推断）", saved)
	}
	if !strings.Contains(filepath.Base(saved), "article-one") {
		t.Errorf("文件名应含 URL 末段（可读性）: %s", saved)
	}

	b, err := os.ReadFile(saved)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "<html>page body</html>" {
		t.Errorf("文件内容 = %q（应为响应原文）", b)
	}
}

func TestSaveResponseMiddlewareExtByContentType(t *testing.T) {
	dir := t.TempDir()
	h := SaveResponseMiddleware(dir, testLogger)(func(r *spider.Request) (*spider.Response, error) {
		return &spider.Response{
			Header:  http.Header{"Content-Type": []string{"application/xml; charset=utf-8"}},
			Body:    []byte("<rss></rss>"),
			Request: r,
		}, nil
	})
	r, _ := spider.NewRequest("", "https://example.com/feed", nil)
	h(r)
	saved, _ := r.Meta["saved_path"].(string)
	if !strings.HasSuffix(saved, ".xml") {
		t.Errorf("XML 响应扩展名 = %q, want .xml", saved)
	}
}

func TestSaveResponseSkipsFailures(t *testing.T) {
	dir := t.TempDir()
	h := SaveResponseMiddleware(dir, testLogger)(failHandle)
	r, _ := spider.NewRequest("", "https://example.com/x", nil)
	if _, err := h(r); err == nil {
		t.Fatal("want error")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("失败的下载不应落盘, got %d 个文件", len(entries))
	}
}

// 语义锁定：Save 在 Retry 外层时，重试的中间响应（2 次 503）不落盘，
// 只有最终成功的那份写入——目录里应恰好 1 个文件。
func TestSaveOnlyFinalResponseOutsideRetry(t *testing.T) {
	srv := httptest.NewServer(statusThenOK(2, http.StatusServiceUnavailable))
	defer srv.Close()

	dir := t.TempDir()
	h := SaveResponseMiddleware(dir, testLogger)(
		RetryMiddleware(fastRetry, testLogger)(
			downloader.NewHTTPDownloader(5 * time.Second).Download))

	r, _ := spider.NewRequest("", srv.URL, nil)
	if _, err := h(r); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("落盘 %d 个文件, want 1（只保存最终响应）", len(entries))
	}
	b, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if string(b) != "ok" {
		t.Errorf("文件内容 = %q, want 最终响应 ok", b)
	}
}

func TestSaveResponseBadDir(t *testing.T) {
	// 保存失败不影响抓取：目录路径是一个已存在的文件 → MkdirAll 失败
	block := filepath.Join(t.TempDir(), "blocker")
	os.WriteFile(block, []byte("x"), 0o644)

	h := SaveResponseMiddleware(block, testLogger)(okHandle)
	r, _ := spider.NewRequest("", "https://example.com/y", nil)
	resp, err := h(r)
	if err != nil {
		t.Fatalf("保存失败不应让抓取失败: %v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Error("响应应原样返回")
	}
	if _, ok := r.Meta["saved_path"]; ok {
		t.Error("失败时不应写 Meta")
	}
}
