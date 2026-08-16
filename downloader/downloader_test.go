package downloader

import (
	"context"
	"errors"
	"io"
	"my-spider-demo/spider"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// observed 用于把 handler goroutine 里看到的信息安全传回测试 goroutine
// （直接写共享变量过不了 -race）。
type observed struct {
	ua, method, query, contentType string
}

func TestDownloadOK(t *testing.T) {
	obs := make(chan observed, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs <- observed{
			ua:          r.Header.Get("User-Agent"),
			method:      r.Method,
			query:       r.URL.Query().Get("x"),
			contentType: r.Header.Get("Content-Type"),
		}
		w.Header().Set("X-Test", "1")
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	req, err := spider.NewRequest("", srv.URL+"/a?x=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := NewHTTPDownloader(5 * time.Second).Download(req)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	o := <-obs

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
	if string(resp.Body) != "hello" {
		t.Errorf("Body = %q, want hello", resp.Body)
	}
	if resp.Header.Get("X-Test") != "1" {
		t.Errorf("响应头 X-Test = %q, want 1", resp.Header.Get("X-Test"))
	}
	if resp.Request != req {
		t.Error("Response 应回溯引用产生它的 Request")
	}
	if o.ua != DefaultUserAgent {
		t.Errorf("服务端看到 UA = %q, want 默认 UA", o.ua)
	}
	if o.method != http.MethodGet {
		t.Errorf("服务端看到 method = %q, want GET", o.method)
	}
	if o.query != "1" {
		t.Errorf("服务端看到 query x = %q, want 1（BuildURL 合并）", o.query)
	}
}

func TestHTTPStatusIsNotError(t *testing.T) {
	for _, code := range []int{400, 404, 429, 500, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		req, err := spider.NewRequest("", srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := NewHTTPDownloader(5 * time.Second).Download(req)
		srv.Close()

		if err != nil {
			t.Fatalf("status %d: 不应把 HTTP 状态当传输错误: %v", code, err)
		}
		if resp.StatusCode != code {
			t.Errorf("status %d: got %d", code, resp.StatusCode)
		}
	}
}

func TestDownloadNetworkError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // 立即关闭，制造连接拒绝

	req, err := spider.NewRequest("", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := NewHTTPDownloader(5 * time.Second).Download(req)
	if err == nil {
		t.Fatal("连接拒绝应返回 error")
	}
	if resp != nil {
		t.Error("出错时不应返回 Response")
	}
}

func TestDownloadHonorsRequestContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()

	// Client 整体超时放得很宽，用请求级 ctx 制造取消。
	req, err := spider.NewRequest("", srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = NewHTTPDownloader(10 * time.Second).Download(req.WithContext(ctx))
	if err == nil {
		t.Fatal("ctx 到期应返回 error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err 应可追溯为 DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("取消后耗时 %v，应及时返回", elapsed)
	}
}

func TestDownloadPOSTBody(t *testing.T) {
	obs := make(chan observed, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		obs <- observed{contentType: r.Header.Get("Content-Type")}
		_, _ = w.Write(b) // 原样返回
	}))
	defer srv.Close()

	req, err := spider.NewRequest(http.MethodPost, srv.URL+"/submit", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Body = []byte(`{"a":1}`)

	resp, err := NewHTTPDownloader(5 * time.Second).Download(req)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != `{"a":1}` {
		t.Errorf("POST body 往返 = %q, want {\"a\":1}", resp.Body)
	}
	if o := <-obs; o.contentType != "application/json" {
		t.Errorf("服务端看到 Content-Type = %q", o.contentType)
	}
}

func TestDownloadFollowsRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/a", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/b", http.StatusFound)
	})
	mux.HandleFunc("/b", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("final"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := spider.NewRequest("", srv.URL+"/a", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := NewHTTPDownloader(5 * time.Second).Download(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(resp.Body) != "final" {
		t.Errorf("重定向后 = %d %q, want 200 final", resp.StatusCode, resp.Body)
	}
}
