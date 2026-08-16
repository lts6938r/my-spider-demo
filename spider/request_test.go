package spider

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

func TestNewRequestSplitsQuery(t *testing.T) {
	req, err := NewRequest("", "https://example.com/list?page=2&cat=book#frag", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if req.Method != http.MethodGet {
		t.Errorf("Method = %q, want GET", req.Method)
	}
	if req.URL != "https://example.com/list" {
		t.Errorf("URL = %q, want https://example.com/list", req.URL)
	}
	if got := req.Query.Get("page"); got != "2" {
		t.Errorf("Query[page] = %q, want 2", got)
	}
	if got := req.Query.Get("cat"); got != "book" {
		t.Errorf("Query[cat] = %q, want book", got)
	}
}

func TestNewRequestRejectsBadURL(t *testing.T) {
	if _, err := NewRequest("", "://no-scheme", nil); err == nil {
		t.Error("want error for url that fails to parse")
	}
	if _, err := NewRequest("", "example.com/path", nil); err == nil {
		t.Error("want error for url without scheme/host")
	}
}

func TestBuildURLMergesQuery(t *testing.T) {
	req, err := NewRequest("get", "https://example.com/search", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := req.BuildURL(); got != "https://example.com/search" {
		t.Errorf("BuildURL = %q, want no query suffix", got)
	}

	// 拼回的 URL 应能 roundtrip 还原 Query（不硬编码百分号编码结果）。
	req.Query.Set("q", "go 爬虫")
	built, err := url.Parse(req.BuildURL())
	if err != nil {
		t.Fatalf("parse BuildURL: %v", err)
	}
	if got := built.Query().Get("q"); got != "go 爬虫" {
		t.Errorf("roundtrip Query[q] = %q, want %q", got, "go 爬虫")
	}
}

func TestRequestContextContract(t *testing.T) {
	// 零值 Request 也必须满足 Context() 非 nil 的契约。
	req := &Request{}
	if req.Context() == nil {
		t.Fatal("Context() = nil, want non-nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	req2 := req.WithContext(ctx)
	cancel()

	select {
	case <-req2.Context().Done():
	default:
		t.Error("req2 context should be cancelled after cancel()")
	}
	select {
	case <-req.Context().Done():
		t.Error("original request context should be untouched")
	default:
	}
}

func TestResponseText(t *testing.T) {
	resp := &Response{StatusCode: http.StatusOK, Body: []byte("<html>hi</html>")}
	if got := resp.Text(); got != "<html>hi</html>" {
		t.Errorf("Text() = %q", got)
	}
}
