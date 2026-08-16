package engine

import (
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"my-spider-demo/rss"
	spider "my-spider-demo/spider"
)

var ogTitleRe = regexp.MustCompile(`<meta property="og:title" content="([^"]*)"`)

// feedParse 与 cmd/rss 中的业务解析同构：ParseRSS → 链接入队。
func feedParse(resp *spider.Response) spider.Result {
	items, err := rss.ParseRSS(resp)
	if err != nil {
		return spider.Result{Err: fmt.Errorf("parse rss: %w", err)}
	}
	res := spider.Result{}
	for _, it := range items {
		req, err := spider.NewRequest("", it.Link, articleParse)
		if err != nil {
			return spider.Result{Err: err}
		}
		req.Meta = map[string]any{"feed_title": it.Title}
		res.Requests = append(res.Requests, req)
	}
	return res
}

func articleParse(resp *spider.Response) spider.Result {
	title := ""
	if m := ogTitleRe.FindStringSubmatch(resp.Text()); m != nil {
		title = html.UnescapeString(m[1])
	}
	return spider.Result{Items: []spider.Item{{
		"feed_title": resp.Request.Meta["feed_title"],
		"title":      title,
		"url":        resp.Request.URL,
		"bytes":      len(resp.Body),
	}}}
}

// 引擎级闭环（离线）：RSS 页 → 条目链接入队 → 文章页下载并提取。
func TestEngineRSSCrawl(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/feed", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<?xml version="1.0"?><rss version="2.0"><channel>%s%s%s</channel></rss>`,
			itemXML(r.Host, 1), itemXML(r.Host, 2), itemXML(r.Host, 3))
	})
	for i := 1; i <= 3; i++ {
		mux.HandleFunc(fmt.Sprintf("/art%d", i), func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `<html><head><meta property="og:title" content="Article %s"/></head></html>`,
				strings.TrimPrefix(r.URL.Path, "/art"))
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	start, _ := spider.NewRequest("", srv.URL+"/feed", feedParse)
	eng, got := captureEngine(nil)
	if err := eng.Run(spiderStub{"rss", []*spider.Request{start}}); err != nil {
		t.Fatal(err)
	}

	if len(*got) != 3 {
		t.Fatalf("捕获 %d 个 item, want 3: %v", len(*got), *got)
	}
	if (*got)[0]["title"] != "Article 1" || (*got)[0]["bytes"].(int) <= 0 {
		t.Errorf("item[0] = %v（文章页应已下载并解析）", (*got)[0])
	}
	if eng.Downloaded != 4 || eng.Parsed != 4 {
		t.Errorf("Downloaded=%d Parsed=%d, want 4/4（feed + 3 篇文章）", eng.Downloaded, eng.Parsed)
	}
}

func itemXML(host string, i int) string {
	return fmt.Sprintf(`<item><title>Item %d</title><link>http://%s/art%d</link></item>`, i, host, i)
}
