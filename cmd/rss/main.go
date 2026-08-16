// rss 演示爬虫（真实场景）：抓取 RSS/Atom 订阅源 → 解析条目链接入队 →
// 下载文章页并提取标题，展示"RSS 发现 → 页面抓取"的两级数据流。
//
// 运行：
//
//	go run ./cmd/rss                                        # 默认 The Verge，前 10 篇
//	go run ./cmd/rss -limit 20                              # 更多文章
//	go run ./cmd/rss -feed https://rss.slashdot.org/Slashdot/slashdot
//	go run ./cmd/rss -feed <url> -config verge.config.json  # 原文落盘 + JSONL
//
// 说明：订阅源是公开的；文章页体积较大，limit 适量、限速运行。
package main

import (
	"context"
	"flag"
	"fmt"
	"html"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"my-spider-demo/dedup"
	"my-spider-demo/engine"
	"my-spider-demo/pipeline"
	"my-spider-demo/rss"
	"my-spider-demo/spider"
)

const defaultFeedURL = "http://www.theverge.com/rss/index.xml"

var (
	ogTitleRe  = regexp.MustCompile(`<meta property="og:title" content="([^"]*)"`)
	titleTagRe = regexp.MustCompile(`<title>([^<]+)</title>`)
)

type RSSSpider struct {
	feedURL string // RSS/Atom 订阅源地址
	limit   int    // 最多入队的文章数，0 = 全部
}

func (s RSSSpider) Name() string { return "rss" }

func (s RSSSpider) StartRequests() []*spider.Request {
	req, err := spider.NewRequest("", s.feedURL, s.parseFeed)
	if err != nil {
		log.Fatal(err)
	}
	req.Priority = 10 // feed 先行（priority 调度下）
	return []*spider.Request{req}
}

// parseFeed：框架负责下载 RSS，这里只做业务决策——
// 框架的 ParseRSS 提取条目，Spider 决定"入队前 limit 篇、挂 parseArticle"。
func (s RSSSpider) parseFeed(resp *spider.Response) spider.Result {
	items, err := rss.ParseRSS(resp)
	if err != nil {
		return spider.Result{Err: fmt.Errorf("parse rss: %w", err)}
	}

	res := spider.Result{}
	for i, it := range items {
		if s.limit > 0 && i >= s.limit {
			break
		}
		req, err := spider.NewRequest("", it.Link, parseArticle)
		if err != nil {
			return spider.Result{Err: fmt.Errorf("build article request %s: %w", it.Link, err)}
		}
		// Meta 沿请求链传递：文章页知道自己在 feed 里的标题与发布时间
		req.Meta = map[string]any{"feed_title": it.Title, "published": it.Published}
		res.Requests = append(res.Requests, req)
	}
	return res
}

// parseArticle：从下载到的文章页提取标题。
// og:title 优先，<title> 兜底。
func parseArticle(resp *spider.Response) spider.Result {
	title := ""
	if m := ogTitleRe.FindStringSubmatch(resp.Text()); m != nil {
		title = html.UnescapeString(m[1])
	} else if m := titleTagRe.FindStringSubmatch(resp.Text()); m != nil {
		title = html.UnescapeString(strings.TrimSpace(m[1]))
	}
	return spider.Result{Items: []spider.Item{{
		"feed_title": resp.Request.Meta["feed_title"],
		"published":  resp.Request.Meta["published"],
		"title":      title,
		"url":        resp.Request.URL,
		"bytes":      len(resp.Body),                  // 页面体积：证明真实下载完成
		"file":       resp.Request.Meta["saved_path"], // 开启 save_dir 时，原文落盘路径
	}}}
}

func main() {
	feed := flag.String("feed", defaultFeedURL, "RSS/Atom 订阅源地址")
	limit := flag.Int("limit", 10, "最多抓取的文章数（0 = 全部）")
	configPath := flag.String("config", "", "JSON 配置文件路径（缺省使用默认配置）")
	flag.Parse()

	cfg, err := engine.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng, err := engine.NewEngineFromConfig(cfg, dedup.NewHashSetDuper(), pipeline.ConsolePipeline{})
	if err != nil {
		log.Fatalf("组装引擎失败: %v", err)
	}
	sp := RSSSpider{feedURL: *feed, limit: *limit}
	if err := eng.RunContext(ctx, sp); err != nil {
		log.Printf("crawl aborted: %v", err)
	}
}
