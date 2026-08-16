// wired 演示爬虫（真实场景）：杂志列表页 → 正则提取文章链接入队 →
// 文章页抓取与保存。配置 downloader: "fallback"：
// HTTP 直连失败（传输错误/403/429 反爬拦截）时自动惰性降级 CDP 真实浏览器。
//
// 运行：
//
//	go run ./cmd/wired                       # 默认前 5 篇
//	go run ./cmd/wired -limit 8 -config wired.config.json
//
// 礼貌性：本配置已按低并发（worker 2）+ 低频（wired 域名 1 req/s）运行。
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
	"my-spider-demo/spider"
)

const listURL = "https://www.wired.com/magazine/"

// 列表页中的文章链接：/story/<slug>/（相对或绝对）。
// 正则仅用于 demo 级提取；生产建议评估 goquery 做结构化解析。
var (
	storyLinkRe = regexp.MustCompile(`href="(?:https://www\.wired\.com)?(/story/[a-z0-9-]+/)"`)
	ogTitleRe   = regexp.MustCompile(`<meta property="og:title" content="([^"]*)"`)
	titleTagRe  = regexp.MustCompile(`<title>([^<]+)</title>`)
)

type WiredSpider struct {
	limit int
}

func (WiredSpider) Name() string { return "wired-magazine" }

func (s WiredSpider) StartRequests() []*spider.Request {
	req, err := spider.NewRequest("", listURL, s.parseList)
	if err != nil {
		log.Fatal(err)
	}
	req.Priority = 10 // 列表页先行
	return []*spider.Request{req}
}

// parseList：从列表页 HTML 提取文章链接。
// 去重交给框架（同链接多次出现只抓一次），这里只负责"发现"。
func (s WiredSpider) parseList(resp *spider.Response) spider.Result {
	seen := map[string]bool{}
	res := spider.Result{}
	for _, m := range storyLinkRe.FindAllStringSubmatch(resp.Text(), -1) {
		url := "https://www.wired.com" + m[1]
		if seen[url] {
			continue
		}
		seen[url] = true
		if s.limit > 0 && len(seen) > s.limit {
			break
		}
		req, err := spider.NewRequest("", url, parseArticle)
		if err != nil {
			return spider.Result{Err: fmt.Errorf("build article request %s: %w", url, err)}
		}
		res.Requests = append(res.Requests, req)
	}
	if len(res.Requests) == 0 {
		return spider.Result{Err: fmt.Errorf("列表页未发现任何文章链接（可能被反爬页取代）")}
	}
	return res
}

func parseArticle(resp *spider.Response) spider.Result {
	title := ""
	if m := ogTitleRe.FindStringSubmatch(resp.Text()); m != nil {
		title = html.UnescapeString(m[1])
	} else if m := titleTagRe.FindStringSubmatch(resp.Text()); m != nil {
		title = html.UnescapeString(strings.TrimSpace(m[1]))
	}
	return spider.Result{Items: []spider.Item{{
		"title": title,
		"url":   resp.Request.URL,
		"bytes": len(resp.Body),
		"file":  resp.Request.Meta["saved_path"], // 开启 save_dir 时的原文路径
	}}}
}

func main() {
	limit := flag.Int("limit", 5, "最多抓取的文章数")
	configPath := flag.String("config", "configs/wired.json", "JSON 配置文件路径")
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
	if err := eng.RunContext(ctx, WiredSpider{limit: *limit}); err != nil {
		log.Printf("crawl aborted: %v", err)
	}
}
