// quotes 演示爬虫：抓取 quotes.toscrape.com 的名言并沿 "Next" 翻页，
// 展示一个完整 Spider 的最小写法——业务逻辑只有 parse 一个函数。
//
// 运行：
//
//	go run ./cmd/quotes                          # 默认配置
//	go run ./cmd/quotes -config config.example.json
//
// 说明：该站点是公开的爬虫练习站；需要网络。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	spider "my-spider-demo"
)

// V1 用正则演示数据流；结构化解析（goquery）是否引入留到后续评估——
// 那是"标准库难以合理解决"的场景，属于允许的第三方依赖。
var (
	quoteRe  = regexp.MustCompile(`<span class="text" itemprop="text">(.+?)</span>`)
	authorRe = regexp.MustCompile(`<small class="author" itemprop="author">(.+?)</small>`)
	nextRe   = regexp.MustCompile(`<li class="next">\s*<a href="(/page/\d+/)"`)
)

type QuotesSpider struct{}

func (QuotesSpider) Name() string { return "quotes" }

func (QuotesSpider) StartRequests() []*spider.Request {
	req, err := spider.NewRequest("", "https://quotes.toscrape.com/", parse)
	if err != nil {
		log.Fatal(err)
	}
	return []*spider.Request{req}
}

func parse(resp *spider.Response) spider.Result {
	texts := quoteRe.FindAllStringSubmatch(resp.Text(), -1)
	authors := authorRe.FindAllStringSubmatch(resp.Text(), -1)

	res := spider.Result{}
	for i := range texts {
		if i >= len(authors) {
			break
		}
		res.Items = append(res.Items, spider.Item{
			"text":   strings.TrimSpace(texts[i][1]),
			"author": strings.TrimSpace(authors[i][1]),
		})
	}

	if m := nextRe.FindStringSubmatch(resp.Text()); m != nil {
		next := strings.TrimSuffix(resp.Request.URL, "/") + m[1]
		req, err := spider.NewRequest("", next, parse)
		if err != nil {
			return spider.Result{Err: fmt.Errorf("build next request: %w", err)}
		}
		res.Requests = append(res.Requests, req)
	}
	return res
}

func main() {
	configPath := flag.String("config", "", "JSON 配置文件路径（缺省使用默认配置）")
	flag.Parse()

	cfg, err := spider.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	// Ctrl-C / kill → ctx 取消 → 引擎停止调度新请求、
	// 取消在途下载、排干已抓到的 item 后退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 中间件链顺序由工厂固定：Logging → Retry → RateLimit → UA → Downloader
	eng, err := spider.NewEngineFromConfig(cfg, spider.NewHashSetDuper(), spider.ConsolePipeline{})
	if err != nil {
		log.Fatalf("组装引擎失败: %v", err)
	}
	if err := eng.RunContext(ctx, QuotesSpider{}); err != nil {
		log.Printf("crawl aborted: %v", err)
	}
}
