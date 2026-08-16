package downloader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"my-spider-demo/spider"
	"net/http"
	"time"
)

// Downloader 负责把 Request 变成 Response，是唯一接触 net/http 的模块。
type Downloader interface {
	// Download 执行一次抓取，约定：
	//
	//  1. 传输层成功（拿到 HTTP 响应）即返回 Response——
	//     4xx/5xx 也算成功，"业务上是否可接受"由解析与中间件判断；
	//     网络失败（连接拒绝、超时、DNS 等）才返回 error。
	//  2. context 随数据走：内部取 r.Context()。超时、停机等生命周期
	//     由调用方组合进 Request（V3 引擎用 WithContext 注入）。
	Download(r *spider.Request) (*spider.Response, error)
}

// DefaultUserAgent 未显式设置 UA 时的默认值。
const DefaultUserAgent = "spider/0.1"

// HTTPDownloader 基于 net/http 的标准实现。
type HTTPDownloader struct {
	client *http.Client
}

// NewHTTPDownloader 创建下载器。timeout 是单次请求的整体超时，
// 传 0 表示不设超时（不建议）。
func NewHTTPDownloader(timeout time.Duration) *HTTPDownloader {
	tr := &http.Transport{
		// 连接池参数：Transport 默认 MaxIdleConnsPerHost 只有 2，
		// 对集中抓取单域名的爬虫是明显瓶颈，调大空闲连接复用率。
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
		// CheckRedirect 保持默认：跟随重定向，最多 10 次。
	}
	return &HTTPDownloader{client: &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}}
}

func (d *HTTPDownloader) Download(r *spider.Request) (*spider.Response, error) {
	var body io.Reader
	if len(r.Body) > 0 {
		body = bytes.NewReader(r.Body)
	}

	httpReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.BuildURL(), body)
	if err != nil {
		return nil, fmt.Errorf("spider: build %s %s: %w", r.Method, r.BuildURL(), err)
	}
	if r.Header != nil {
		httpReq.Header = r.Header.Clone()
	}
	if httpReq.Header.Get("User-Agent") == "" {
		httpReq.Header.Set("User-Agent", DefaultUserAgent)
	}

	start := time.Now()
	httpResp, err := d.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("spider: %s %s: %w", r.Method, r.BuildURL(), err)
	}

	// 必须把 body 读完再 Close：读到达 EOF，连接才能带着完整状态
	// 回到空闲池被复用；中途出错时 Close 会放弃这条连接。
	// body 的生命周期到此为止，上层模块永远接触不到 io.ReadCloser。
	bodyBytes, readErr := io.ReadAll(httpResp.Body)
	closeErr := httpResp.Body.Close()
	cost := time.Since(start)

	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("spider: read body %s: %w", r.BuildURL(), err)
	}

	return &spider.Response{
		StatusCode: httpResp.StatusCode,
		Header:     httpResp.Header,
		Body:       bodyBytes,
		Request:    r,
		Cost:       cost,
	}, nil
}
