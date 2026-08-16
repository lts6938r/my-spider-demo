package downloader

import (
	"fmt"
	"io"
	"my-spider-demo/spider"
	"sync"
)

// FallbackDownloader 是下载器层的组合实现：首选失败时降级到备选。
// 典型用途：HTTP 直连（快、便宜）失败或被反爬拦截（403/429）时，
// 降级 CDP 真实浏览器（慢、重，但能过大多数指纹/JS 挑战）。
//
// 降级判定：传输错误（err != nil）或 403/429。
// 两侧都失败时返回**备选侧**的错误——它更接近问题的最新状态。
//
// 已知取舍：限流中间件在本层之外，一次降级序列（HTTP+CDP 两次真实
// 访问）只消耗一个令牌；降级是少数路径，接受该误差并记录在案。
type FallbackDownloader struct {
	primary   Downloader
	secondary Downloader
}

func NewFallbackDownloader(primary, secondary Downloader) *FallbackDownloader {
	return &FallbackDownloader{primary: primary, secondary: secondary}
}

func (f *FallbackDownloader) Download(r *spider.Request) (*spider.Response, error) {
	resp, err := f.primary.Download(r)
	if err == nil && !isBlocked(resp) {
		return resp, nil
	}
	return f.secondary.Download(r)
}

// isBlocked 识别"传输成功但被拒绝"的反爬拦截状态码。
func isBlocked(resp *spider.Response) bool {
	return resp != nil && (resp.StatusCode == 403 || resp.StatusCode == 429)
}

// Close 关闭实现了 io.Closer 的两侧（如 CDP 浏览器）。
func (f *FallbackDownloader) Close() error {
	var errs []error
	for _, d := range []Downloader{f.primary, f.secondary} {
		if c, ok := d.(io.Closer); ok {
			if err := c.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("spider: fallback close: %v", errs)
	}
	return nil
}

// lazyCDP 把 CDP 下载器的启动推迟到首次真正降级：
// 全程 HTTP 成功的运行不会拉起 Chrome 进程。
// sync.Once 保证并发 worker 下只启动一次。
type lazyCDP struct {
	cfg  CDPConfig
	once sync.Once
	d    *CDPDownloader
	err  error
}

// NewLazyCDP 返回把 CDP 启动推迟到首次调用的下载器（并发安全）。
// 供降级组装使用：全程未触发降级则不启动浏览器进程。
func NewLazyCDP(cfg CDPConfig) Downloader { return &lazyCDP{cfg: cfg} }

func (l *lazyCDP) Download(r *spider.Request) (*spider.Response, error) {
	l.once.Do(func() {
		l.d, l.err = NewCDPDownloader(l.cfg)
	})
	if l.err != nil {
		return nil, fmt.Errorf("spider: cdp 降级不可用: %w", l.err)
	}
	return l.d.Download(r)
}

func (l *lazyCDP) Close() error {
	if l.d != nil {
		return l.d.Close()
	}
	return nil
}
