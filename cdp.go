package spider

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/chromedp/chromedp"
)

// CDPDownloader 通过 Chrome DevTools Protocol 驱动真实 Chrome 渲染页面，
// 用于 JS 渲染 / 客户端指纹检测严格的站点。实现 Downloader 接口：
// 引擎、中间件、调度、去重对"下载方式"完全无感知（V1 抽象的兑现）。
//
// 与 HTTPDownloader 的差异：
//   - 并发单位是标签页（tab）而非连接，内存开销大——此模式建议 Workers 调低（2~4）
//   - 仅支持 GET 导航；Method/Body/自定义 Header 不生效（浏览器指纹才是意义所在）
//   - Cookie 由浏览器跨 tab 共享（登录态自然保持）
//   - 重定向、gzip、JS 渲染全部由浏览器处理
type CDPDownloader struct {
	browserCtx  context.Context
	cancelBrow  context.CancelFunc
	cancelAlloc context.CancelFunc

	timeout time.Duration // 单页导航超时
	wait    time.Duration // load 事件后的额外渲染等待（等异步数据上屏）
}

// CDPConfig 配置 CDP 下载器。
type CDPConfig struct {
	Headless bool          // 无头模式；false 可肉眼观察浏览器行为（调试用）
	Timeout  time.Duration // 导航超时，<=0 取 30s
	Wait     time.Duration // load 后额外等待，给 JS 异步渲染留时间
}

// NewCDPDownloader 启动一个共享的浏览器实例。
// 立即启动而非惰性：失败在构造时暴露，且首个请求不背启动延迟。
func NewCDPDownloader(cfg CDPConfig) (*CDPDownloader, error) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", cfg.Headless),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	browserCtx, cancelBrow := chromedp.NewContext(allocCtx)
	if err := chromedp.Run(browserCtx); err != nil {
		cancelBrow()
		cancelAlloc()
		return nil, fmt.Errorf("spider: launch chrome: %w", err)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &CDPDownloader{
		browserCtx:  browserCtx,
		cancelBrow:  cancelBrow,
		cancelAlloc: cancelAlloc,
		timeout:     timeout,
		wait:        cfg.Wait,
	}, nil
}

// Download 导航到目标 URL，返回渲染后的 DOM。
// 传输层语义与 HTTP 版一致：拿到页面即成功（4xx/5xx 也是 Response），
// 导航失败（网络错误、超时、被取消）才返回 error——重试中间件无需感知差异。
func (d *CDPDownloader) Download(r *Request) (*Response, error) {
	start := time.Now()

	tabCtx, cancelTab, err := d.newTab(r.Context())
	if err != nil {
		return nil, err
	}
	defer cancelTab()

	navCtx, cancelNav := context.WithTimeout(tabCtx, d.timeout)
	defer cancelNav()

	res, err := chromedp.RunResponse(navCtx, chromedp.Navigate(r.BuildURL()))
	if err != nil {
		return nil, fmt.Errorf("spider: cdp navigate %s: %w", r.BuildURL(), err)
	}

	if d.wait > 0 {
		select {
		case <-time.After(d.wait):
		case <-tabCtx.Done():
			return nil, fmt.Errorf("spider: cdp wait %s: %w", r.BuildURL(), tabCtx.Err())
		}
	}

	var html string
	if err := chromedp.Run(tabCtx, chromedp.OuterHTML("html", &html, chromedp.ByQuery)); err != nil {
		return nil, fmt.Errorf("spider: cdp read dom %s: %w", r.BuildURL(), err)
	}

	resp := &Response{
		Body:    []byte(html),
		Request: r,
		Cost:    time.Since(start),
		Header:  make(http.Header),
	}
	if res != nil {
		resp.StatusCode = int(res.Status)
		for k, v := range res.Headers {
			if s, ok := v.(string); ok {
				resp.Header.Set(k, s)
			}
		}
	}
	return resp, nil
}

// newTab 为单个请求开一个标签页。
// chromedp 的 tab ctx 必须派生自浏览器 ctx（否则会另起浏览器），
// 而请求自己的 ctx（超时/停机）需要桥接进来：监视 reqCtx，
// 取消时连带取消 tab。桥接 goroutine 以 parent.Done() 为退出条件，不泄漏。
func (d *CDPDownloader) newTab(reqCtx context.Context) (context.Context, context.CancelFunc, error) {
	parent, cancel := context.WithCancel(d.browserCtx)
	go func() {
		select {
		case <-reqCtx.Done():
			cancel()
		case <-parent.Done():
		}
	}()

	tabCtx, _ := chromedp.NewContext(parent)
	if err := chromedp.Run(tabCtx); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("spider: cdp new tab: %w", err)
	}
	return tabCtx, cancel, nil
}

// Close 关闭浏览器与启动器，幂等由 context 取消语义保证。
func (d *CDPDownloader) Close() error {
	d.cancelBrow()
	d.cancelAlloc()
	return nil
}
