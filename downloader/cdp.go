package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"

	"my-spider-demo/spider"
)

// CDPDownloader 通过 Chrome DevTools Protocol 驱动真实 Chrome 渲染页面，
// 用于 JS 渲染 / 客户端指纹检测严格的站点。实现 Downloader 接口：
// 引擎、中间件、调度、去重对"下载方式"完全无感知。
//
// 协议栈为自实现（不依赖 chromedp）：最小 WebSocket 客户端（cdpws.go）
// + DevTools 的三个域——
//
//	Page.enable / Network.enable        会话准备
//	Page.navigate                       导航，响应含 frameId / errorText
//	Network.responseReceived (事件)     主文档的 HTTP 状态码与响应头
//	Page.loadEventFired (事件)          页面加载完成
//	Runtime.evaluate                    取 document.documentElement.outerHTML
//
// 三种接入模式：
//   - Attach：Addr 非空且已有 Chrome 在该端口运行，直接连接。
//   - Attach-or-Launch：Addr 非空且 Headless=false，先尝试连接；失败则自动
//     在该地址启动真实（非 headless）Chrome 浏览器，适用于反检测场景。
//   - Launch（默认，Addr 为空）：自选端口、独立临时 profile 启动 Chrome，
//     Close 时回收进程与目录。
//
// 并发模型：每个请求独占一个标签页（HTTP /json/new 创建、用毕 /json/close
// 关闭），每个标签页一条独立 ws 连接——会话之间天然隔离，无需跨会话锁。
// tab 内存远大于连接，此模式 Workers 建议调低（2~4）；
// Cookie 由浏览器跨 tab 共享（登录态自然保持）。
type CDPConfig struct {
	Headless   bool          // Launch 模式下是否无头；false 可肉眼观察或用于反检测
	Timeout    time.Duration // 单页整体超时，<=0 取 30s
	Wait       time.Duration // load 事件后的额外渲染等待，给 JS 异步渲染留时间
	Addr       string        // 调试端口地址（如 127.0.0.1:9222）；空 = 自动 Launch
	UserAgent  string        // 非空则通过 CDP 覆盖 navigator.userAgent（反 headless 检测）
	ProfileDir string        // 自定义 user-data-dir 路径；空 = 临时目录。非空时 Close 不删除
}

type CDPDownloader struct {
	addr        string
	cmd         *exec.Cmd // 本方法启动的 Chrome 进程（attach 模式为 nil）
	profileDir  string    // Chrome user-data-dir
	hc          *http.Client
	timeout     time.Duration
	wait        time.Duration
	userAgent   string // 非空时通过 CDP Emulation.setUserAgentOverride 注入
	keepProfile bool   // true 时不删除 profileDir（用户指定或 attach 模式）
}

// NewCDPDownloader 启动/接入浏览器。
// Addr 非空时先尝试 attach；连接失败且 Headless=false 则自动启动真实 Chrome。
// Launch 模式立即启动并等待就绪：失败在构造时暴露，且首个请求不背启动延迟。
func NewCDPDownloader(cfg CDPConfig) (*CDPDownloader, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	d := &CDPDownloader{
		hc:        &http.Client{Timeout: 5 * time.Second},
		timeout:   cfg.Timeout,
		wait:      cfg.Wait,
		userAgent: cfg.UserAgent,
	}

	// ---- Attach 或 Attach-or-Launch ----
	if cfg.Addr != "" {
		d.addr = cfg.Addr
		if err := d.waitReady(2 * time.Second); err == nil {
			return d, nil // attach 成功
		}
		// 连接失败：Headless=false 时自动启动真实浏览器
		if !cfg.Headless {
			if launchErr := d.launchAtAddr(cfg); launchErr != nil {
				return nil, fmt.Errorf("spider: cdp attach %s 失败且启动浏览器也失败: %v", cfg.Addr, launchErr)
			}
			return d, nil
		}
		return nil, fmt.Errorf("spider: cdp attach %s: 连接失败（地址不可达）", cfg.Addr)
	}

	// ---- 标准 Launch 模式（Addr 为空）----
	exe, err := findChrome()
	if err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	dir := cfg.ProfileDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "spider-cdp-*")
		if err != nil {
			return nil, fmt.Errorf("spider: temp profile: %w", err)
		}
	} else {
		d.keepProfile = true
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	args := []string{
		"--disable-gpu", "--no-first-run", "--no-default-browser-check",
		fmt.Sprintf("--remote-debugging-port=%d", port),
		fmt.Sprintf("--remote-allow-origins=http://%s", addr),
		"--user-data-dir=" + dir, // 必须独立 profile：新版 Chrome 拒绝在默认 profile 上开调试端口
		"about:blank",
	}
	if cfg.Headless {
		args = append([]string{"--headless"}, args...)
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		if !d.keepProfile {
			os.RemoveAll(dir)
		}
		return nil, fmt.Errorf("spider: launch chrome %s: %w", exe, err)
	}
	d.addr = addr
	d.cmd = cmd
	d.profileDir = dir

	if err := d.waitReady(15 * time.Second); err != nil {
		d.Close()
		return nil, fmt.Errorf("spider: chrome 未在期限内就绪: %w", err)
	}
	return d, nil
}

// launchAtAddr 在 cfg.Addr 指定的地址启动真实（非 headless）Chrome 浏览器。
// 用于 attach-or-launch 模式：连接已有实例失败时自动拉起。
func (d *CDPDownloader) launchAtAddr(cfg CDPConfig) error {
	exe, err := findChrome()
	if err != nil {
		return err
	}
	_, portStr, _ := net.SplitHostPort(cfg.Addr)
	port, _ := strconv.Atoi(portStr)

	dir := cfg.ProfileDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "spider-cdp-*")
		if err != nil {
			return fmt.Errorf("temp profile: %w", err)
		}
	}

	args := []string{
		"--no-first-run", "--no-default-browser-check",
		fmt.Sprintf("--remote-debugging-port=%d", port),
		fmt.Sprintf("--remote-allow-origins=http://%s", cfg.Addr),
		"--user-data-dir=" + dir,
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		if cfg.ProfileDir == "" {
			os.RemoveAll(dir)
		}
		return fmt.Errorf("launch chrome: %w", err)
	}
	d.cmd = cmd
	d.profileDir = dir
	d.keepProfile = cfg.ProfileDir != "" // 用户指定了持久目录则保留
	return d.waitReady(15 * time.Second)
}

// waitReady 轮询 /json/version 直到调试端口可用。
func (d *CDPDownloader) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := d.hc.Get("http://" + d.addr + "/json/version")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}

// Close 回收本方法启动的 Chrome 进程与临时目录。
// attach 模式或用户指定 ProfileDir 时不删除浏览器和 profile。
// 幂等。
func (d *CDPDownloader) Close() error {
	if d.cmd != nil && d.cmd.Process != nil {
		_ = d.cmd.Process.Kill()
		_ = d.cmd.Wait()
	}
	d.cmd = nil
	if d.profileDir != "" && !d.keepProfile {
		os.RemoveAll(d.profileDir)
	}
	d.profileDir = ""
	return nil
}

// ---- 标签页管理（HTTP 端点）----

type devtoolsTarget struct {
	ID string `json:"id"`
	WS string `json:"webSocketDebuggerUrl"`
}

// newTab 新建标签页。新版 Chrome 要求 PUT，老版本仅支持 GET——依次尝试。
func (d *CDPDownloader) newTab() (*devtoolsTarget, error) {
	var lastErr error
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		req, err := http.NewRequest(method, "http://"+d.addr+"/json/new?about:blank", nil)
		if err != nil {
			return nil, err
		}
		resp, err := d.hc.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d: %.120s", resp.StatusCode, body)
			continue
		}
		var t devtoolsTarget
		if err := json.Unmarshal(body, &t); err != nil || t.WS == "" {
			lastErr = fmt.Errorf("非预期的 /json/new 响应: %.120s", body)
			continue
		}
		return &t, nil
	}
	return nil, fmt.Errorf("spider: cdp new tab: %w", lastErr)
}

func (d *CDPDownloader) closeTab(id string) {
	// 尽力而为：泄漏的标签页只占内存，不影响正确性
	resp, err := d.hc.Get("http://" + d.addr + "/json/close/" + id)
	if err == nil {
		resp.Body.Close()
	}
}

// ---- 会话层：单条 ws 连接上的 CDP 消息路由 ----

type cdpEvent struct {
	Method string
	Params json.RawMessage
}

type cdpReply struct {
	result json.RawMessage
	err    error
}

// wireMsg 是 CDP 的两种消息共用信封：带 id 的是命令响应，带 method 的是事件。
type wireMsg struct {
	ID     int64  `json:"id"`
	Method string `json:"method"`

	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type cdpSession struct {
	ws      *wsConn
	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan *cdpReply

	// onEvent 在读循环 goroutine 中回调，必须非阻塞
	//（Network 域事件流量大，不能阻塞读循环）。
	onEvent func(cdpEvent)
}

func newCDPSession(ws *wsConn, onEvent func(cdpEvent)) *cdpSession {
	s := &cdpSession{ws: ws, pending: make(map[int64]chan *cdpReply), onEvent: onEvent}
	go s.readLoop()
	return s
}

func (s *cdpSession) readLoop() {
	for {
		data, err := s.ws.ReadMessage()
		if err != nil {
			s.failAllPending(err)
			return
		}
		var m wireMsg
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		switch {
		case m.ID != 0: // 命令响应：按 id 路由
			s.mu.Lock()
			ch := s.pending[m.ID]
			delete(s.pending, m.ID)
			s.mu.Unlock()
			if ch != nil {
				if m.Error != nil {
					ch <- &cdpReply{err: fmt.Errorf("cdp: %d %s", m.Error.Code, m.Error.Message)}
				} else {
					ch <- &cdpReply{result: m.Result}
				}
			}
		case m.Method != "": // 事件
			if s.onEvent != nil {
				s.onEvent(cdpEvent{Method: m.Method, Params: m.Params})
			}
		}
	}
}

func (s *cdpSession) failAllPending(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ch := range s.pending {
		ch <- &cdpReply{err: fmt.Errorf("cdp: 连接中断: %w", err)}
		delete(s.pending, id)
	}
}

// call 发送命令并等待其响应；ctx 超时/取消会中断等待。
func (s *cdpSession) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	ch := make(chan *cdpReply, 1)
	s.pending[id] = ch
	s.mu.Unlock()

	payload := map[string]any{"id": id, "method": method}
	if params != nil {
		payload["params"] = params
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if err := s.ws.SendText(data); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, err
	}

	select {
	case r := <-ch:
		return r.result, r.err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ---- Download：单次抓取 ----

// docCapture 收集 Document 类型的 Network 响应。
// 重定向链上每一跳都是一条 Document responseReceived（iframe 也有自己的），
// 全部保留，load 后按主 frameId 从后往前挑——第一条匹配的即最终响应。
type docCapture struct {
	frameID string
	status  int
	headers map[string]any
}

// Download 导航到目标 URL，返回渲染后的 DOM。
// 传输语义与 HTTP 版一致：拿到页面即成功（4xx/5xx 也是 Response），
// 导航失败（网络错误、超时、被取消）才返回 error——重试中间件无感复用。
func (d *CDPDownloader) Download(r *spider.Request) (*spider.Response, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), d.timeout)
	defer cancel()

	tab, err := d.newTab()
	if err != nil {
		return nil, err
	}
	defer d.closeTab(tab.ID)

	ws, err := wsDial(tab.WS, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("spider: cdp dial: %w", err)
	}
	defer ws.Close()

	// ctx 取消/超时 → 关闭连接解除所有阻塞读（会话的中断机制；
	// 桥接 goroutine 以 watchDone 为退出条件，不泄漏）
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			ws.Close()
		case <-watchDone:
		}
	}()
	defer close(watchDone)

	var (
		evMu   sync.Mutex
		docs   []docCapture
		loaded = make(chan struct{}, 1)
	)
	onEvent := func(e cdpEvent) {
		switch e.Method {
		case "Page.loadEventFired":
			select {
			case loaded <- struct{}{}:
			default:
			}
		case "Network.responseReceived":
			var p struct {
				Type     string `json:"type"`
				FrameID  string `json:"frameId"`
				Response *struct {
					Status  int            `json:"status"`
					Headers map[string]any `json:"headers"`
				} `json:"response"`
			}
			if json.Unmarshal(e.Params, &p) != nil || p.Response == nil {
				return
			}
			if p.Type != "Document" {
				return
			}
			evMu.Lock()
			if len(docs) < 64 { // iframe 多的页面防御性封顶
				docs = append(docs, docCapture{
					frameID: p.FrameID,
					status:  p.Response.Status,
					headers: p.Response.Headers,
				})
			}
			evMu.Unlock()
		}
	}
	sess := newCDPSession(ws, onEvent)

	// 必须先启用域再导航：事件只在域启用后才会推送
	if _, err := sess.call(ctx, "Page.enable", nil); err != nil {
		return nil, fmt.Errorf("spider: cdp page.enable: %w", err)
	}
	if _, err := sess.call(ctx, "Network.enable", nil); err != nil {
		return nil, fmt.Errorf("spider: cdp network.enable: %w", err)
	}

	// 导航前覆盖 User-Agent（反 headless 检测：去掉 HeadlessChrome 标识）
	if d.userAgent != "" {
		if _, err := sess.call(ctx, "Emulation.setUserAgentOverride", map[string]string{
			"userAgent": d.userAgent,
		}); err != nil {
			return nil, fmt.Errorf("spider: cdp setUserAgentOverride: %w", err)
		}
	}

	navRaw, err := sess.call(ctx, "Page.navigate", map[string]string{"url": r.BuildURL()})
	if err != nil {
		return nil, fmt.Errorf("spider: cdp navigate %s: %w", r.BuildURL(), err)
	}
	var nav struct {
		FrameID   string `json:"frameId"`
		ErrorText string `json:"errorText"`
	}
	if err := json.Unmarshal(navRaw, &nav); err != nil {
		return nil, fmt.Errorf("spider: cdp navigate 响应: %w", err)
	}
	if nav.ErrorText != "" { // net::ERR_* 一类传输级失败
		return nil, fmt.Errorf("spider: cdp navigate %s: %s", r.BuildURL(), nav.ErrorText)
	}

	select {
	case <-loaded:
	case <-ctx.Done():
		return nil, fmt.Errorf("spider: cdp load %s: %w", r.BuildURL(), ctx.Err())
	}

	if d.wait > 0 {
		select {
		case <-time.After(d.wait):
		case <-ctx.Done():
			return nil, fmt.Errorf("spider: cdp wait %s: %w", r.BuildURL(), ctx.Err())
		}
	}

	evalRaw, err := sess.call(ctx, "Runtime.evaluate", map[string]any{
		"expression":    "document.documentElement.outerHTML",
		"returnByValue": true,
	})
	if err != nil {
		return nil, fmt.Errorf("spider: cdp evaluate %s: %w", r.BuildURL(), err)
	}
	var ev struct {
		Result struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"result"`
		ExceptionDetails *struct {
			Text string `json:"text"`
		} `json:"exceptionDetails"`
	}
	if err := json.Unmarshal(evalRaw, &ev); err != nil {
		return nil, fmt.Errorf("spider: cdp evaluate 响应: %w", err)
	}
	if ev.ExceptionDetails != nil {
		return nil, fmt.Errorf("spider: cdp evaluate: %s", ev.ExceptionDetails.Text)
	}

	resp := &spider.Response{
		Header:  make(http.Header),
		Body:    []byte(ev.Result.Value),
		Request: r,
		Cost:    time.Since(start),
	}
	evMu.Lock()
	for i := len(docs) - 1; i >= 0; i-- {
		if docs[i].frameID == nav.FrameID {
			resp.StatusCode = docs[i].status
			for k, v := range docs[i].headers {
				if s, ok := v.(string); ok {
					resp.Header.Set(k, s)
				}
			}
			break
		}
	}
	evMu.Unlock()
	return resp, nil
}

// ---- Chrome 可执行文件发现 ----

func findChrome() (string, error) {
	if p := os.Getenv("CHROME_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	for _, p := range chromeCandidates() {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	for _, name := range []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser"} {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("spider: 未找到 Chrome（可用 CHROME_PATH 环境变量指定路径）")
}

func chromeCandidates() []string {
	switch runtime.GOOS {
	case "windows":
		c := []string{
			`C:\Program Files\Google\Chrome\Application\chrome.exe`,
			`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
		}
		if appdata := os.Getenv("LOCALAPPDATA"); appdata != "" {
			c = append(c, appdata+`\Google\Chrome\Application\chrome.exe`)
		}
		// Edge 同内核，作为最后兜底
		c = append(c,
			`C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe`,
			`C:\Program Files\Microsoft\Edge\Application\msedge.exe`)
		return c
	case "darwin":
		return []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
		}
	default:
		return []string{
			"/usr/bin/google-chrome",
			"/usr/bin/google-chrome-stable",
			"/usr/bin/chromium",
			"/usr/bin/chromium-browser",
		}
	}
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
