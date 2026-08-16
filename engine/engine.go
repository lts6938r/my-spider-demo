package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"my-spider-demo/dedup"
	"my-spider-demo/downloader"
	"my-spider-demo/middleware"
	"my-spider-demo/pipeline"
	"my-spider-demo/scheduler"
	"my-spider-demo/spider"
)

// DefaultWorkers 是 Workers 未设置（<=0）时的并发执行数。
// IO 密集任务的并发度不受 CPU 核数约束：合理上限取
// min(限流允许的并发, 内存/句柄预算)，8 是对多数站点的温和起步值。
const DefaultWorkers = 8

// Spider 是用户接入框架的入口：提供名字与起始请求。
// 解析逻辑以 ParseFunc 的形式随 Request 走；V3 起 ParseFunc
// 会在多个 worker 中并发调用，因此不应读写共享可变状态。
type Spider interface {
	Name() string
	StartRequests() []*spider.Request
}

// ResumeResolver 由支持断点续爬的 Spider 可选实现：
// 为重启后恢复的未完成请求重新指定解析函数。
// Request 的 ParseFunc 是闭包、不可序列化，断点只存 URL——
// 恢复时靠这个钩子把"URL → 解析逻辑"的映射补回来。
// 未实现时恢复的请求只下载、不解析。
type ResumeResolver interface {
	ParseFor(url string) spider.ParseFunc
}

// checkpointDuper 是断点续爬去重器（dedup.Checkpoint）的完整能力。
// 经 Duper 类型断言探测接入（可选能力模式，同 io.Closer），
// 引擎对断点功能无编译期依赖。
type checkpointDuper interface {
	Load() ([]string, error)    // 恢复 visited，返回未完成 URL
	Complete(r *spider.Request) // 请求完成（传输成功）：pending → visited
	Save() error                // 中断时保存状态
	Clear() error               // 正常结束后清除
}

// outcome 是 worker 回传给协调者的执行结果。
type outcome struct {
	req    *spider.Request
	resp   *spider.Response
	err    error
	result spider.Result // err == nil 且 req.Parse != nil 时有效
}

// Engine 把 scheduler.Scheduler / middleware.Middleware / downloader.Downloader / pipeline.Pipeline 串成数据流闭环。
//
// V3 并发架构：单协调者 + Worker 执行池。
//
//	协调者（唯一）                       workers ×N
//	─────────────────                  ──────────────────
//	 scheduler.Scheduler ←─ push 新请求           for req := range jobs
//	     │ Pop                              ├─ 中间件链下载
//	     └─ jobs ───────────────────►      └─ ParseFunc
//	                                   results ──► 协调者回收
//
// 关键不变量（single-writer）：scheduler.Scheduler、去重、统计、ID 分配
// 只被协调者一个 goroutine 触碰；worker 只做无共享状态的执行
// （下载 + 解析）。因此统计与终止判断无需任何锁和原子操作。
//
// 终止条件：pending（已入队未回收 = 排队中 + 在途）归零。
// pending 只在协调者中变更，天然无竞态——这是 V1 "队列空 = 结束"
// 在并发下的严格化表述（队列空 且 无在途请求）。
type Engine struct {
	Sched   scheduler.Scheduler
	Down    downloader.Downloader
	Mws     []middleware.Middleware // 中间件链，Mws[0] 最外层（最先见请求、最后见响应）
	Pipes   []pipeline.Pipeline
	Duper   dedup.Duper  // nil 表示不去重
	Logger  *slog.Logger // nil → slog.Default()
	Workers int          // 并发执行的 worker 数；<=0 时取 DefaultWorkers

	nextID     int64 // 请求编号，仅协调者读写
	Downloaded int   // 成功拿到响应（含 4xx/5xx）
	Failed     int   // 传输失败（含被 ctx 取消）
	Parsed     int   // 实际调用过 ParseFunc 的次数
	Dropped    int   // 被 pipeline.Pipeline 丢弃的 item 数
	Dup        int   // 被去重丢弃的请求数
	Abandoned  int   // 停机后不再调度而被放弃的请求数
}

// Run 以 context.Background() 驱动（不可取消），兼容简单场景。
func (e *Engine) Run(sp Spider) error {
	return e.RunContext(context.Background(), sp)
}

// RunContext 驱动一次完整抓取。ctx 取消（如 SIGINT）触发 graceful shutdown：
// 停止调度新请求 → 取消在途下载 → 排干已回收结果的 item → 退出。
func (e *Engine) RunContext(ctx context.Context, sp Spider) error {
	lg := e.logger()
	download := middleware.Chain(e.Down.Download, e.Mws...)

	workers := e.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}

	jobs := make(chan *spider.Request)
	// results 缓冲 = worker 数 ⇒ 每个 worker 至多压一个未回收结果，
	// 发送永不阻塞 ⇒ worker 必然回到 jobs 接收 ⇒ 协调者的投喂必然
	// 最终成功——这是全链路无死锁的根源。
	results := make(chan outcome, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range jobs {
				results <- e.execute(ctx, req, download)
			}
		}()
	}
	// 资源收尾顺序（defer LIFO，后注册先执行）：
	// close(jobs) → wg.Wait → closeResources——
	// 资源最后关：所有 item 处理完、worker 全部退出之后。
	// 覆盖正常完成、取消停机、panic 三条路径；
	// 关闭对象：实现了 io.Closer 的下载器（如 CDP 浏览器）与管道（文件/SQLite）。
	defer e.closeResources()
	defer wg.Wait()
	defer close(jobs)

	shutdown := false
	// 两个计数都只被协调者修改（single-writer，无竞态）：
	// queued   = 还在队列里等待派发
	// inflight = 已交给 worker、尚未回收结果
	// 停机时只有 inflight 会产生结果，queued 直接放弃——
	// 若混用一个计数，"放弃了一个其实已派发的请求"会让排空永远等不到结果（死锁）。
	queued, inflight := 0, 0

	// 断点续爬：Duper 实现了可选能力接口（dedup.Checkpoint）时启用。
	var cp checkpointDuper
	cp, hasCheckpoint := e.Duper.(checkpointDuper)

	// 恢复上次未完成的请求。Request 携带 ParseFunc（闭包）不可序列化，
	// 队列快照只存 URL，解析函数由 Spider 的可选钩子 ParseFor 重新挂载；
	// Spider 未实现钩子时恢复的请求只下载不解析。
	if hasCheckpoint {
		urls, err := cp.Load()
		if err != nil {
			return fmt.Errorf("engine: 加载断点: %w", err)
		}
		var resolve func(string) spider.ParseFunc
		if rr, ok := sp.(ResumeResolver); ok {
			resolve = rr.ParseFor
		}
		for _, u := range urls {
			var parse spider.ParseFunc // 未实现钩子：只下载不解析
			if resolve != nil {
				parse = resolve(u)
			}
			req, err := spider.NewRequest("", u, parse)
			if err != nil {
				lg.Warn("断点中的 URL 非法，丢弃", "url", u, "err", err)
				continue
			}
			if e.push(req) {
				queued++
			}
		}
		if len(urls) > 0 {
			lg.Info("断点恢复", "resumed", len(urls))
		}
	}

	for _, r := range sp.StartRequests() {
		if e.push(r) {
			queued++
		}
	}

	for !shutdown && (queued > 0 || inflight > 0) {
		// 投喂阶段：把队列里的请求递给空闲 worker。
		// jobs 无缓冲 ⇒ 没有空闲 worker 时阻塞（背压），
		// 在途请求数始终 ≤ worker 数。
	feed:
		for queued > 0 {
			req, ok := e.Sched.Pop(ctx)
			if !ok {
				break // 队列暂时空了，去收结果
			}
			queued--
			e.nextID++
			req.ID = e.nextID
			for {
				select {
				case jobs <- req:
					inflight++
					continue feed
				case o := <-results:
					// 没有空闲 worker：先回收结果把它腾出来。
					// 缺少这个分支会死锁：worker 在旧结果未被回收时
					// 接了新任务、产出第二个结果，发不进已满的缓冲，
					// 从此不再回到 jobs 接收端，协调者的投喂永远阻塞。
					inflight--
					queued += e.handle(o, true)
				case <-ctx.Done(): // 投喂被停机打断：手里这个请求放弃
					e.Abandoned++
					shutdown = true
					break feed
				}
			}
		}
		if shutdown {
			break
		}
		if inflight == 0 {
			continue // 防御：队列有货但没能派发时回到投喂
		}

		// 回收阶段：协调者唯一的主动等待点。
		select {
		case o := <-results:
			inflight--
			queued += e.handle(o, true)
		case <-ctx.Done():
			shutdown = true
		}
	}

	// 停机收尾：队列中未派发的请求放弃（计入 Abandoned）；
	// 在途下载已被请求级 ctx 取消，很快回收。
	// 已完成的 item 照常进入管道——已抓到的数据不丢。
	e.Abandoned += queued
	for inflight > 0 {
		o := <-results
		inflight--
		e.handle(o, false)
	}

	// 断点收尾：中断则保存（visited + pending），完整结束则清除。
	if hasCheckpoint {
		if shutdown {
			if err := cp.Save(); err != nil {
				lg.Error("保存断点失败", "err", err)
			} else {
				lg.Info("断点已保存，下次运行自动续爬")
			}
		} else if err := cp.Clear(); err != nil {
			lg.Warn("清除断点文件失败", "err", err)
		}
	}

	lg.Info("engine done",
		"spider", sp.Name(),
		"downloaded", e.Downloaded,
		"failed", e.Failed,
		"parsed", e.Parsed,
		"dropped", e.Dropped,
		"dup", e.Dup,
		"abandoned", e.Abandoned,
	)
	if shutdown {
		return ctx.Err()
	}
	return nil
}

// execute 是 worker 的单次执行：中间件链下载 + 解析。
// 未自带 ctx 的请求由引擎 ctx 接管（停机时在途下载被立即取消）；
// 用户已设置 ctx 的请求尊重用户自己的预算，不强制合并（design-log 011）。
func (e *Engine) execute(ctx context.Context, req *spider.Request, download middleware.Handle) outcome {
	if req.Context() == context.Background() {
		req = req.WithContext(ctx)
	}
	resp, err := download(req)
	o := outcome{req: req, resp: resp, err: err}
	if err == nil && req.Parse != nil {
		o.result = req.Parse(resp)
	}
	return o
}

// handle 处理一个回收结果，返回本次新调度的请求数。
// schedule=false（停机排空阶段）时新请求只计数不调度；item 照常处理。
func (e *Engine) handle(o outcome, schedule bool) (scheduled int) {
	lg := e.logger()
	if o.err != nil {
		e.Failed++
		lg.Warn("download failed", "id", o.req.ID, "url", o.req.BuildURL(), "err", o.err)
		return 0
	}
	e.Downloaded++
	// 断点续爬：传输成功即记完成（含解析失败——重下也修不了解析 bug；
	// 传输失败不记，留在 pending，下次运行重试）
	if cp, ok := e.Duper.(checkpointDuper); ok {
		cp.Complete(o.req)
	}

	if o.req.Parse == nil {
		return 0 // 纯触发型请求，响应直接丢弃
	}
	e.Parsed++
	if o.result.Err != nil {
		lg.Warn("parse failed", "id", o.req.ID, "url", o.req.URL, "err", o.result.Err)
		return 0 // 数据不完整的 Result 整体丢弃：宁可少，不可错
	}
	if schedule {
		for _, r := range o.result.Requests {
			if e.push(r) {
				scheduled++
			}
		}
	} else if _, ok := e.Duper.(checkpointDuper); ok {
		// 停机但开着断点：子请求照常登记入队（本轮不会执行），
		// Save 时随 pending 落盘，下次运行续爬——否则这些链接
		// 会因父页已 visited 而永远无法再被发现
		for _, r := range o.result.Requests {
			e.push(r)
		}
	} else {
		e.Abandoned += len(o.result.Requests)
		lg.Debug("requests abandoned after shutdown", "n", len(o.result.Requests))
	}
	for _, item := range o.result.Items {
		e.processItem(item)
	}
	return scheduled
}

// push 入队前先过 dedup，返回是否真正入队。
func (e *Engine) push(r *spider.Request) bool {
	if r == nil {
		return false
	}
	if e.Duper != nil && !e.Duper.Visit(r) {
		e.Dup++
		e.logger().Debug("duplicate dropped", "url", r.BuildURL())
		return false
	}
	e.Sched.Push(r)
	return true
}

// processItem 依次过 pipeline.Pipeline 链，第一个 error 即停止（item 丢弃）。
// 在协调者中串行执行：写文件/控制台的管道天然不会交错。
func (e *Engine) processItem(item spider.Item) {
	for _, p := range e.Pipes {
		if err := p.ProcessItem(item); err != nil {
			e.Dropped++
			e.logger().Warn("pipeline dropped item", "err", err)
			return
		}
	}
}

func (e *Engine) logger() *slog.Logger {
	if e.Logger == nil {
		return slog.Default()
	}
	return e.Logger
}

// closeResources 关闭实现了 io.Closer 的组件：下载器（CDP 浏览器进程）
// 与管道（文件句柄、数据库连接）。
// 类型断言探测可选能力，而非把 Close 塞进接口——
// 那会迫使 HTTP 下载器、Console/函数管道实现无意义的空方法。
func (e *Engine) closeResources() {
	if c, ok := e.Down.(io.Closer); ok {
		if err := c.Close(); err != nil {
			e.logger().Warn("close downloader", "err", err)
		}
	}
	for _, p := range e.Pipes {
		if c, ok := p.(io.Closer); ok {
			if err := c.Close(); err != nil {
				e.logger().Warn("close pipeline", "err", err)
			}
		}
	}
}
