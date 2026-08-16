# spider

一个用 Go 标准库实现的 Web 数据采集框架：任务调度（FIFO / 优先级）、中间件链、请求去重、限流、重试、Worker Pool 并发、Graceful Shutdown、双下载器（net/http + CDP 真实浏览器）、JSONL / SQLite 持久化，组件全部可插拔。

## 架构

```
Spider.StartRequests()
        │
        ▼
    Request ──► Scheduler ──► (Middleware 链, V2) ──► Downloader ──► Response
        ▲                                                        │
        │                                                        ▼
        └── 新 Request ◄── Parse (Request.ParseFunc) ◄────────────┘
                                 │
                                 ▼
                              Item ──► Pipeline 链
```

框架负责通用能力（调度 / 并发 / 重试 / 限流 / 持久化），Spider 只负责业务逻辑（产出 Request、解析 Response）。

V3 并发架构：单协调者 + Worker 执行池。协调者是唯一碰 Scheduler / 去重 / 统计的 goroutine（single-writer，无锁正确）；worker 只做下载 + 解析。

## 快速开始

```bash
go test ./...                            # 全部单元测试（httptest 本地服务器，离线可跑）
go run ./cmd/quotes                      # 演示爬虫：默认配置（8 worker）
go run ./cmd/quotes -config configs/example.json   # 配置化：并发/限流/重试/优先级/落盘
go run ./cmd/rss -limit 5                # 真实场景：The Verge RSS → 文章页抓取
go run ./cmd/wired -limit 5              # 真实场景：列表页解析 + HTTP→CDP 降级 + 低频抓取
```

`cmd/rss` 展示"RSS 发现 → 页面抓取"的两级数据流：框架的 `ParseRSS` 解析 RSS 2.0 / RSS 1.0(RDF) / Atom 提取条目链接（自动识别格式，含 ISO-8859-1 字符集转换），Spider 决定入队策略；文章页下载后提取标题（og:title），输出含页面体积等字段。

- stdout 输出 item（JSON 行），stderr 输出结构化日志（JSON 行），`> out.jsonl` 可分离两者
- 运行中按 `Ctrl-C` 触发 graceful shutdown：停止调度新请求 → 取消在途下载 → 已完成的 item 照常落盘 → 退出

## 写一个自己的 Spider

业务代码只需要两样东西：`StartRequests()` 入口和一个 `ParseFunc`。所有通用能力（去重、限流、重试、并发、停机）由框架负责：

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	spider "my-spider-demo"
)

type MySpider struct{}

func (MySpider) Name() string { return "my" }

func (MySpider) StartRequests() []*spider.Request {
	req, err := spider.NewRequest("", "https://example.com/list?page=1", parseList)
	if err != nil {
		log.Fatal(err)
	}
	return []*spider.Request{req}
}

// 一页一策：解析函数作为数据随请求流转，不在 Spider 接口上
func parseList(resp *spider.Response) spider.Result {
	res := spider.Result{}
	res.Items = append(res.Items, spider.Item{"url": resp.Request.URL})

	if next, err := spider.NewRequest("", "https://example.com/list?page=2", parseList); err == nil {
		next.Priority = -1 // 翻页靠后，详情类请求可设更高优先级先行
		res.Requests = append(res.Requests, next)
	}
	return res
}

func main() {
	cfg, err := spider.LoadConfig("config.json") // 传 "" 用默认配置
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := spider.NewEngineFromConfig(cfg, spider.NewHashSetDuper(), spider.ConsolePipeline{})
	if err := eng.RunContext(ctx, MySpider{}); err != nil {
		log.Printf("aborted: %v", err)
	}
	fmt.Println(eng.Downloaded, eng.Failed, eng.Dup) // 运行统计
}
```

## 配置项

| 键 | 默认 | 说明 |
|---|---|---|
| `worker` | `8` | 并发 worker 数；IO 密集，上限 ≈ min(限流并发, 内存/句柄预算) |
| `scheduler` | `fifo` | `fifo` 或 `priority`（`Request.Priority` 大值优先，同级 FIFO） |
| `downloader` | `http` | `http`（net/http）、`cdp`（驱动真实 Chrome，见下）或 `fallback`（HTTP 优先，传输失败/403/429 时惰性降级 CDP） |
| `render_wait` | `0` | CDP 模式：load 事件后的额外渲染等待（如 `"2s"`，等异步数据上屏） |
| `save_dir` | 空 | 非空则把每个成功响应的原文保存到该目录（中间件实现，只存最终成功版；路径写入 `Meta["saved_path"]`） |
| `output` | 空 | 非空则追加 JSONL 文件管道（追加模式，不覆盖历史） |
| `sqlite` | 空 | 非空则追加 SQLite 管道（数据库文件路径） |
| `sqlite_table` | `items` | SQLite 表名（白名单校验） |
| `user_agent` | `spider/0.1` | 未显式设置 UA 的请求使用 |
| `timeout` | `30s` | 单请求整体超时（字符串写法） |
| `max_attempts` | `3` | 重试总次数（含首次）；4xx 不重试，429/5xx/传输错误重试 |
| `retry_base_delay` | `500ms` | 指数退避基数，`base × 2^n` + 抖动，封顶 `retry_max_delay` |
| `retry_max_delay` | `10s` | 单次重试等待上限 |
| `global_qps` | `0`（不限） | 全局令牌桶速率 |
| `domain_qps` | `{}` | 域名级令牌桶，如 `{"example.com": 2}` |
| `log_level` | `info` | `debug` / `info` / `warn` / `error`，JSON 结构化输出到 stderr |

文件中缺失的键保留默认值。中间件链顺序由工厂固定：`Logging → Retry → RateLimit → UA → Downloader`（顺序即语义：重试也受限流约束）。

### CDP 模式：驱动真实浏览器

反爬严格的站点（JS 渲染、浏览器指纹检测）把 `downloader` 切到 `cdp`：

```json
{ "downloader": "cdp", "render_wait": "2s", "worker": 3, "timeout": "30s" }
```

- 需要本机安装 Chrome；共享一个浏览器实例，每个请求一个标签页
- **并发单位是 tab 不是连接**，内存开销大，建议 `worker` 调低（2~4）
- 仅支持 GET 导航；自定义 Header/UA 不生效——真实浏览器指纹正是意义所在；Cookie 跨 tab 共享（登录态自然保持）
- 传输语义与 HTTP 版一致：4xx/5xx 返回 Response、导航失败才 error，重试/限流/去重中间件全部无感复用

### SQLite 存储

`sqlite` 配置为文件路径即可落库（与 `output` JSONL 可同时启用）：

```bash
go run ./cmd/quotes -config configs/example.json   # 修改配置加 "sqlite": "quotes.db"
sqlite3 quotes.db "SELECT json_extract(data,'$.author'), COUNT(*) FROM items GROUP BY 1;"
```

存储选型：Item 是无 schema 的 map，采用「一行一 item 的 JSON 文档」而非固定列——固定列会把业务 schema 强加给通用框架；查询用 SQLite 的 `json_extract`。驱动用 modernc.org/sqlite（纯 Go、无 cgo，Windows 无 gcc 也可编译）。MySQL/ES 等未来通过同一个 `Pipeline` 接口接入。

## 迭代路线

- **V1 最小可用** ✅：Request/Response、Scheduler(FIFO)、Downloader(net/http)、Engine、Pipeline
- **V2 框架化** ✅：Middleware 链（Logging/Retry/RateLimit/UA）、sha256 去重、JSON Config、slog
- **V3 并发与可靠性** ✅：单协调者 + Worker Pool、Context 取消传播、Graceful Shutdown、无锁统计
- **V4 工程化** ✅：Priority Queue（container/heap）、JSONL 文件管道（io.Closer 生命周期）、CDP 浏览器下载器（chromedp）、SQLite 管道（modernc.org/sqlite）
- 未做（按"不为堆功能而做"原则明确取舍）：Proxy/Cookie 中间件、CLI 子命令、Benchmark 报告——需要时按 design-log 的模式扩展

## 设计决策

每个模块的方案取舍与踩坑记录（含一次真实的停机死锁分析）见 [docs/design-log.md](docs/design-log.md)。

## 项目结构

按模块边界组织为 8 个包，依赖方向严格单向（engine → 各组件 → spider 核心类型）：

```
spider/          核心数据类型：Request / Response / Item / Result / ParseFunc
scheduler/       Scheduler 接口 + FIFO / Priority(container/heap) 实现
downloader/      Downloader 接口 + HTTP / CDP(chromedp) / Fallback 降级实现
middleware/      洋葱机制（Handle/Middleware/Chain）+ UA/Logging/Retry/RateLimit/Save
dedup/           Duper 接口 + sha256 Hash Set 去重
pipeline/        Pipeline 接口 + Console / JSONL 文件 / SQLite 实现
rss/             RSS 2.0 / RSS 1.0(RDF) / Atom 解析（含字符集转换）
engine/          单协调者 + Worker Pool、Graceful Shutdown、JSON 配置与组装工厂
cmd/             quotes（演示）/ rss（订阅源抓取）/ wired（列表页 + 降级）
configs/         各场景示例配置
docs/            设计决策记录（design-log）
```
