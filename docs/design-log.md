# 设计决策记录

每条记录：背景 / 备选方案 / 结论与理由。面试时回答"为什么这样设计"的素材库，正序追加。

## 001 Request / Response 抽象（2026-08-16，V1）

**问题：为什么需要统一抽象，而不是 Spider 里直接调 http.Client？**

`http.Client.Do()` 是"调用即执行"；Request 是"可反复消费的意图"。去重、限流、重试、优先级调度都要求框架先拿到一份尚未执行的请求描述，决策后才执行——直接调用没有这些环节的插入点。附带收益：Spider 单测可完全离线（手工构造 Response）；body 生命周期责任收口在 Downloader 一处。

**问题 1：解析逻辑放在哪？**

- A. Spider 接口单 `Parse` 方法：接口最简，但列表页/详情页多页面类型时要在 Parse 内靠 URL/Meta 做 if-else 分支
- B. `Request.Parse ParseFunc` 回调随请求走（Scrapy 模式）：天然支持"一页一策"，Engine 无需感知具体 Spider 类型
- C. URL 模式路由表：对爬虫场景过度设计

→ **采用 B**。Spider 接口不消失，退化为入口（`StartRequests()`），V1 引擎轮引入。

**问题 2：Request 字段一次给全还是按消费者分期？**

Priority（V4 优先级队列才消费）、RetryCount（V3 重试才消费）暂不加入：没有消费者的字段是投机设计，Go 给结构体加字段零成本，分期加入。

**问题 3：Response.Body 物化还是流式？**

物化为 `[]byte`，`io.ReadCloser` 在 Downloader 内读完即 Close，上层碰不到需要关闭的资源——用内存换"绝无 body 泄漏"。大文件流式下载留给真实需求出现时再加。

**问题 4：URL 与 Query 的单一事实来源**

`URL` 只存 scheme://host/path，query 只存 `Query url.Values`，下载时 `BuildURL()` 合并。同一信息存两处迟早漂移（改了 Query 忘了 URL）。

**问题 5：Context 惯例**

完全镜像 `net/http.Request`：`ctx` 不导出；`Context()` 永不返回 nil（零值结构体的 ctx 字段是 nil 接口，需兜底 Background，否则下游调用 `Done()` panic）；`WithContext()` 返回浅拷贝（Header/Query/Meta 等 map 共享）。

**命名说明**：Header 字段名对齐 `net/http`（`Header http.Header`）而非 "Headers"，降低使用者的心智切换成本。

## 002 Scheduler（2026-08-16，V1）

**问题 1：Pop 的语义——阻塞、返回 nil、还是返回 bool？**

- A. 返回 `(*Request, bool)`，空队列立即 false（当前引擎单线程，"队列空 = 结束"）
- B. 阻塞式 Pop（V3 worker 需要空队列时挂起等待，而非忙轮询）
- C. 暴露 channel（`Chan() <-chan *Request`，把队列实现和 channel 绑死）

→ **采用 A，但签名预留 ctx**：`Pop(ctx context.Context) (*Request, bool)`。返回 nil 会与"合法的 nil 请求"混淆；当前实现不阻塞，V3 并发版在同一契约下改为阻塞等待 + 响应取消，引擎调用点不变。

**问题 2：实现选型**

- slice + mutex：最直白；出队用 head 下标前移，越过半容量再搬移（均摊 O(1)），出队处置 nil 让 GC 回收
- buffered channel：天然阻塞/背压，但容量固定，V1 要无界
- container/list：O(1) 头删但指针开销、缓存不友好

→ **slice + mutex**。channel 方案在 V3 讨论 Worker Pool 与背压时再评估（固定容量 + Push 阻塞 = 内存控制的免费机制）。

**问题 3（上轮遗留）：去重放 Scheduler 内还是外？**

→ **独立组件**。去重回答"见没见过"（存储+策略问题：set/bloom/磁盘），调度回答"何时执行"（顺序问题：FIFO/priority），变化维度不同，耦合会让 V2 换 Bloom Filter 时被迫动调度器。

## 003 Downloader（2026-08-16，V1）

**问题 1：什么算失败？** 传输层拿到 HTTP 响应即成功——4xx/5xx 也是 Response，由解析/中间件决定业务含义；连接拒绝、超时、DNS 才是 error。否则 404 的重试策略会被迫侵入下载层。

**问题 2：context 随调用走还是随数据走？**

- 常规 Go 签名 `Download(ctx, r)`：但 Request 自带 ctx 字段，两个来源会打架
- `Download(r)`，内部用 `r.Context()`：取消语义随请求流转，引擎/中间件负责把生命周期组合进 Request（`WithContext`）

→ **随数据走**。与"Request 是完整意图描述"的定位一致，V3 停机 = 引擎派生 root ctx 注入每个请求。

**问题 3：连接池与 body 生命周期**。`MaxIdleConnsPerHost` 默认仅 2，单域名抓取必须调大；body 必须 `io.ReadAll` 到 EOF 再 `Close`，连接才能回到空闲池被复用——读完关、错误也关，Close 责任不出 Download 函数。

**开发中真实踩坑**：`fmt.Fprintln(os.Stdout, []byte)` 会把 JSON 打成 `[123 34 ...]` 数字数组（%v 对 byte slice 的行为），转 `string(b)` 修复——测试当场抓住。

## 004 Engine + Spider（2026-08-16，V1）

**问题 1：Spider 接口留多大？** 只有 `Name()` + `StartRequests()`。解析不在接口上（随 Request 的 ParseFunc 走），一个 Spider 天然支持多页面类型，引擎也无需感知具体 Spider。

**问题 2：V1 怎么判断结束？** 单线程同步：队列空 = 无在途请求 = 结束。这个判断在 V3 多 worker 下失效（队列空时可能有在途请求会再产出新请求），届时需要"在途计数 + ctx 取消"，这是 V3 的核心设计点之一。

**问题 3：Result.Err 的语义**。解析报错 → 整个 Result（含半成品 items 和新 requests）丢弃。数据不完整的条目宁可少不可错。

**问题 4：Pipeline 链的错误**。第一个 error 即停止后续管道并丢弃该 item（校验失败不该落库），其他 item 继续处理。

## 005 Pipeline（2026-08-16，V1）

**问题 1：接口形态**。`ProcessItem(Item) error`，就地修改 map（Clean 场景）+ error 表示丢弃（Validate 场景），Scrapy 兼容心智模型。流式/channel 版本对 V1 过度设计。

**问题 2：为什么只有 Console 实现**。文件/数据库 Pipeline 持有资源，需要 Close 生命周期，接口要长出 `Close() error`——等真实持久化需求出现（V2）再演进，现在加是投机设计。附带 `PipelineFunc` 适配器（http.HandlerFunc 同款手法）让测试和临时管道零成本接入。

## 006 Middleware 机制（2026-08-16，V2）

**问题：链的形态——函数式洋葱、双钩子接口、还是事件订阅？**

- A. `type Middleware func(next Handle) Handle`：洋葱模型，`next` 前是请求阶段、后是响应阶段
- B. `ProcessRequest/ProcessResponse` 接口：阶段显式，但**表达不了 Retry**——重试需要"重新发起下载"，只有包装调用本身才做得到，观察式钩子没有控制流
- C. 事件总线：解耦最彻底，但同样没有同步控制流，且 V2 场景下过度设计

→ **采用 A**。装配即循环：`for i := len(mws)-1; i >= 0; i-- { h = mws[i](h) }`，`Mws[0]` 最外层。`Handle` 与 `Downloader.Download` 同构，中间件可以包装任何下载函数——Downloader 接口因此不用动。

## 007 Retry / Backoff（2026-08-16，V2）

**错误分类**：传输错误 → 重试；429/500/502/503/504 → 重试；其余 4xx → 不重试；请求 ctx 已死 → 不重试（ctx 已死，再试只会立即失败）。关键区分：用 `r.Context().Err()` 判断"用户级预算耗尽"（不重试），而 `Client.Timeout` 这类网络超时不会置位请求 ctx，仍按传输错误重试——两者混为一谈就会把"该重试的超时"和"不该重试的取消"搞反。

**退避**：`base * 2^(n-1)` + [0,25%) 抖动，抖动后以 MaxDelay 封顶（含 int64 左移溢出保护）。抖动防 thundering herd：多个客户端同步重试会在同一时刻集中打爆服务器。

**链上位置**：Retry 在 RateLimit 外层——每次真实发包（含重试）都要重新过限流，否则重试绕过限流。

## 008 RateLimit（2026-08-16，V2）

**自写令牌桶 vs golang.org/x/time/rate**：核心只有几十行（连续补充、突发容量、ctx 打断），亲手实现一次比引依赖更值得；x/time/rate 留给 V4 需要更复杂语义（分布式、优先级）时评估。

**语义**：容量 = ceil(QPS)，允许瞬时突发一整秒的量再进入匀速；等待用 `select { time.After | ctx.Done }`，被取消立即返回。全局桶 + 域名桶两级依次获取。多个 worker 并发时靠 mutex 保证"补充-消费"原子——这就是限流必须实现为共享组件、不能各 worker 自带计数器的原因。

## 009 请求去重（2026-08-16，V2）

**指纹定义**：`sha256(Method + "\n" + BuildURL())`。`Query.Encode()` 按键名排序输出，天然消除 query 顺序差异。Header 不入指纹（UA 变化不应导致重抓）；Body 不入指纹（POST 同 URL 不同 body 会被误判，届时再演进）。**检查与记录必须在同一临界区**（`Visit` 返回 bool），否则并发下同一 URL 会入队两次。

**内存账**：32B 指纹 + map 槽位 ≈ 80~100B/URL，千万级约 1GB。Bloom Filter 用 ~1/8 内存换误判（漏抓率可控、永不误放行），亿级 URL 或内存受限时切换——这是"什么情况下该用 Bloom"的量化答案。

## 010 Config + Logger（2026-08-16，V2）

**Config**：JSON（标准库零依赖，YAML 需要第三方）+ `Duration` 字符串解析（"5s"，原生只认纳秒数字）。缺失键保留默认值。只收录有消费者的键——Worker/Proxy/Output 等出现消费者再进配置。

**Logger**：`log/slog`（Go 1.21+ 标准库的结构化日志），JSON 输出到 stderr，级别可配。引擎与中间件共享同一个 logger 实例；请求带 `ID`（引擎出队时分配，重试共享），日志可按 id 串联一次抓取的完整生命周期——"为什么这个 URL 失败"= 按 id 过滤。

**组装工厂**：`NewEngineFromConfig` 固化中间件顺序 Logging → Retry → RateLimit → UA，顺序即语义（Logging 看全程、重试受限流），不暴露给使用者拼装出错的机会。

## 011 V3 并发模型（2026-08-16）

**问题：worker 如何消费队列？**

- A. N 个 worker 直接 Pop 队列（直觉方案）：终止判断"队列空 && 在途为 0"是两次独立的原子读，存在 TOCTOU——读完队列为空、读在途为 0 之间恰好有人 push 又退出，会误判完成。修复要把队列状态与计数放进同一临界区或引入唤醒通道，复杂度暴涨
- B. **单协调者 + Worker 执行池**：协调者是唯一碰 Scheduler/去重/统计/ID 的 goroutine（single-writer），worker 只做无共享的执行（下载 + 解析）

→ **采用 B。并发正确性最便宜的获得方式不是加锁，而是消除写并发**：全部统计字段、ID 分配保持普通 int/无锁即正确。

**终止条件**：`queued`（排队）与 `inflight`（已派发未回收）两个计数都只在协调者中变更——V1"队列空 = 结束"在并发下的严格化。**死锁论证**：results 缓冲 = worker 数 ⇒ worker 发送结果永不阻塞 ⇒ worker 必回到 jobs 接收 ⇒ 协调者的 `jobs <- req` 必然最终成功。jobs 无缓冲 ⇒ 在途 ≤ worker 数（背压，内存有界）。

**开发中真实踩坑（值得记住）**：初版用单一 `pending` 计数混装排队与在途。停机时 `select { jobs<-req | ctx.Done() }` 两个分支同时就绪，Go 随机选择——运气差时把一个"已派发"的请求当作"已放弃"扣减计数，排空循环永远等不到它的结果 → 死锁。测试以 30s 超时的 goroutine 转储抓到（worker 全部停在 `range jobs`）。修复：计数拆分为 queued/inflight，排空只等 inflight。教训：**select 的随机分支选择本身就是竞态来源，两个分支都就绪时必须保证任一选择都正确**。

**取消传播**：`signal.NotifyContext` → root ctx →（1）协调者 select 停止投喂（2）worker 给未自带 ctx 的请求 `WithContext(root)` → 在途下载即刻中止。停机收尾：队列中未派发的计入 Abandoned；已完成的 item 照常入管道（数据不丢）。

**已知局限**：用户自带 ctx 的请求不被 root 强制取消——标准库没有 ctx 合并原语（需自写桥接 goroutine），V3 选择文档化而非复杂化，列入未来工作。

**Worker 数**：IO 密集，不受 CPU 核约束；上限 ≈ min(限流允许的并发, 内存/句柄预算)。默认 8。

## 012 V4：Priority Queue + 文件 Pipeline（2026-08-16）

**Priority Queue**：`container/heap` 实现，落地即插进 V1 预留的 `Scheduler` 接口——引擎零改动，这是"接口预留"的兑现时刻。语义决策：`Priority` 大值优先（直觉），heap 算法本身不稳定（相同优先级出队顺序不保证），用单调递增的入队序号打破平局 ⇒ 同优先级严格 FIFO ⇒ **优先级调度是 FIFO 的严格超集**，行为完全可预测（有专门测试验证"全 0 优先级 == FIFO"）。Priority 不参与去重指纹：去重管身份，调度管顺序——同一 URL 不同优先级仍是同一请求。

**文件 Pipeline 与 Close 生命周期**：Pipeline 接口保持一个方法不变，资源型管道实现 `io.Closer`，引擎退出时用类型断言探测并关闭——"可选能力"模式。对比把 `Close()` 塞进 Pipeline 接口：会破坏所有既有实现和 `PipelineFunc`（被迫写空方法）。收尾顺序（defer LIFO）：`close(jobs) → wg.Wait → closePipelines`——所有 item 处理完、worker 全部退出之后才关文件，正常完成、取消停机、panic 三条路径都覆盖。jsonl 追加模式：重复运行累积不覆盖。停机语义有专门测试：快页 item 必须已落盘、慢页被取消的不能落盘。

## 013 CDP 浏览器下载器（2026-08-16，V4 增量）

**问题：反爬严格的站点怎么办？** net/http 的 TLS/JA3 指纹、无 JS 执行，一眼爬虫。方案：CDP 驱动真实 Chrome。库选 chromedp（事实标准的 Go CDP 封装）——这是章程允许的第三方依赖："标准库难以合理解决"（CDP 是 websocket 上的上百个域的协议，裸写不现实）。

**关键：零引擎改动**。CDPDownloader 实现 V1 就存在的 `Downloader` 接口，调度/去重/限流/重试/停机对下载方式完全无感知——两层抽象各兑现一次。传输语义对齐 HTTP 版：4xx/5xx 返回 Response、导航失败才 error ⇒ RetryMiddleware 的分类逻辑直接复用。

**并发模型**：共享一个浏览器实例 + 每请求一个 tab；tab 内存远大于连接，此模式 Workers 应调低（2~4）——"Worker 数上限 ≈ min(限流并发, 资源预算)"在浏览器场景的具体化。

**ctx 合并问题终于躲不开**：chromedp 的 tab ctx 必须派生自浏览器 ctx，而请求自带 ctx（超时/停机）要桥接进来。解法是"桥接 goroutine"：监视 `reqCtx.Done()` 取消 tab，以 `parent.Done()` 为退出条件防泄漏（V3 记录的"未来工作"在此落地）。

**测试**：JS 渲染用例是本质证明——页面源码只有 `loading`，JS 执行后才有 `rendered-by-js`，断言渲染后 DOM 包含之；另测 404 非 error、请求级取消及时返回。本机无 Chrome 时 Skip。

## 014 SQLite 存储管道（2026-08-16，V4 增量）

**驱动选型：modernc.org/sqlite（纯 Go）而非 mattn/go-sqlite3（cgo）**——本机无 gcc，cgo 驱动无法编译；纯 Go 版跨平台零工具链，代价是性能略低，对爬虫写入量（协调者串行写）完全够。这是"环境约束决定选型"的直观案例。

**Schema 取舍**：Item 是无 schema 的 map。固定列表会把业务 schema 强加给通用框架（换站点就要改表）；采用「一行一 item 的 JSON 文档」：`items(id, data JSON, created_at)`，查询用 `json_extract(data, '$.author')`。MySQL/ES 未来走同一个 Pipeline 接口。

**注入防护**：表名不能走占位符参数，必须白名单校验（`^[A-Za-z_][A-Za-z0-9_]*$`）后拼接——SQL 注入的非常规入口。

**踩坑**：引擎组装后若从未 Run，`closeResources` 不会执行，Windows 下未关闭的 SQLite 句柄会锁住临时文件（`t.TempDir` 清理失败）。测试中显式收尾；也说明"资源生命周期绑定在 Run 上"的契约需要在文档中明示。

## 015 RSS 解析与第二次死锁（2026-08-16）

**RSS 的架构定位**：格式级解析（XML → 条目列表）进框架（`rss.go`，与 JSON 解析同级）；"把链接变成请求去抓"是抓取策略，留在 Spider 层组合。`ParseRSS` 支持 RSS 2.0 与 Atom 自动识别；Atom 的链接取 `rel` 为空或 alternate（人类可读页），跳过 rel=self。

**第二次死锁（真实场景测试抓出来的）**：RSS 一轮解析产出 3 个链接，引擎随即死锁——goroutine 转储显示协调者阻塞在投喂的 `jobs <- req`，worker 阻塞在 `results <-`。根因：V3 死锁修复后的简化删掉了投喂循环里"顺手回收结果"的 select 分支，依据是一条错误的不变量——"results 缓冲 = worker 数 ⇒ 发送永不阻塞"。反例：worker 发出结果后**不必等协调者回收**就会去接下一个 job，单个 worker 即可压着两个未回收结果，塞满缓冲后 worker 卡在发送、不再回到 jobs 接收端，协调者卡在投喂。既有测试全是"每轮只产一个新请求"的链式拓扑，从未覆盖多轮派发。修复：投喂 select 恢复 `jobs<-req / <-results / ctx.Done` 三分支——发不出去时先回收结果把 worker 腾出来。回归测试：RSS 爬取用例 + 并发测试加到 12 请求（超过两轮派发）。教训：**不变量论证错一句就是死锁；测试拓扑必须覆盖"宽产出"（一轮多个子请求），只有链式不够**。011 的"无死锁论证"以本条为准修正。

## 016 SaveResponseMiddleware：响应原文落盘（2026-08-16）

**问题：保存网页原文的正确位置在哪？**

- A. 解析函数把 HTML 塞进 Item → 现有管道可用，但 Item 臃肿、SQLite 一行存整页
- B. 新增落盘管道 → **走不通**：Pipeline 接口只见 Item，看不到 Response
- C. **中间件** → 响应阶段天然拿得到完整 body，业务零感知，与其他管道并存

→ **采用 C**。这是 Pipeline 接口设计的必然推论：管道面向"提取后的结构化数据"，原文保存是传输层的横切关注点，归属中间件。

**链上位置**：必须在 Retry 外层——只保存最终成功的响应，重试途中的 429/5xx 中间产物不落盘（有专门测试锁定该语义：2 次 503 + 1 次 200 → 目录恰好 1 个文件）。

**语义**：保存失败只记 Warn、不影响抓取（辅助能力不应拖死主流程，有测试）；成功把路径写入 `Meta["saved_path"]`，解析函数可带上 Item 形成"元数据 → 原文"的索引闭环。文件名 = URL 末段（净化非法字符、截断 80 字符）+ URL 哈希前 4 字节（防冲突）；扩展名按 Content-Type 推断（html/xml/json/bin）。

**真实场景验证**（The Verge）：6 个响应全部落盘——RSS 源正确得到 `.xml`（按 Content-Type），5 篇文章 `.html`，文件大小与 Item 的 bytes 字段逐一对上，jsonl 里每条记录携带原文路径。

## 017 第二个真实订阅源：RSS 1.0 (RDF) 与字符集（2026-08-16）

**换了 Slashdot 一测，立刻暴露三个格式问题**——这就是真实场景测试的价值：

1. **RSS 1.0 (RDF) 的条目与 channel 平级**（根元素是 `<rdf:RDF>`，`<item>` 不在 `<channel>` 内）。修复：结构体同时收两个位置的 item，RSS 2.0 与 RDF 一套结构通吃。
2. **声明编码是 ISO-8859-1**，而 Go 的 `encoding/xml` 只接受 UTF-8。修复：用官方预留的 `Decoder.CharsetReader` 钩子做转换——Latin-1 的字节值即 Unicode 码点，按字节映射即可，无需引 `golang.org/x/text`。
3. **RDF 的发布时间在 Dublin Core 的 `<dc:date>`** 而非 `<pubDate>`。修复：结构体多收一个字段（encoding/xml 按局部名匹配，忽略命名空间前缀），`firstNonEmpty` 取值。

**顺带的工程改进**：解析失败的错误信息现在携带 RSS/Atom 两种格式各自的失败原因（此前被"无法识别"一句话掩盖，这次定位全靠日志里贴出的文档开头）。测试用 `\xE9` 构造真实 Latin-1 字节验证转换，而非 UTF-8 假样本。

## 018 FallbackDownloader：HTTP 优先、失败降级 CDP（2026-08-16）

**问题：列表页 + 文章页抓取，部分站点会拦截直连。**

**方案**：下载器层的组合实现 `FallbackDownloader{primary, secondary}`——降级是"下载方式选择"，归属 Downloader 实现层，引擎与中间件零改动（Downloader 接口第三次兑现）。**判定集合**：传输错误或 403/429（典型反爬拦截）；404/500 不降级（那是业务语义，归重试/解析管）。双侧失败返回**降级侧**错误（更接近问题最新状态）。

**惰性启动**：CDP 由 `lazyCDP`（sync.Once）包装，首次降级才拉起 Chrome——全程 HTTP 成功的运行不付浏览器成本。真实跑 Wired 时 HTTP 全部直连成功，Chrome 全程未启动，恰好验证了这一点。

**已知取舍**：限流在降级层之外，一次降级序列（HTTP+CDP 两次真实访问）只取一个令牌；降级是少数路径，接受并记录。降级与 Retry 的组合顺序：Retry(Fallback(HTTP, CDP))——重试的每次尝试内部先 HTTP 后 CDP。

**礼貌性配置**（用户要求低并发低频）：`worker: 2`、`domain_qps: {www.wired.com: 1}`、`global_qps: 2`、`max_attempts: 2`。Wired 文章页 1.4~1.6MB/篇，低频也是对自己带宽的节约。


## 019 按模块边界拆包（2026-08-16）

**时机**：V1 时单包是有意为之（"拆包也容易"），现在拆是因为 8 个模块边界经四个版本 + 五次真实场景验证后已经稳定——拆包是把验证过的边界物理化，而不是提前猜边界。

**粒度**：按**接口缝**拆，不按文件拆——`spider`（核心数据类型：Request/Response/Item/Result/ParseFunc）、`scheduler`、`downloader`（HTTP/CDP/Fallback）、`middleware`（链 + 四个实现）、`dedup`、`pipeline`（三种实现）、`rss`、`engine`（引擎 + 配置 + 组装工厂）。依赖严格单向：engine → 各组件 → spider；spider 零依赖。测试随实现走（引擎级测试移入 engine 包），跨包仅 testLogger 一行重复——不值得为此引入 testutil 包。

**代价与收获**：收获是物理边界强制低耦合（scheduler 无法伸手摸 downloader 的内部）+ 72 个测试按包独立运行（改中间件只跑 middleware 包的测试）；代价是引用要带包名、`lazyCDP` 等内部构造器需要导出（`NewLazyCDP`）。单包 vs 多包在此规模都成立——拆是因为边界已验证稳定且被明确要求，不是因为单包错误。

**机械重构的方法教训**：批量 sed/python 替换会产生"字段名被误限定"（`Duper dedup.Duper`）、"函数名被误伤"（`TestEngineWithRetryMiddleware` → `TestEngineWithmiddleware.RetryMiddleware`）、"双限定"（`scheduler.scheduler.`）三类损伤——全部被 `go vet` 逐个暴露，修完 72 测试零丢失。工具能加速机械部分，边界（）和上下文（函数名、字段名）仍要人盯。
