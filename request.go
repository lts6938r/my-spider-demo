package spider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Item 是 Spider 从页面中提取出的一条数据。
// V1 采用松散的 map 结构：对调用方零约束、天然兼容 JSON 序列化；
// 强类型 Item 等出现真实的管道需求后再评估。
type Item map[string]any

// ParseFunc 是业务逻辑的最小单元：从 Response 中提取 Item，
// 并产出后续要抓取的 Request。
type ParseFunc func(resp *Response) Result

// Result 是一次 Parse 的产出：数据 + 后续请求。
type Result struct {
	Items    []Item
	Requests []*Request

	// Err 表示解析失败（如页面结构与预期不符）。
	// 解析函数只负责报告，记录与计数由框架统一处理。
	Err error
}

// Request 描述一次"待执行"的抓取任务。
//
// Request 是纯数据描述（外加一个解析回调），本身不执行网络 IO：
// 调度、去重、限流、重试、下载全部由框架的其他模块基于这份描述完成。
// 这也是 Spider 不能直接调用 http.Client 的原因——
// 调用即执行，调度器将失去对请求的控制权（无法排队、判重、重试）。
type Request struct {
	// URL 只包含 scheme://host/path 部分，query 统一存放在 Query 字段，
	// 避免同一信息存两处（单一事实来源）。
	URL string

	// Method 为空时按 GET 处理，构造时统一转为大写。
	Method string

	Header http.Header

	// Query 是 URL query 的唯一存放处，下载时经 BuildURL 合并。
	Query url.Values

	// Body 是请求体（POST 等），V1 仅支持 []byte。
	Body []byte

	// Meta 携带跨请求流转的业务元数据（如翻页深度、列表页 ID），
	// 框架不解释其内容，也不会自动复制到新请求上。
	Meta map[string]any

	// Priority 调度优先级：数值越大越先被调度，默认 0。
	// 典型用法：详情页(10) 先于翻页(0)，让高价值数据先行。
	// 只影响顺序，不影响去重——去重管身份，调度管顺序。
	Priority int

	// ID 由引擎出队时分配的顺序编号，用于日志与观测追踪；
	// 同一请求的多次重试共享同一 ID（重试不重新出队）。
	ID int64

	// Parse 指定框架收到该请求的响应后调用哪个解析函数。
	// 为 nil 表示丢弃响应（纯触发型请求）。
	Parse ParseFunc

	// ctx 遵循 net/http.Request 的惯例：
	// 不导出、永不为 nil，通过 Context / WithContext 访问。
	ctx context.Context
}

// NewRequest 创建一个请求。rawURL 中的 query 会被拆到 Query 字段，
// fragment 直接丢弃（fragment 本就不会发送到服务端）。
func NewRequest(method, rawURL string, parse ParseFunc) (*Request, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("spider: parse url %q: %w", rawURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("spider: url %q missing scheme or host", rawURL)
	}

	if method == "" {
		method = http.MethodGet
	}
	q := u.Query()
	u.RawQuery = ""
	u.Fragment = ""

	return &Request{
		URL:    u.String(),
		Method: strings.ToUpper(method),
		Header: make(http.Header),
		Query:  q,
		Parse:  parse,
		ctx:    context.Background(),
	}, nil
}

// BuildURL 返回下载时实际使用的完整 URL（path 与 query 的合并结果）。
func (r *Request) BuildURL() string {
	if len(r.Query) == 0 {
		return r.URL
	}
	sep := "?"
	if strings.Contains(r.URL, "?") {
		sep = "&"
	}
	return r.URL + sep + r.Query.Encode()
}

// Context 返回与 Request 关联的 context，保证永不为 nil。
// 零值 Request 的 ctx 字段是 nil 接口，这里兜底返回 Background，
// 避免下游对 nil 调用 Done() 时 panic。
func (r *Request) Context() context.Context {
	if r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// WithContext 返回关联了新 context 的 Request 浅拷贝，
// 语义与 net/http.Request.WithContext 一致。
// 注意：Header、Query、Meta 等 map 在副本间是共享的。
func (r *Request) WithContext(ctx context.Context) *Request {
	if ctx == nil {
		panic("spider: nil context")
	}
	r2 := new(Request)
	*r2 = *r
	r2.ctx = ctx
	return r2
}
