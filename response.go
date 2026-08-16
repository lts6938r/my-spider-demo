package spider

import (
	"net/http"
	"time"
)

// Response 是 Downloader 产出的统一响应。
//
// Body 在下载阶段已被完整读入内存并关闭底层连接：
// 上层模块（解析、管道）接触不到 io.ReadCloser，
// 因此不存在忘记 Close 导致连接无法回收的问题。
// 代价是大响应的内存占用——V1 接受该取舍，
// 真实的大文件抓取需求出现时再增加流式下载通道。
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte

	// Request 是产生本响应的请求回溯引用，
	// 解析函数通常需要它（读取 Meta、区分页面来源）。
	Request *Request

	// Cost 记录从发起请求到读完 body 的总耗时。
	Cost time.Duration
}

// Text 将 Body 按 UTF-8 转为字符串。
// V1 不做 charset 探测，非 UTF-8 页面的解码由解析函数自行处理。
func (r *Response) Text() string {
	return string(r.Body)
}
