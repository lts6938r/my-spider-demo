package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"my-spider-demo/spider"
	"os"
	"sync"
)

// Pipeline 处理解析产出的 spider.Item（清洗、校验、持久化……）。
//
// spider.Item 是 map 引用，Pipeline 可以就地增删改字段；
// 返回 error 表示该 item 应被丢弃（如校验失败），
// Engine 会停止链上后续 Pipeline 并记录。
// 持有资源的实现（文件、数据库）出现时，接口将增加 Close 生命周期（V2+）。
type Pipeline interface {
	ProcessItem(item spider.Item) error
}

// PipelineFunc 让普通函数直接当作 Pipeline 使用（http.HandlerFunc 同款手法）。
type PipelineFunc func(item spider.Item) error

func (f PipelineFunc) ProcessItem(item spider.Item) error { return f(item) }

// ConsolePipeline 把每个 spider.Item 序列化成一行 JSON 打印到 stdout。
// JSONL 格式天然可重定向到文件：go run ./cmd/quotes > out.jsonl
type ConsolePipeline struct{}

func (ConsolePipeline) ProcessItem(item spider.Item) error {
	b, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("console pipeline: %w", err)
	}
	// 注意转 string：Fprintln 直接打 []byte 会输出成 [123 34 ...] 数字数组
	_, err = fmt.Fprintln(os.Stdout, string(b))
	return err
}

// JSONLFilePipeline 把每个 spider.Item 以 JSON 行追加写入文件（jsonl：
// 每行一个独立 JSON，天然支持流式读取和 grep/wc 等工具链）。
//
// 持有文件句柄，因此实现 io.Closer，由引擎在退出时探测并关闭——
// "可选能力"模式：Pipeline 接口保持最小，Console/函数管道
// 不必被迫实现空的 Close。
// 文件以追加模式打开：重复运行累积，不覆盖历史数据。
type JSONLFilePipeline struct {
	path   string
	mu     sync.Mutex
	f      *os.File
	enc    *json.Encoder
	closed bool
}

func NewJSONLFilePipeline(path string) *JSONLFilePipeline {
	return &JSONLFilePipeline{path: path}
}

// ProcessItem 惰性打开文件（首个 item 到达时），后续复用句柄。
func (p *JSONLFilePipeline) ProcessItem(item spider.Item) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("spider: jsonl pipeline already closed")
	}
	if p.f == nil {
		f, err := os.OpenFile(p.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("spider: open %s: %w", p.path, err)
		}
		p.f = f
		p.enc = json.NewEncoder(f)
	}
	if err := p.enc.Encode(item); err != nil {
		return fmt.Errorf("spider: write %s: %w", p.path, err)
	}
	return nil
}

// Close 关闭文件句柄，幂等（重复调用返回 nil）。
func (p *JSONLFilePipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.f == nil {
		return nil
	}
	return p.f.Close()
}
