package dedup

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"my-spider-demo/spider"
)

// Checkpoint 是带持久化的去重器：断点续爬的全部状态。
//
// 两类状态：
//   - visited：已完成下载的指纹——重启后不重抓
//   - pending：已发现未完成的 URL——重启后重新入队
//
// 完成语义（由引擎在处理结果时回调 Complete）：
//   - 传输失败（重试耗尽）：留在 pending，下次运行重试（瞬时故障值得再给一次机会）
//   - 下载成功但解析失败：记 visited（重新下载不会修复解析 bug）
//
// 引擎经 Duper 的可选能力接口（Load/Complete/Save/Clear，类型断言探测）
// 接入本类型，引擎对断点功能无编译期依赖。
// 文件格式 JSON（可读性优先；千万级 URL 应换二进制快照，见 design-log 020）。
type Checkpoint struct {
	path string

	mu      sync.Mutex
	visited map[[32]byte]struct{}
	pending map[[32]byte]string
}

func NewCheckpoint(path string) *Checkpoint {
	return &Checkpoint{
		path:    path,
		visited: make(map[[32]byte]struct{}),
		pending: make(map[[32]byte]string),
	}
}

type checkpointFile struct {
	Visited []string `json:"visited"` // 指纹 hex
	Pending []string `json:"pending"` // 未完成 URL
}

// Load 从断点文件恢复 visited 状态，返回未完成的 URL（排序保证确定性）。
// 文件不存在不是错误：返回空列表，全新开始。
// 注意 pending 不进内存表——恢复的 URL 由引擎重新 push，
// 经 Visit 自然重新登记，避免"已加载又判重"的自锁。
func (c *Checkpoint) Load() ([]string, error) {
	data, err := os.ReadFile(c.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dedup: read checkpoint: %w", err)
	}
	var f checkpointFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("dedup: parse checkpoint %s: %w", c.path, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, h := range f.Visited {
		k, err := decodeFingerprint(h)
		if err != nil {
			return nil, err
		}
		c.visited[k] = struct{}{}
	}
	sort.Strings(f.Pending) // map 无序，排序让恢复顺序确定（也利于测试）
	return f.Pending, nil
}

// Visit 实现 Duper 接口：visited 或 pending 任一命中即判重。
func (c *Checkpoint) Visit(r *spider.Request) bool {
	k := fingerprint(r)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.visited[k]; ok {
		return false
	}
	if _, ok := c.pending[k]; ok {
		return false
	}
	c.pending[k] = r.BuildURL()
	return true
}

// Complete 由引擎在请求完成（传输成功）时回调：pending → visited。
func (c *Checkpoint) Complete(r *spider.Request) {
	k := fingerprint(r)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, k)
	c.visited[k] = struct{}{}
}

// Save 写状态快照。临时文件 + 重命名，避免写一半崩溃留下损坏文件。
func (c *Checkpoint) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := checkpointFile{
		Visited: make([]string, 0, len(c.visited)),
		Pending: make([]string, 0, len(c.pending)),
	}
	for k := range c.visited {
		f.Visited = append(f.Visited, hex.EncodeToString(k[:]))
	}
	for _, u := range c.pending {
		f.Pending = append(f.Pending, u)
	}
	sort.Strings(f.Visited)

	data, err := json.Marshal(&f)
	if err != nil {
		return fmt.Errorf("dedup: marshal checkpoint: %w", err)
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("dedup: write checkpoint: %w", err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("dedup: rename checkpoint: %w", err)
	}
	return nil
}

// Clear 正常完成后删除断点文件（无事可续）。文件不存在视为成功。
func (c *Checkpoint) Clear() error {
	if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("dedup: remove checkpoint: %w", err)
	}
	return nil
}

func decodeFingerprint(s string) ([32]byte, error) {
	var k [32]byte
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(k) {
		return k, fmt.Errorf("dedup: 非法指纹 %q", s)
	}
	copy(k[:], b)
	return k, nil
}
