package spider

import (
	"crypto/sha256"
	"sync"
)

// Duper 负责请求去重：判断并记录"是否第一次见到该请求"。
// 独立于 Scheduler 存在——去重是存储+策略问题（set/bloom/磁盘），
// 调度是顺序问题（FIFO/priority），两者变化维度不同，
// 耦合会让"HashSet 换 Bloom Filter"被迫牵动调度器。
type Duper interface {
	// Visit 原子地"检查并记录"：true = 首次出现，false = 重复。
	// 检查和记录必须是同一次临界区，否则并发下同一 URL 会入队两次。
	Visit(r *Request) bool
}

// HashSetDuper 基于 sha256 指纹 + map 的精确去重（无误判、不漏判）。
// 内存账：每条目 32B 指纹 + map 槽位开销，约 80~100B/URL；
// 千万级 URL 约 1GB、亿级约 8~10GB——这就是 Bloom Filter 的引入时机
// （可容忍误判换取 ~1/8 内存），见 design-log 009。
type HashSetDuper struct {
	mu   sync.Mutex
	seen map[[32]byte]struct{}
}

func NewHashSetDuper() *HashSetDuper {
	return &HashSetDuper{seen: make(map[[32]byte]struct{})}
}

func (d *HashSetDuper) Visit(r *Request) bool {
	k := fingerprint(r)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[k]; ok {
		return false
	}
	d.seen[k] = struct{}{}
	return true
}

// fingerprint = sha256(Method + "\n" + 规范化 URL)。
// BuildURL 里 Query.Encode() 按键名排序输出，
// 天然消除 ?a=1&b=2 与 ?b=2&a=1 的顺序差异（同一资源只抓一次）。
// 未纳入指纹：Header（UA 变化不应导致重抓）、Body
// （POST 场景同 URL 不同 body 会被误判重复，届时再演进指纹定义）。
func fingerprint(r *Request) [32]byte {
	return sha256.Sum256([]byte(r.Method + "\n" + r.BuildURL()))
}
