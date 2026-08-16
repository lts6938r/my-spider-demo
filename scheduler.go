package spider

import (
	"container/heap"
	"context"
	"sync"
)

// Scheduler 是任务调度的模块边界：Engine 只依赖这个接口，
// FIFO 实现未来可被优先级队列（V4）替换而不动引擎代码。
type Scheduler interface {
	// Push 将请求加入队列。V1 队列无界、必然成功；
	// 并发版/有界版可能演进为返回 error 或 (bool)。
	Push(*Request)

	// Pop 取出下一个请求，队列空时立即返回 false。
	// ctx 是为 V3 并发版（阻塞等待 + graceful shutdown）预留的契约，
	// 当前实现不阻塞也不取消。
	Pop(ctx context.Context) (*Request, bool)

	// Len 返回当前排队请求数，供测试断言与观测使用。
	Len() int
}

// FIFOScheduler 是基于 slice + mutex 的先入先出实现。
type FIFOScheduler struct {
	mu   sync.Mutex
	q    []*Request
	head int // 队头下标：出队只移动下标，不做整体搬移
}

func NewFIFOScheduler() *FIFOScheduler {
	return &FIFOScheduler{}
}

func (s *FIFOScheduler) Push(r *Request) {
	if r == nil {
		return
	}
	s.mu.Lock()
	s.q = append(s.q, r)
	s.mu.Unlock()
}

func (s *FIFOScheduler) Pop(ctx context.Context) (*Request, bool) {
	_ = ctx // 并发版才会用到：阻塞等待或响应取消
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.head >= len(s.q) {
		return nil, false
	}
	r := s.q[s.head]
	s.q[s.head] = nil // 清引用，让已出队的 Request 可被 GC
	s.head++
	// head 只前进会让底层数组永不收缩（内存只增不减），
	// 越过一半容量时搬移一次，均摊 O(1)。
	if s.head > 64 && s.head*2 >= cap(s.q) {
		n := copy(s.q, s.q[s.head:])
		s.q = s.q[:n]
		s.head = 0
	}
	return r, true
}

func (s *FIFOScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.q) - s.head
}

// PriorityQueueScheduler 是基于 container/heap 的优先级调度实现，
// 实现同一个 Scheduler 接口——引擎代码零改动即可替换（V1 接口预留的兑现）。
//
// 语义：Priority 数值大的先出队；同优先级按入队顺序 FIFO。
// heap 算法本身不稳定（相同元素出队顺序不保证），
// 用单调递增的序号打破平局，使行为完全确定——
// 因此优先级调度是 FIFO 调度的严格超集。
type PriorityQueueScheduler struct {
	mu  sync.Mutex
	h   reqHeap
	seq int64
}

func NewPriorityQueueScheduler() *PriorityQueueScheduler {
	return &PriorityQueueScheduler{}
}

func (s *PriorityQueueScheduler) Push(r *Request) {
	if r == nil {
		return
	}
	s.mu.Lock()
	s.seq++
	heap.Push(&s.h, pqEntry{req: r, seq: s.seq})
	s.mu.Unlock()
}

func (s *PriorityQueueScheduler) Pop(ctx context.Context) (*Request, bool) {
	_ = ctx // 与 FIFOScheduler 相同：非阻塞语义，契约见接口注释
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h.Len() == 0 {
		return nil, false
	}
	return heap.Pop(&s.h).(pqEntry).req, true
}

func (s *PriorityQueueScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.h.Len()
}

// pqEntry 给请求附加入队序号，用于同优先级的 FIFO 平局裁决。
type pqEntry struct {
	req *Request
	seq int64
}

type reqHeap []pqEntry

func (h reqHeap) Len() int { return len(h) }
func (h reqHeap) Less(i, j int) bool {
	pi, pj := h[i].req.Priority, h[j].req.Priority
	if pi != pj {
		return pi > pj // 大值优先
	}
	return h[i].seq < h[j].seq // 同优先级先入队先出
}
func (h reqHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *reqHeap) Push(x any)   { *h = append(*h, x.(pqEntry)) }
func (h *reqHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}
