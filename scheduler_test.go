package spider

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// mkReq 构造带唯一 URL 的请求（测试辅助，出错直接 panic：
// t.Fatal 不允许在非测试 goroutine 中调用）。
func mkReq(id int) *Request {
	r, err := NewRequest("", fmt.Sprintf("https://example.com/item/%d", id), nil)
	if err != nil {
		panic(err)
	}
	return r
}

func TestFIFOSchedulerOrderAndEmpty(t *testing.T) {
	s := NewFIFOScheduler()
	for i := 0; i < 3; i++ {
		s.Push(mkReq(i))
	}
	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}
	for i := 0; i < 3; i++ {
		r, ok := s.Pop(context.Background())
		if !ok {
			t.Fatalf("pop %d: want ok", i)
		}
		if want := fmt.Sprintf("https://example.com/item/%d", i); r.URL != want {
			t.Errorf("pop %d = %s, want %s（FIFO 顺序）", i, r.URL, want)
		}
	}
	if _, ok := s.Pop(context.Background()); ok {
		t.Error("空队列 Pop 应返回 false")
	}
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
}

// 并发 Push/Pop：用 -race 验证互斥保护，验证总量无丢失。
func TestFIFOSchedulerConcurrent(t *testing.T) {
	s := NewFIFOScheduler()
	const producers, perProducer = 4, 250

	done := make(chan struct{})
	var prodWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for i := 0; i < perProducer; i++ {
				s.Push(mkReq(p*perProducer + i))
			}
		}(p)
	}
	go func() {
		prodWG.Wait()
		close(done)
	}()

	var mu sync.Mutex
	seen := make(map[string]bool)
	var consWG sync.WaitGroup
	consWG.Add(1)
	go func() {
		defer consWG.Done()
		for {
			r, ok := s.Pop(context.Background())
			if ok {
				mu.Lock()
				seen[r.URL] = true
				mu.Unlock()
				continue
			}
			// 队列暂时为空：只有生产者全部结束且仍为空才算消费完毕。
			select {
			case <-done:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	consWG.Wait()

	if want := producers * perProducer; len(seen) != want {
		t.Errorf("消费到 %d 个不同请求, want %d（请求丢失）", len(seen), want)
	}
}

func prioReq(id, prio int) *Request {
	r := mkReq(id)
	r.Priority = prio
	return r
}

func TestPrioritySchedulerOrder(t *testing.T) {
	s := NewPriorityQueueScheduler()
	// 入队顺序：A(0) B(10) C(5) D(10)
	for _, r := range []*Request{prioReq(1, 0), prioReq(2, 10), prioReq(3, 5), prioReq(4, 10)} {
		s.Push(r)
	}
	if s.Len() != 4 {
		t.Fatalf("Len = %d, want 4", s.Len())
	}

	// 期望出队：B(10,先入) D(10,后入) C(5) A(0)
	want := []int{2, 4, 3, 1}
	for i, id := range want {
		r, ok := s.Pop(context.Background())
		if !ok {
			t.Fatalf("pop %d: want ok", i)
		}
		if got := fmt.Sprintf("https://example.com/item/%d", id); r.URL != got {
			t.Errorf("pop %d = %s, want item/%d", i, r.URL, id)
		}
	}
	if _, ok := s.Pop(context.Background()); ok {
		t.Error("空队列 Pop 应返回 false")
	}
}

func TestPrioritySchedulerUpgradesFIFO(t *testing.T) {
	// 全部同优先级（默认 0）时，行为必须与 FIFO 完全一致
	s := NewPriorityQueueScheduler()
	for i := 0; i < 5; i++ {
		s.Push(mkReq(i))
	}
	for i := 0; i < 5; i++ {
		r, ok := s.Pop(context.Background())
		if !ok || r.URL != fmt.Sprintf("https://example.com/item/%d", i) {
			t.Errorf("pop %d = %+v ok=%v, want item/%d（同优先级 FIFO）", i, r, ok, i)
		}
	}
}
