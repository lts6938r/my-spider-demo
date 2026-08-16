package spider

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestHashSetDuperFingerprint(t *testing.T) {
	d := NewHashSetDuper()

	r1, _ := NewRequest("", "https://example.com/p?a=1&b=2", nil)
	r2, _ := NewRequest("", "https://example.com/p?b=2&a=1", nil) // query 顺序不同
	r3, _ := NewRequest("", "https://example.com/q?a=1&b=2", nil) // path 不同
	r4, _ := NewRequest("POST", "https://example.com/p?a=1&b=2", nil)

	if !d.Visit(r1) {
		t.Error("首次访问应返回 true")
	}
	if d.Visit(r2) {
		t.Error("query 顺序不同的同一 URL 应判为重复")
	}
	if !d.Visit(r3) {
		t.Error("不同 path 不应判重")
	}
	if !d.Visit(r4) {
		t.Error("不同 method 不应判重")
	}
}

func TestHashSetDuperConcurrent(t *testing.T) {
	d := NewHashSetDuper()
	var firsts atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := NewRequest("", "https://example.com/same", nil)
			if err != nil {
				t.Error(err)
				return
			}
			if d.Visit(r) {
				firsts.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := firsts.Load(); n != 1 {
		t.Errorf("50 个并发相同请求，first = %d, want 1（检查与记录必须同临界区）", n)
	}
}
