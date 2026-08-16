package dedup

import (
	"os"
	"path/filepath"
	"testing"

	"my-spider-demo/spider"
)

func cpReq(method, url string) *spider.Request {
	r, err := spider.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	return r
}

func TestCheckpointRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	c := NewCheckpoint(path)

	// 三个请求入队，一个完成
	if !c.Visit(cpReq("", "https://example.com/a")) {
		t.Fatal("首次 Visit 应返回 true")
	}
	if c.Visit(cpReq("", "https://example.com/a")) {
		t.Fatal("pending 中的请求应判重")
	}
	if !c.Visit(cpReq("", "https://example.com/b")) {
		t.Fatal("b 首次应 true")
	}
	if !c.Visit(cpReq("", "https://example.com/c")) {
		t.Fatal("c 首次应 true")
	}
	c.Complete(cpReq("", "https://example.com/a")) // a 完成

	if err := c.Save(); err != nil {
		t.Fatal(err)
	}

	// 新实例从文件恢复
	c2 := NewCheckpoint(path)
	pending, err := c2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("恢复的 pending = %v, want b/c 两个", pending)
	}
	// 已完成的 a 在恢复后仍判重；b/c 可重新入队
	if c2.Visit(cpReq("", "https://example.com/a")) {
		t.Error("恢复后 visited 中的 a 应判重（不重抓）")
	}
	if !c2.Visit(cpReq("", "https://example.com/b")) {
		t.Error("恢复后 b 应可重新入队")
	}
}

func TestCheckpointLoadMissing(t *testing.T) {
	c := NewCheckpoint(filepath.Join(t.TempDir(), "nope.json"))
	urls, err := c.Load()
	if err != nil {
		t.Fatalf("文件不存在不是错误: %v", err)
	}
	if len(urls) != 0 {
		t.Errorf("无断点应返回空, got %v", urls)
	}
}

func TestCheckpointClear(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	c := NewCheckpoint(path)
	c.Visit(cpReq("", "https://example.com/a"))
	c.Save()

	if _, err := os.Stat(path); err != nil {
		t.Fatal("断点文件应存在")
	}
	if err := c.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("Clear 后文件应被删除")
	}
	if err := c.Clear(); err != nil {
		t.Error("Clear 应幂等")
	}
}

func TestCheckpointCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	os.WriteFile(path, []byte("{not json"), 0o644)
	if _, err := NewCheckpoint(path).Load(); err == nil {
		t.Error("损坏的断点文件应报错而非静默")
	}
}
