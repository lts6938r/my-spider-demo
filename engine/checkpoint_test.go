package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"my-spider-demo/dedup"
	"my-spider-demo/downloader"
	"my-spider-demo/pipeline"
	"my-spider-demo/scheduler"
	"my-spider-demo/spider"
)

// ---- 断点续爬 e2e：闸门服务器精确控制"哪些请求已完成" ----

// cpSpider：列表页 + 文章页共用 jsonParse（{links}/{items} 格式），
// 并实现 ResumeResolver——恢复的 URL 一律重挂 jsonParse。
type cpSpider struct {
	base string
}

func (cpSpider) Name() string { return "checkpoint-e2e" }

func (s cpSpider) StartRequests() []*spider.Request {
	r, err := spider.NewRequest("", s.base+"/start", jsonParse)
	if err != nil {
		panic(err)
	}
	return []*spider.Request{r}
}

func (cpSpider) ParseFor(string) spider.ParseFunc { return jsonParse }

// gateServer：/start 返回 a1..a4 链接；每个 /aN 在闸门放行前挂起。
// 只统计"放行后完成响应"的次数（served）——被取消的尝试不计。
type gateServer struct {
	mu     sync.Mutex
	served map[string]int
	gates  []chan struct{}
	opened []bool
}

func newGateServer(t *testing.T, n int) (*gateServer, *httptest.Server) {
	t.Helper()
	g := &gateServer{
		served: make(map[string]int),
		gates:  make([]chan struct{}, n+1),
		opened: make([]bool, n+1),
	}
	for i := 1; i <= n; i++ {
		g.gates[i] = make(chan struct{})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.served["start"]++
		g.mu.Unlock()
		links := ""
		for i := 1; i <= n; i++ {
			links += fmt.Sprintf(`"http://%s/a%d",`, r.Host, i)
		}
		fmt.Fprintf(w, `{"links":[%s]}`, links[:len(links)-1])
	})
	for i := 1; i <= n; i++ {
		mux.HandleFunc(fmt.Sprintf("/a%d", i), func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-g.gates[i]:
			case <-r.Context().Done():
				return // 客户端取消：不计 served
			}
			g.mu.Lock()
			g.served[fmt.Sprintf("a%d", i)]++
			g.mu.Unlock()
			fmt.Fprintf(w, `{"items":[{"id":%d}]}`, i)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		g.openAll()
		srv.Close()
	})
	return g, srv
}

func (g *gateServer) open(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.opened[i] {
		g.opened[i] = true
		close(g.gates[i])
	}
}

func (g *gateServer) openAll() {
	for i := 1; i < len(g.gates); i++ {
		g.open(i)
	}
}

func (g *gateServer) count(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.served[key]
}

func TestCheckpointResumeE2E(t *testing.T) {
	g, srv := newGateServer(t, 4)
	cpPath := filepath.Join(t.TempDir(), "cp.json")

	// ---- 第一轮：只放行 a1，拿到第一个 item 后中断 ----
	items := make(chan spider.Item, 100)
	eng1 := &Engine{
		Sched:   scheduler.NewFIFOScheduler(),
		Down:    downloader.NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Duper:   dedup.NewCheckpoint(cpPath),
		Pipes:   []pipeline.Pipeline{pipeline.PipelineFunc(func(it spider.Item) error { items <- it; return nil })},
		Logger:  testLogger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	g.open(1) // 只允许 a1 完成
	go func() { done <- eng1.RunContext(ctx, cpSpider{srv.URL}) }()

	// 等 a1 的 item（Complete 发生在 item 之前，见 handle 顺序）
	select {
	case it := <-items:
		if it["id"] != 1 {
			t.Fatalf("第一个 item 应为 a1, got %v", it)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待首个 item 超时")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("第一轮应返回 context.Canceled, got %v", err)
	}

	// 断点文件应存在，且 a2..a4 未完成
	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("中断后断点文件应存在: %v", err)
	}
	if c := g.count("a1"); c != 1 {
		t.Errorf("a1 完成次数 = %d, want 1", c)
	}
	if c := g.count("a4"); c != 0 {
		t.Errorf("a4 完成次数 = %d, want 0（未到）", c)
	}

	// ---- 第二轮：放行全部，断点续爬 ----
	g.open(2)
	g.open(3)
	g.open(4)
	var got2 []spider.Item
	eng2 := &Engine{
		Sched:   scheduler.NewFIFOScheduler(),
		Down:    downloader.NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Duper:   dedup.NewCheckpoint(cpPath),
		Pipes:   []pipeline.Pipeline{pipeline.PipelineFunc(func(it spider.Item) error { got2 = append(got2, it); return nil })},
		Logger:  testLogger,
	}
	if err := eng2.RunContext(context.Background(), cpSpider{srv.URL}); err != nil {
		t.Fatalf("第二轮应完整完成: %v", err)
	}

	// 核心断言：已完成的 start 和 a1 不重抓（服务端只 served 一次）
	if c := g.count("start"); c != 1 {
		t.Errorf("start 服务次数 = %d, want 1（visited 不重抓）", c)
	}
	if c := g.count("a1"); c != 1 {
		t.Errorf("a1 服务次数 = %d, want 1（visited 不重抓）", c)
	}
	for i := 2; i <= 4; i++ {
		if c := g.count(fmt.Sprintf("a%d", i)); c != 1 {
			t.Errorf("a%d 服务次数 = %d, want 1（pending 续爬）", i, c)
		}
	}
	// 第二轮只下载 pending 的 3 个；start/a1 被 visited 跳过
	if eng2.Downloaded != 3 {
		t.Errorf("第二轮 Downloaded = %d, want 3", eng2.Downloaded)
	}
	// 两轮 item 合计恰为 1..4，无重复
	ids := map[int]bool{1: true}
	for _, it := range got2 {
		ids[it["id"].(int)] = true
	}
	if len(ids) != 4 {
		t.Errorf("两轮 item 合计 = %v, want 1..4 无重复", ids)
	}
	// 正常完成后断点清除
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Error("完整结束后断点文件应被清除")
	}
}

// 未实现 ResumeResolver 的 Spider：恢复的请求只下载、不解析。
func TestCheckpointResumeWithoutResolver(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	}))
	defer srv.Close()

	// 手工构造断点：一个指向上面服务的 pending URL，一个别的起始请求
	cpPath := filepath.Join(t.TempDir(), "cp.json")
	cp := dedup.NewCheckpoint(cpPath)
	resumed, _ := spider.NewRequest("", srv.URL+"/resumed", nil)
	cp.Visit(resumed)
	if err := cp.Save(); err != nil {
		t.Fatal(err)
	}

	other, _ := spider.NewRequest("", srv.URL+"/other", jsonParse)
	var got []spider.Item
	eng := &Engine{
		Sched:   scheduler.NewFIFOScheduler(),
		Down:    downloader.NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Duper:   dedup.NewCheckpoint(cpPath),
		Pipes:   []pipeline.Pipeline{pipeline.PipelineFunc(func(it spider.Item) error { got = append(got, it); return nil })},
		Logger:  testLogger,
	}
	// spiderStub 未实现 ResumeResolver
	if err := eng.Run(spiderStub{"no-resolver", []*spider.Request{other}}); err != nil {
		t.Fatal(err)
	}

	if eng.Downloaded != 2 {
		t.Errorf("Downloaded = %d, want 2（恢复的 + 起始的）", eng.Downloaded)
	}
	if len(got) != 1 {
		t.Errorf("item = %v, want 仅起始请求产出 1 个（恢复请求无 Parse）", got)
	}
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Error("完成后断点应清除")
	}
}
