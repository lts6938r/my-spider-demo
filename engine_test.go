package spider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type spiderStub struct {
	name string
	reqs []*Request
}

func (s spiderStub) Name() string              { return s.name }
func (s spiderStub) StartRequests() []*Request { return s.reqs }

// 测试用页面格式：{"links":["http://.../p2"],"items":[{"id":1}]}
type testPage struct {
	Links []string `json:"links"`
	Items []struct {
		ID int `json:"id"`
	} `json:"items"`
}

// jsonParse 是多页面型解析的缩影：产 item + 追加链接请求，
// 并演示 Meta 沿请求链传递。
func jsonParse(resp *Response) Result {
	var p testPage
	if err := json.Unmarshal(resp.Body, &p); err != nil {
		return Result{Err: fmt.Errorf("decode %s: %w", resp.Request.URL, err)}
	}

	res := Result{}
	for _, it := range p.Items {
		res.Items = append(res.Items, Item{"id": it.ID, "page": resp.Request.Meta["page"]})
	}
	for _, link := range p.Links {
		nr, err := NewRequest("", link, jsonParse)
		if err != nil {
			return Result{Err: err}
		}
		nr.Meta = map[string]any{"page": 2}
		res.Requests = append(res.Requests, nr)
	}
	return res
}

// captureEngine 组装一个用捕获管道收 item 的引擎。
// Workers=1：保证既有测试的顺序断言确定性；
// 并发行为由 TestEngineConcurrent* 系列专门验证。
func captureEngine(duper Duper, mws ...Middleware) (*Engine, *[]Item) {
	got := &[]Item{}
	return &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(5 * time.Second),
		Mws:     mws,
		Pipes:   []Pipeline{PipelineFunc(func(it Item) error { *got = append(*got, it); return nil })},
		Duper:   duper,
		Logger:  testLogger,
		Workers: 1,
	}, got
}

func TestEngineEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/p1":
			fmt.Fprintf(w, `{"links":["http://%s/p2"],"items":[{"id":1},{"id":2}]}`, r.Host)
		case "/p2":
			fmt.Fprint(w, `{"items":[{"id":3}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	start, err := NewRequest("", srv.URL+"/p1", jsonParse)
	if err != nil {
		t.Fatal(err)
	}
	start.Meta = map[string]any{"page": 1}

	eng, got := captureEngine(nil)
	if err := eng.Run(spiderStub{"demo", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(*got) != 3 {
		t.Fatalf("捕获 %d 个 item, want 3: %v", len(*got), *got)
	}
	wantID := []int{1, 2, 3}
	wantPage := []int{1, 1, 2}
	for i, it := range *got {
		if it["id"] != wantID[i] {
			t.Errorf("item[%d].id = %v, want %d", i, it["id"], wantID[i])
		}
		if it["page"] != wantPage[i] {
			t.Errorf("item[%d].page = %v, want %d（Meta 传递）", i, it["page"], wantPage[i])
		}
	}
	if eng.Downloaded != 2 || eng.Parsed != 2 || eng.Failed != 0 || eng.Dropped != 0 || eng.Dup != 0 {
		t.Errorf("统计 = downloaded:%d failed:%d parsed:%d dropped:%d dup:%d, want 2/0/2/0/0",
			eng.Downloaded, eng.Failed, eng.Parsed, eng.Dropped, eng.Dup)
	}
	if start.ID != 1 {
		t.Errorf("起始请求 ID = %d, want 1（引擎顺序分配）", start.ID)
	}
}

func TestEngineParseErrorDropsResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `ok`)
	}))
	defer srv.Close()

	boom := func(*Response) Result {
		// 模拟"页面结构不符合预期"：带着半成品 item 报错
		return Result{Items: []Item{{"bad": true}}, Err: errors.New("unexpected layout")}
	}
	start, _ := NewRequest("", srv.URL, boom)

	eng, got := captureEngine(nil)
	if err := eng.Run(spiderStub{"boom", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(*got) != 0 {
		t.Errorf("Err 置位的 Result 应整体丢弃, got %v", *got)
	}
	if eng.Parsed != 1 {
		t.Errorf("Parsed = %d, want 1", eng.Parsed)
	}
}

func TestEngineDownloadFailureContinues(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":7}]}`)
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	bad.Close() // 制造连接拒绝

	goodReq, _ := NewRequest("", good.URL, jsonParse)
	badReq, _ := NewRequest("", bad.URL, jsonParse)
	eng, got := captureEngine(nil)
	if err := eng.Run(spiderStub{"mixed", []*Request{badReq, goodReq}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if eng.Failed != 1 || eng.Downloaded != 1 {
		t.Errorf("Failed=%d Downloaded=%d, want 1/1", eng.Failed, eng.Downloaded)
	}
	if len(*got) != 1 || (*got)[0]["id"] != 7 {
		t.Errorf("单点失败不应中断整个抓取, got %v", *got)
	}
}

func TestEnginePipelineStopsOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	}))
	defer srv.Close()

	reject := PipelineFunc(func(Item) error { return errors.New("invalid") })
	var secondSaw int
	count := PipelineFunc(func(Item) error { secondSaw++; return nil })

	start, _ := NewRequest("", srv.URL, jsonParse)
	eng, _ := captureEngine(nil)
	eng.Pipes = []Pipeline{reject, count}
	if err := eng.Run(spiderStub{"pipe", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if secondSaw != 0 {
		t.Error("链上第一个 error 后，后续 Pipeline 不应再看到该 item")
	}
	if eng.Dropped != 1 {
		t.Errorf("Dropped = %d, want 1", eng.Dropped)
	}
}

func TestEngineDedup(t *testing.T) {
	var p2 atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		// 同一链接出现两次（列表页重复引用很常见）
		fmt.Fprintf(w, `{"links":["http://%s/p2","http://%s/p2"]}`, r.Host, r.Host)
	})
	mux.HandleFunc("/p2", func(w http.ResponseWriter, r *http.Request) {
		p2.Add(1)
		fmt.Fprint(w, `{"items":[{"id":9}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	start, _ := NewRequest("", srv.URL+"/start", jsonParse)
	eng, got := captureEngine(NewHashSetDuper())
	if err := eng.Run(spiderStub{"dedup", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if p2.Load() != 1 {
		t.Errorf("/p2 实际下载 %d 次, want 1（去重）", p2.Load())
	}
	if eng.Dup != 1 {
		t.Errorf("Dup = %d, want 1", eng.Dup)
	}
	if eng.Downloaded != 2 {
		t.Errorf("Downloaded = %d, want 2（start + 一次 p2）", eng.Downloaded)
	}
	if len(*got) != 1 || (*got)[0]["id"] != 9 {
		t.Errorf("item = %v", *got)
	}
}

func TestEngineWithRetryMiddleware(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"items":[{"id":5}]}`)
	}))
	defer srv.Close()

	start, _ := NewRequest("", srv.URL, jsonParse)
	cfg := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
	eng, got := captureEngine(nil, RetryMiddleware(cfg, testLogger))
	if err := eng.Run(spiderStub{"retry", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(*got) != 1 || (*got)[0]["id"] != 5 {
		t.Fatalf("重试恢复后应拿到 item, got %v", *got)
	}
	// 两次 503 由中间件内部消化：引擎只看到最终成功的那次响应
	if eng.Failed != 0 || eng.Downloaded != 1 {
		t.Errorf("Failed=%d Downloaded=%d, want 0/1（重试对引擎透明）", eng.Failed, eng.Downloaded)
	}
}

// ---- V3：并发与可靠性 ----

func TestEngineConcurrentDownloads(t *testing.T) {
	var cur, maxC atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := cur.Add(1)
		for {
			m := maxC.Load()
			if c <= m || maxC.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		cur.Add(-1)
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	var reqs []*Request
	for i := 0; i < 8; i++ {
		r, err := NewRequest("", fmt.Sprintf("%s/%d", srv.URL, i), nil)
		if err != nil {
			t.Fatal(err)
		}
		reqs = append(reqs, r)
	}

	eng := &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(5 * time.Second),
		Workers: 4,
		Logger:  testLogger,
	}
	start := time.Now()
	if err := eng.Run(spiderStub{"concurrent", reqs}); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if eng.Downloaded != 8 || eng.Failed != 0 {
		t.Errorf("Downloaded=%d Failed=%d, want 8/0", eng.Downloaded, eng.Failed)
	}
	if m := maxC.Load(); m < 2 {
		t.Errorf("最大并发 = %d, want >= 2（没有真正并发）", m)
	} else if m > 4 {
		t.Errorf("最大并发 = %d, want <= Workers=4（池没有限住并发）", m)
	}
	// 8 × 150ms 任务：串行 1.2s，4 worker 理论 ~300ms
	if elapsed > 700*time.Millisecond {
		t.Errorf("耗时 %v，并发加速未生效", elapsed)
	}
}

func TestEngineConcurrentDedup(t *testing.T) {
	var same atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		links := make([]string, 20)
		for i := range links {
			links[i] = fmt.Sprintf("http://%s/same", r.Host)
		}
		b, _ := json.Marshal(map[string][]string{"links": links})
		_, _ = w.Write(b)
	})
	mux.HandleFunc("/same", func(w http.ResponseWriter, r *http.Request) {
		same.Add(1)
		fmt.Fprint(w, `{"items":[{"id":9}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	start, _ := NewRequest("", srv.URL+"/start", jsonParse)
	eng := &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(5 * time.Second),
		Workers: 8,
		Duper:   NewHashSetDuper(),
		Logger:  testLogger,
		Pipes:   []Pipeline{PipelineFunc(func(Item) error { return nil })},
	}
	if err := eng.Run(spiderStub{"dedup2", []*Request{start}}); err != nil {
		t.Fatal(err)
	}

	if n := same.Load(); n != 1 {
		t.Errorf("/same 下载 %d 次, want 1（并发下检查+记录必须同临界区）", n)
	}
	if eng.Dup != 19 {
		t.Errorf("Dup = %d, want 19", eng.Dup)
	}
	if eng.Downloaded != 2 {
		t.Errorf("Downloaded = %d, want 2", eng.Downloaded)
	}
}

func TestEngineGracefulShutdown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // 客户端取消时立刻结束（模拟在途请求被中止）
		case <-time.After(2 * time.Second):
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	var reqs []*Request
	for i := 0; i < 6; i++ {
		r, err := NewRequest("", fmt.Sprintf("%s/%d", srv.URL, i), nil)
		if err != nil {
			t.Fatal(err)
		}
		reqs = append(reqs, r)
	}

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)

	eng := &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(10 * time.Second),
		Workers: 4,
		Logger:  testLogger,
	}
	start := time.Now()
	err := eng.RunContext(ctx, spiderStub{"shutdown", reqs})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("停机耗时 %v：在途下载应被取消，而非等完 2s", elapsed)
	}
	// RunContext 返回前 wg.Wait 已完成，worker 应全部退出。
	// 轮询等待残余的服务端 handler goroutine 收尾。
	for i := 0; i < 100; i++ {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("疑似 goroutine 泄漏: before=%d now=%d", before, runtime.NumGoroutine())
}

func TestRunIsUncancellableEquivalent(t *testing.T) {
	// Run = RunContext(Background)：完整跑完不报错。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	}))
	defer srv.Close()

	start, _ := NewRequest("", srv.URL, jsonParse)
	eng, got := captureEngine(nil)
	if err := eng.Run(spiderStub{"plain", []*Request{start}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(*got) != 1 {
		t.Errorf("got %v", *got)
	}
}

// ---- V4：优先级调度 + 文件管道 ----

func TestEnginePriorityScheduling(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mux := http.NewServeMux()
	mux.HandleFunc("/low", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "low")
		mu.Unlock()
	})
	mux.HandleFunc("/high", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "high")
		mu.Unlock()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	low, _ := NewRequest("", srv.URL+"/low", nil)
	high, _ := NewRequest("", srv.URL+"/high", nil)
	high.Priority = 10

	eng := &Engine{
		Sched:   NewPriorityQueueScheduler(),
		Down:    NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Logger:  testLogger,
	}
	if err := eng.Run(spiderStub{"prio", []*Request{low, high}}); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "high" || order[1] != "low" {
		t.Errorf("下载顺序 = %v, want [high low]（高优先级先行）", order)
	}
}

func TestEngineShutdownPersistsCompletedItems(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		fmt.Fprint(w, `{"items":[{"id":2}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "out.jsonl")
	fast, _ := NewRequest("", srv.URL+"/fast", jsonParse)
	slow, _ := NewRequest("", srv.URL+"/slow", jsonParse)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)

	eng := &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(10 * time.Second),
		Workers: 2,
		Pipes:   []Pipeline{NewJSONLFilePipeline(path)},
		Logger:  testLogger,
	}
	if err := eng.RunContext(ctx, spiderStub{"persist", []*Request{fast, slow}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// 停机语义：已完成的 item 必须已落盘且文件正常关闭（可读出）
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读结果文件: %v（管道未正常关闭?）", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("落盘 %d 行, want 1（fast 已完成、slow 被取消）: %q", len(lines), b)
	}
	var it Item
	if err := json.Unmarshal([]byte(lines[0]), &it); err != nil || it["id"] != float64(1) {
		t.Errorf("落盘内容 = %s", lines[0])
	}
}
