package spider

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipelineFuncAdapter(t *testing.T) {
	var got Item
	fn := PipelineFunc(func(it Item) error {
		got = it
		return nil
	})
	if err := fn.ProcessItem(Item{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if got["a"] != 1 {
		t.Errorf("adapter 未透传 item, got %v", got)
	}
}

func TestConsolePipelineOutput(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w

	err = ConsolePipeline{}.ProcessItem(Item{"k": "v", "n": 1})

	w.Close()
	os.Stdout = old
	if err != nil {
		t.Fatalf("ProcessItem: %v", err)
	}

	out, _ := io.ReadAll(r)
	var got Item
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("输出不是合法 JSON: %v, got %q", err, out)
	}
	if got["k"] != "v" || got["n"] != float64(1) {
		t.Errorf("roundtrip = %v, want k=v n=1", got)
	}
}

func TestJSONLFilePipeline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	p := NewJSONLFilePipeline(path)

	if err := p.ProcessItem(Item{"id": 1}); err != nil {
		t.Fatal(err)
	}
	if err := p.ProcessItem(Item{"id": 2}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Error("Close 应幂等")
	}
	if err := p.ProcessItem(Item{"id": 3}); err == nil {
		t.Error("Close 后 ProcessItem 应报错")
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 {
		t.Fatalf("文件 %d 行, want 2（Close 后的写入不应落盘）", len(lines))
	}
	var it Item
	if err := json.Unmarshal([]byte(lines[0]), &it); err != nil {
		t.Fatalf("首行不是合法 JSON: %v", err)
	}
	if it["id"] != float64(1) {
		t.Errorf("首行 = %v, want id=1", it)
	}
}

func TestJSONLFilePipelineAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")

	p1 := NewJSONLFilePipeline(path)
	p1.ProcessItem(Item{"run": 1})
	p1.Close()

	p2 := NewJSONLFilePipeline(path)
	p2.ProcessItem(Item{"run": 2})
	p2.Close()

	b, _ := os.ReadFile(path)
	if n := len(strings.Split(strings.TrimSpace(string(b)), "\n")); n != 2 {
		t.Errorf("追加模式下行数 = %d, want 2（不覆盖历史）", n)
	}
}
