package spider

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLitePipeline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	p, err := NewSQLitePipeline(path, "items")
	if err != nil {
		t.Fatal(err)
	}

	for _, it := range []Item{
		{"author": "Einstein", "text": "imagination"},
		{"author": "Tesla", "text": "future"},
	} {
		if err := p.ProcessItem(it); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Error("Close 应幂等")
	}
	if err := p.ProcessItem(Item{"x": 1}); err == nil {
		t.Error("Close 后 ProcessItem 应报错")
	}

	// 重新打开验证持久化与 json_extract 查询
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("行数 = %d, want 2", n)
	}

	rows, err := db.Query(`SELECT json_extract(data, '$.author') FROM items ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		got = append(got, a)
	}
	if len(got) != 2 || got[0] != "Einstein" || got[1] != "Tesla" {
		t.Errorf("json_extract 查询 = %v", got)
	}
}

func TestSQLitePipelineCustomTableAndRejectsBadName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	p, err := NewSQLitePipeline(path, "quotes")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ProcessItem(Item{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := NewSQLitePipeline(path, "drop table x"); err == nil {
		t.Error("非法表名应被白名单拦截")
	}
}

func TestEngineWithSQLitePipeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "e2e.db")
	sqlp, err := NewSQLitePipeline(path, "")
	if err != nil {
		t.Fatal(err)
	}

	start, _ := NewRequest("", srv.URL, jsonParse)
	eng := &Engine{
		Sched:   NewFIFOScheduler(),
		Down:    NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Pipes:   []Pipeline{sqlp},
		Logger:  testLogger,
	}
	if err := eng.Run(spiderStub{"sql", []*Request{start}}); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("items 表行数 = %d, want 1（引擎退出前应已写入并正常关闭）", n)
	}
}
