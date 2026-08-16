package engine

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"my-spider-demo/downloader"
	"my-spider-demo/pipeline"
	"my-spider-demo/scheduler"
	"my-spider-demo/spider"
)

func TestEngineWithSQLitePipeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[{"id":1}]}`)
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "e2e.db")
	sqlp, err := pipeline.NewSQLitePipeline(path, "")
	if err != nil {
		t.Fatal(err)
	}

	start, _ := spider.NewRequest("", srv.URL, jsonParse)
	eng := &Engine{
		Sched:   scheduler.NewFIFOScheduler(),
		Down:    downloader.NewHTTPDownloader(5 * time.Second),
		Workers: 1,
		Pipes:   []pipeline.Pipeline{sqlp},
		Logger:  testLogger,
	}
	if err := eng.Run(spiderStub{"sql", []*spider.Request{start}}); err != nil {
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
