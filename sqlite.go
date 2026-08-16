package spider

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
)

// SQLitePipeline 把 Item 持久化到 SQLite（modernc.org/sqlite，纯 Go 实现，
// 无需 cgo——本机没有 gcc 也能编译；代价是比 cgo 版略慢，对爬虫写入量足够）。
//
// Schema 取舍：Item 是无 schema 的 map，采用「一行一 item 的 JSON 文档」
// 而非固定列表——固定列会把业务 schema 强加给通用框架。
// 查询用 SQLite 的 json_extract：
//
//	SELECT json_extract(data, '$.author') FROM items;
//
// 实现 io.Closer，由引擎在退出时统一关闭（同 JSONLFilePipeline）。
type SQLitePipeline struct {
	db     *sql.DB
	insert *sql.Stmt
	table  string

	mu     sync.Mutex
	closed bool
}

// 表名只允许字母数字下划线：表名不能用占位符参数，
// 必须拼进 SQL，白名单校验杜绝注入。
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func NewSQLitePipeline(path, table string) (*SQLitePipeline, error) {
	if table == "" {
		table = "items"
	}
	if !identRe.MatchString(table) {
		return nil, fmt.Errorf("spider: 非法表名 %q（仅允许字母数字下划线）", table)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("spider: open sqlite %s: %w", path, err)
	}
	// WAL：写入不阻塞读取，崩溃安全性也更好
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		db.Close()
		return nil, fmt.Errorf("spider: sqlite pragma: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			data       TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`, table)); err != nil {
		db.Close()
		return nil, fmt.Errorf("spider: create table %s: %w", table, err)
	}
	insert, err := db.Prepare(fmt.Sprintf(
		`INSERT INTO %s (data) VALUES (?)`, table))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("spider: prepare insert: %w", err)
	}
	return &SQLitePipeline{db: db, insert: insert, table: table}, nil
}

func (p *SQLitePipeline) ProcessItem(item Item) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("spider: sqlite pipeline %s already closed", p.table)
	}
	b, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("sqlite pipeline: %w", err)
	}
	if _, err := p.insert.Exec(string(b)); err != nil {
		return fmt.Errorf("sqlite pipeline: insert: %w", err)
	}
	return nil
}

// Close 关闭语句与连接，幂等。
func (p *SQLitePipeline) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if err := p.insert.Close(); err != nil {
		p.db.Close()
		return err
	}
	return p.db.Close()
}
