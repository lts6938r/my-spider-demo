package middleware

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"my-spider-demo/spider"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// SaveResponseMiddleware 把每个成功下载的响应原文写入本地文件。
//
// 定位：保存原文是横切关注点——Pipeline 只见 Item（提取后的结构化数据），
// 看不到 Response；中间件在响应阶段天然拿得到完整 body。
// 建议放在 Retry 外层：只保存最终成功的那份响应，
// 重试途中的 429/5xx 中间产物不落盘。
//
// 语义：
//   - 保存失败只记 Warn 日志、不影响抓取（辅助能力不应拖死主流程）
//   - 成功后把文件路径写入 r.Meta["saved_path"]，解析函数可带上 Item
//   - 文件名 = URL 末段（净化）+ URL 哈希前 4 字节（防冲突），
//     扩展名按 Content-Type 推断（html/xml/json，未知为 bin）
func SaveResponseMiddleware(dir string, lg *slog.Logger) Middleware {
	if lg == nil {
		lg = slog.Default()
	}
	return func(next Handle) Handle {
		return func(r *spider.Request) (*spider.Response, error) {
			resp, err := next(r)
			if err == nil && resp != nil {
				saved, serr := saveResponse(dir, r, resp)
				if serr != nil {
					lg.Warn("save response failed", "url", r.BuildURL(), "err", serr)
				} else {
					if r.Meta == nil {
						r.Meta = make(map[string]any)
					}
					r.Meta["saved_path"] = saved
					lg.Info("response saved",
						"url", r.BuildURL(), "file", saved, "bytes", len(resp.Body))
				}
			}
			return resp, err
		}
	}
}

var unsafeNameRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func saveResponse(dir string, r *spider.Request, resp *spider.Response) (string, error) {
	// 构造函数没有 error 返回位，目录惰性创建；
	// 并发安全：不同 URL 文件名不同，且去重保证一个 URL 只下载一次
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("spider: mkdir %s: %w", dir, err)
	}

	sum := sha256.Sum256([]byte(r.BuildURL()))
	name := baseNameFromURL(r.URL)
	if len(name) > 80 {
		name = name[:80]
	}
	name = fmt.Sprintf("%s-%x.%s", name, sum[:4], extOf(resp.Header.Get("Content-Type")))

	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, resp.Body, 0o644); err != nil {
		return "", fmt.Errorf("spider: write %s: %w", p, err)
	}
	return p, nil
}

// baseNameFromURL 取 URL 路径末段作为可读文件名主体，非法字符替换为 _。
func baseNameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "index"
	}
	seg := path.Base(u.Path)
	if seg == "" || seg == "." || seg == "/" {
		return "index"
	}
	return unsafeNameRe.ReplaceAllString(seg, "_")
}

func extOf(contentType string) string {
	ct := strings.ToLower(contentType)
	switch {
	case strings.Contains(ct, "html"):
		return "html"
	case strings.Contains(ct, "xml"), strings.Contains(ct, "rss"), strings.Contains(ct, "atom"):
		return "xml"
	case strings.Contains(ct, "json"):
		return "json"
	default:
		return "bin"
	}
}
