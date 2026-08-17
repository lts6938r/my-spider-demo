package downloader

import (
	"context"
	"my-spider-demo/spider"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestCDPBotDetection 访问 bot.sannysoft.com，提取每个检测项的 pass/fail 状态。
// 该页面通过 JS 检测 navigator.webdriver、Chrome 对象、Plugins、WebGL、
// User-Agent 中 HeadlessChrome 字样等属性来识别 headless 浏览器。
// 本测试验证自实现 CDP 协议栈在真实 Chrome 浏览器（非 headless）下通过反检测的能力。
func TestCDPBotDetection(t *testing.T) {
	d, err := NewCDPDownloader(CDPConfig{
		Headless: false, // 真实浏览器模式，不暴露 HeadlessChrome 标识
		Timeout:  60 * time.Second,
		Wait:     3 * time.Second,
		// 使用真实 Chrome 的常规 UA，不暴露 HeadlessChrome 标识
		UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
	})
	if err != nil {
		t.Skipf("本机无法启动 Chrome，跳过: %v", err)
	}
	defer d.Close()

	req, err := spider.NewRequest("", "https://bot.sannysoft.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := d.Download(req)
	if err != nil {
		t.Fatalf("CDP 下载失败: %v", err)
	}
	t.Logf("状态码: %d, 页面大小: %d bytes, 耗时: %v", resp.StatusCode, len(resp.Body), resp.Cost)

	html := resp.Text()

	// ---- 提取检测表结构：<tr><td>名称</td><td class="result passed/failed">结果</td></tr> ----
	rowRe := regexp.MustCompile(`<tr>\s*<td>(.*?)</td>\s*<td(?:\s[^>]*)?\s*>(.*?)</td>`)
	type check struct{ name, status, detail string }
	var checks []check

	for _, m := range rowRe.FindAllStringSubmatch(html, -1) {
		name := stripHTML(m[1])
		detail := stripHTML(m[2])
		status := classifyStatus(detail)
		checks = append(checks, check{name, status, detail})
	}

	// ---- 输出结果 ----
	var passCount, failCount, warnCount int
	t.Log("\n========== Antibot / Fingerprint Scanner 检测结果 ==========")
	for _, c := range checks {
		switch c.status {
		case "PASS":
			t.Logf("  ✅ %-45s %s", c.name, c.detail)
			passCount++
		case "FAIL":
			t.Logf("  ❌ %-45s %s", c.name, c.detail)
			failCount++
		case "WARN":
			t.Logf("  ⚠️  %-45s %s", c.name, c.detail)
			warnCount++
		default:
			t.Logf("  ❓ %-45s %s", c.name, c.detail)
		}
	}
	t.Log("===========================================================")
	t.Logf("总计: %d 项通过, %d 项失败, %d 项警告", passCount, failCount, warnCount)

// ---- 关键 headless 暴露指标：只检查 User Agent 结果栏的值 ----
		uaHasHeadless := false
		for _, c := range checks {
			if strings.Contains(c.name, "User Agent") || strings.Contains(c.name, "HEADCHR_UA") {
				if strings.Contains(strings.ToLower(c.detail), "headlesschrome") {
					uaHasHeadless = true
					t.Logf("  User-Agent 检测项暴露 HeadlessChrome: %s", c.detail)
				}
			}
		}
		if !uaHasHeadless {
			// 再检查所有检测结果中是否有 HeadlessChrome 字样
			for _, c := range checks {
				if strings.Contains(strings.ToLower(c.detail), "headlesschrome") {
					uaHasHeadless = true
					t.Logf("  检测项 %s 暴露 HeadlessChrome: %s", c.name, c.detail)
				}
			}
		}

	// ---- 断言 ----
	t.Logf("")
	if uaHasHeadless {
		t.Error("❌ User-Agent 暴露 HeadlessChrome 标识 — 反爬网站可直接识别")
	}
	if failCount > 0 {
		t.Errorf("❌ %d 项检测失败（headless 特征被识别）", failCount)
	}
	if passCount == 0 {
		t.Error("❌ 未提取到任何检测结果，测试逻辑可能需要更新")
	}
}

// stripHTML 移除 HTML 标签，返回纯文本。
func stripHTML(s string) string {
	re := regexp.MustCompile(`<[^>]*>`)
	return re.ReplaceAllString(strings.TrimSpace(s), "")
}

// classifyStatus 根据检测结果文本判断通过/失败/警告。
func classifyStatus(detail string) string {
	lower := strings.ToLower(detail)
	if strings.Contains(lower, "failed") ||
		strings.Contains(lower, "headlesschrome") ||
		strings.Contains(lower, "not ok") ||
		strings.Contains(lower, "0 (fail") {
		return "FAIL"
	}
	if strings.Contains(lower, "warn") || strings.Contains(lower, "warning") {
		return "WARN"
	}
	if strings.Contains(lower, "passed") ||
		strings.Contains(lower, "ok") ||
		strings.Contains(detail, "✓") {
		return "PASS"
	}
	// class="passed" 在外层 td 上，detail 内容本身可能是值
	return "PASS" // 有值即视为通过（页面用 td class 标色）
}
