package rss

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"my-spider-demo/spider"
	"strings"
)

// RSSItem 是从 RSS 2.0 / RSS 1.0(RDF) / Atom 提取的一条条目。
type RSSItem struct {
	Title     string
	Link      string
	Published string
}

// —— RSS 2.0 / RSS 1.0 (RDF) 结构（未知元素自动忽略，CDATA 自动解包）——
//
// 两者的条目结构相同，差异在位置：RSS 2.0 的 item 嵌套在 channel 内，
// RDF 的 item 与 channel 平级——两个位置都收，即同时支持两种格式。
type rss2Feed struct {
	Channel rss2Channel `xml:"channel"`
	Items   []rss2Item  `xml:"item"` // RDF：与 channel 平级的条目
}

type rss2Channel struct {
	Title string     `xml:"title"`
	Items []rss2Item `xml:"item"` // RSS 2.0：channel 内的条目
}

type rss2Item struct {
	Title   string `xml:"title"`
	Link    string `xml:"link"`
	PubDate string `xml:"pubDate"` // RSS 2.0
	DCDate  string `xml:"date"`    // RDF 常用 Dublin Core 的 <dc:date>（局部名匹配）
}

// —— Atom 结构（链接是属性，且区分 rel）——

type atomFeed struct {
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title     string     `xml:"title"`
	Links     []atomLink `xml:"link"`
	Published string     `xml:"published"`
}

type atomLink struct {
	Rel  string `xml:"rel,attr"`
	Href string `xml:"href,attr"`
}

// ParseRSS 从 Response 提取 RSS 2.0 / RSS 1.0(RDF) / Atom 的条目列表，自动识别格式。
// 只做格式级解析——把链接变成请求去抓取是 Spider 的业务决策，
// 不属于框架（与 JSON 解析同级的定位）。
// 返回 error 的情况：内容不是上述任何一种格式（如 HTML 页面），
// 错误信息携带两种格式各自的解析失败原因，便于定位。
func ParseRSS(resp *spider.Response) ([]RSSItem, error) {
	// RSS 2.0 / RDF 共用一套结构
	var r2 rss2Feed
	errRSS := newXMLDecoder(resp.Body).Decode(&r2)
	if errRSS == nil {
		all := append(r2.Channel.Items, r2.Items...)
		items := make([]RSSItem, 0, len(all))
		for _, it := range all {
			link := strings.TrimSpace(it.Link)
			if link == "" {
				continue // 无链接的条目无法产生抓取任务
			}
			items = append(items, RSSItem{
				Title: strings.TrimSpace(it.Title),
				Link:  link,
				Published: firstNonEmpty(
					strings.TrimSpace(it.PubDate),
					strings.TrimSpace(it.DCDate)),
			})
		}
		if len(items) > 0 {
			return items, nil
		}
	}

	// 回退按 Atom 解析：链接取 rel 为空或 alternate 的（人类可读页面），
	// 跳过 rel=self 等机器链接
	var af atomFeed
	errAtom := newXMLDecoder(resp.Body).Decode(&af)
	if errAtom == nil {
		var items []RSSItem
		for _, e := range af.Entries {
			link := ""
			for _, l := range e.Links {
				if l.Rel == "" || l.Rel == "alternate" {
					link = strings.TrimSpace(l.Href)
					break
				}
			}
			if link == "" {
				continue
			}
			items = append(items, RSSItem{
				Title:     strings.TrimSpace(e.Title),
				Link:      link,
				Published: strings.TrimSpace(e.Published),
			})
		}
		if len(items) > 0 {
			return items, nil
		}
	}

	return nil, fmt.Errorf("spider: 无法识别的 RSS/Atom 内容（%d 字节）: rss=%v atom=%v",
		len(resp.Body), errRSS, errAtom)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// newXMLDecoder 构造带字符集转换的 XML 解码器。
// Go 的 encoding/xml 只接受 UTF-8，CharsetReader 是官方预留的转换钩子。
// 老站点常见 ISO-8859-1（Slashdot 即是）：Latin-1 的字节值就是 Unicode 码点，
// 按字节映射即可，无需第三方依赖。
func newXMLDecoder(body []byte) *xml.Decoder {
	dec := xml.NewDecoder(bytes.NewReader(body))
	dec.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
		switch strings.ToLower(charset) {
		case "", "utf-8":
			return input, nil
		case "iso-8859-1", "latin1", "windows-1252":
			// 注：cp1252 在 0x80-0x9F 与 latin1 不同（弯引号等），
			// 此处按声明的 Latin-1 语义处理，对标题类文本差异极小
			b, err := io.ReadAll(input)
			if err != nil {
				return nil, err
			}
			out := make([]rune, len(b))
			for i, c := range b {
				out[i] = rune(c)
			}
			return strings.NewReader(string(out)), nil
		default:
			return nil, fmt.Errorf("spider: 不支持的字符集 %q", charset)
		}
	}
	return dec
}
