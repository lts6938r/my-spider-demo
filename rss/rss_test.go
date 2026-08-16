package rss

import (
	"testing"

	"my-spider-demo/spider"
)

const sampleRSS2 = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>Demo Feed</title>
<item>
  <title><![CDATA[A & B]]></title>
  <link>  https://example.com/a  </link>
  <pubDate>Mon, 01 Jan 2026 00:00:00 GMT</pubDate>
</item>
<item>
  <title>Second</title>
  <link>https://example.com/b</link>
</item>
<item>
  <title>无链接条目应被跳过</title>
</item>
</channel></rss>`

const sampleAtom = `<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<title>Atom Feed</title>
<entry>
  <title>Entry One</title>
  <link rel="self" href="https://example.com/x?fmt=atom"/>
  <link rel="alternate" href="https://example.com/x"/>
  <published>2026-01-02T00:00:00Z</published>
</entry>
<entry>
  <title>Entry Two</title>
  <link href="https://example.com/y"/>
</entry>
</feed>`

func TestParseRSS2(t *testing.T) {
	items, err := ParseRSS(&spider.Response{Body: []byte(sampleRSS2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("条目数 = %d, want 2（无链接条目跳过）", len(items))
	}
	if items[0].Link != "https://example.com/a" {
		t.Errorf("Link = %q, want 去空白", items[0].Link)
	}
	if items[0].Title != "A & B" {
		t.Errorf("Title = %q, want CDATA 解包", items[0].Title)
	}
	if items[0].Published == "" {
		t.Error("Published 未解析")
	}
}

func TestParseAtom(t *testing.T) {
	items, err := ParseRSS(&spider.Response{Body: []byte(sampleAtom)})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("条目数 = %d, want 2", len(items))
	}
	if items[0].Link != "https://example.com/x" {
		t.Errorf("Link = %q, want alternate 而非 self", items[0].Link)
	}
	if items[1].Link != "https://example.com/y" {
		t.Errorf("无 rel 的链接应视为 alternate, got %q", items[1].Link)
	}
}

func TestParseRSSErrors(t *testing.T) {
	if _, err := ParseRSS(&spider.Response{Body: []byte("<html><body>not a feed</body></html>")}); err == nil {
		t.Error("HTML 内容应报错而非静默")
	}
}

// 真实世界抓出来的两个兼容性问题（Slashdot）：
// RSS 1.0 (RDF) 的 item 与 channel 平级；声明编码为 ISO-8859-1。
// 样本用 \xE9 构造真实 Latin-1 字节（而非 UTF-8 的 é）。
const sampleRDF = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
	"<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" +
	"         xmlns=\"http://purl.org/rss/1.0/\">\n" +
	"<channel rdf:about=\"https://slashdot.org/\"><title>Slashdot</title></channel>\n" +
	"<item rdf:about=\"https://slashdot.org/story/1\"><title>Caf\xE9 launch\xE9</title><link>https://slashdot.org/story/1</link><dc:date>2026-08-16T06:32:25+00:00</dc:date></item>\n" +
	"<item rdf:about=\"https://slashdot.org/story/2\"><title>Plain</title><link>https://slashdot.org/story/2</link></item>\n" +
	"</rdf:RDF>"

func TestParseRDFLatin1(t *testing.T) {
	items, err := ParseRSS(&spider.Response{Body: []byte(sampleRDF)})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("条目数 = %d, want 2（RDF 平级 item）", len(items))
	}
	if items[0].Title != "Café launché" {
		t.Errorf("Title = %q, want Latin-1 正确转为 UTF-8", items[0].Title)
	}
	if items[0].Link != "https://slashdot.org/story/1" {
		t.Errorf("Link = %q", items[0].Link)
	}
	if items[0].Published != "2026-08-16T06:32:25+00:00" {
		t.Errorf("Published = %q, want 取 Dublin Core 的 dc:date", items[0].Published)
	}
}
