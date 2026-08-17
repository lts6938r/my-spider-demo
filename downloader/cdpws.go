package downloader

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"
)

// cdpws 是 CDP 专用的最小 WebSocket 客户端（RFC 6455 子集）：
//   - 仅文本帧；客户端发送的帧按规范强制掩码
//   - 处理服务端分片（continuation）、ping（自动回 pong）、close
//   - 不协商 permessage-deflate：握手不携带扩展头，Chrome 即不压缩，
//     帧头 RSV 位非零即报错
//
// 手写而不引入 websocket 库：CDP 只需要这一个用例，
// 且握手（HTTP Upgrade）与帧格式是理解应用层协议的好素材（design-log 021）。
type wsConn struct {
	conn net.Conn
	br   *bufio.Reader

	wmu sync.Mutex // 写互斥：数据帧与 pong 可能并发发送
}

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsDial 完成 HTTP Upgrade 握手并返回可收发文本消息的连接。
func wsDial(rawURL string, timeout time.Duration) (*wsConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("cdpws: url: %w", err)
	}
	host := u.Host
	if u.Port() == "" {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return nil, fmt.Errorf("cdpws: dial: %w", err)
	}

	// 握手期间的读写整体限时；会话期由上层用 Close 中断阻塞读
	conn.SetDeadline(time.Now().Add(timeout))
	defer conn.SetDeadline(time.Time{})

	var kb [16]byte
	if _, err := rand.Read(kb[:]); err != nil {
		conn.Close()
		return nil, fmt.Errorf("cdpws: rand: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(kb[:])

	req := "GET " + u.RequestURI() + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("cdpws: write handshake: %w", err)
	}

	br := bufio.NewReader(conn)
	tp := textproto.NewReader(br)
	statusLine, err := tp.ReadLine()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("cdpws: read status: %w", err)
	}
	// "HTTP/1.1 101 Switching Protocols"
	fields := strings.Fields(statusLine)
	if len(fields) < 2 || fields[1] != "101" {
		conn.Close()
		return nil, fmt.Errorf("cdpws: 握手被拒绝: %s", statusLine)
	}
	hdr, err := tp.ReadMIMEHeader()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("cdpws: read headers: %w", err)
	}
	if !strings.EqualFold(hdr.Get("Upgrade"), "websocket") {
		conn.Close()
		return nil, errors.New("cdpws: 响应不含 Upgrade: websocket")
	}
	// 校验 Accept：SHA1(key + GUID) 的 base64，防止连到非 ws 服务
	sum := sha1.Sum([]byte(key + wsGUID))
	if hdr.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		conn.Close()
		return nil, errors.New("cdpws: Sec-WebSocket-Accept 校验失败")
	}
	return &wsConn{conn: conn, br: br}, nil
}

// SendText 发送一条（单帧、掩码的）文本消息。CDP 请求都是小 JSON，无需分片。
func (c *wsConn) SendText(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	var hdr [14]byte
	hdr[0] = 0x81 // FIN + text
	n := len(data)
	hlen := 2
	switch {
	case n < 126:
		hdr[1] = 0x80 | byte(n) // MASK + 长度
	case n <= 0xFFFF:
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		hlen = 4
	default:
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		hlen = 10
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return fmt.Errorf("cdpws: mask rand: %w", err)
	}
	copy(hdr[hlen:hlen+4], mask[:])

	buf := make([]byte, 0, hlen+4+n)
	buf = append(buf, hdr[:hlen]...)
	buf = append(buf, mask[:]...)
	buf = append(buf, data...)
	// 掩码：payload 与按位循环的 mask 异或
	off := len(buf) - n
	for i := 0; i < n; i++ {
		buf[off+i] ^= mask[i%4]
	}
	_, err := c.conn.Write(buf)
	return err
}

// ReadMessage 阻塞读取一条完整消息（自动拼接分片、应答 ping）。
// 连接关闭（对端 close 帧或底层错误）返回错误。
func (c *wsConn) ReadMessage() ([]byte, error) {
	var msg []byte
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 0x9: // ping → pong
			if err := c.sendPong(payload); err != nil {
				return nil, err
			}
		case 0xA: // pong，忽略
		case 0x8: // close
			return nil, io.EOF
		case 0x0: // continuation
			msg = append(msg, payload...)
			if fin {
				return msg, nil
			}
		case 0x1, 0x2: // text / binary
			msg = append(msg[:0], payload...)
			if fin {
				return msg, nil
			}
		default:
			return nil, fmt.Errorf("cdpws: 未知帧类型 %d", opcode)
		}
	}
}

func (c *wsConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	if h[0]&0x70 != 0 {
		err = errors.New("cdpws: RSV 位非零（压缩未协商）")
		return
	}
	fin = h[0]&0x80 != 0
	opcode = h[0] & 0x0F
	if h[1]&0x80 != 0 {
		err = errors.New("cdpws: 服务端帧不应掩码")
		return
	}
	plen := uint64(h[1] & 0x7F)
	switch plen {
	case 126:
		var e [2]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		plen = uint64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		if _, err = io.ReadFull(c.br, e[:]); err != nil {
			return
		}
		plen = binary.BigEndian.Uint64(e[:])
	}
	// 上限防御：CDP 消息不会超过 64MB
	if plen > 64<<20 {
		err = fmt.Errorf("cdpws: 帧过大 %d", plen)
		return
	}
	payload = make([]byte, plen)
	_, err = io.ReadFull(c.br, payload)
	return
}

func (c *wsConn) sendPong(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	hdr := []byte{0x8A, byte(0x80 | len(data))} // FIN + pong + MASK
	var mask [4]byte
	rand.Read(mask[:])
	buf := append(hdr, mask[:]...)
	base := len(buf)
	buf = append(buf, data...)
	for i := range data {
		buf[base+i] ^= mask[i%4]
	}
	_, err := c.conn.Write(buf)
	return err
}

// Close 发送 close 帧并关闭底层连接（幂等容忍）。
// 阻塞中的 ReadMessage 会因连接关闭而返回错误——这是会话层的中断机制。
func (c *wsConn) Close() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.conn.Write([]byte{0x88, 0x80, 0, 0, 0, 0}) // close 帧，尽力而为
	return c.conn.Close()
}
