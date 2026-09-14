package yandex

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

// =====================================================================
// OFSP v1 (OpenFlux Stream Protocol) — thin envelope over raw IP packets
// that gives us reliable multi-stream delivery:
//
//   [0]      magic   0xFF       (never a valid IP version nibble)
//   [1]      version 0x01
//   [2]      flags   bit0=needs-ACK, bit1=is-ACK,
//                    bit2=critical, bit3=keep-alive
//   [3..10]  seq     uint64 big-endian
//   [11..]   payload (raw IP packet, or empty for ACK/keep-alive)
//
// Sender assigns a seq to each outbound packet, stores it in a pending
// map, and retransmits if no ACK arrives before RTO. Receiver deduplicates
// by seq and sends back ACK envelopes. Critical TCP packets (SYN/FIN/RST)
// are additionally sent to 2 distinct alive sessions for redundancy.
// =====================================================================

const (
	envMagic   byte = 0xFF
	envVersion byte = 0x01
	envHdrLen       = 11 // magic + version + flags + seq(8)

	flagNeedsACK  byte = 0x01
	flagIsACK     byte = 0x02
	flagCritical  byte = 0x04
	flagKeepAlive byte = 0x08

	maxRetransmitAttempts = 8
	initialRTO            = 300 * time.Millisecond
	maxRTO                = 3 * time.Second
	retransmitTick        = 100 * time.Millisecond
	dedupTTL              = 30 * time.Second
	dedupCleanupTick      = 10 * time.Second
)

type YandexDocsInfo struct {
	CookieStr   string
	Token       string
	DocID       string
	CallbackURL string
	UserID      string
	Origin      string
	Host        string
	WsURL       string
	Permissions map[string]interface{}
	OpenCmd     map[string]interface{}
}

type DocSession struct {
	Info                 YandexDocsInfo
	Conn                 *websocket.Conn
	WriteQueue           chan []byte
	UserID               string
	writeMu              sync.Mutex
	Alive                atomic.Bool
	LastDisconnectReason atomic.Int32
}

func (s *DocSession) safeWrite(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.Conn.WriteMessage(messageType, data)
}

// pendingEntry tracks an outbound packet waiting for its ACK.
type pendingEntry struct {
	envelope []byte
	sentAt   time.Time
	attempts int
	critical bool
	mu       sync.Mutex
}

// dedupEntry records when a seq was first seen inbound.
type dedupEntry struct {
	seenAt time.Time
}

type YandexDocsTransport struct {
	*transport.BaseTransport

	urls        []string
	sessions    []*DocSession
	sessionMu   sync.RWMutex
	rrCounter   atomic.Uint64
	userCounter atomic.Int32
	baseUserID  string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	// OFSP: seq + pending + inbound dedup
	seqCounter   atomic.Uint64
	pending      sync.Map // uint64 -> *pendingEntry (outbound, awaiting ACK)
	receivedSeqs sync.Map // uint64 -> *dedupEntry  (inbound, dedup)
}

func NewYandexDocsTransport(urls []string, config transport.TransportConfig) *YandexDocsTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		urls:          urls,
		sessions:      make([]*DocSession, len(urls)),
		ctx:           ctx,
		cancel:        cancel,
	}
	t.baseUserID = randUserID()
	return t
}

func (t *YandexDocsTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	t.baseUserID = randUserID()

	// Keep-alive loop
	t.wg.Add(1)
	utils.SafeGo("yandex.keepAlive", func() {
		defer t.wg.Done()
		t.keepAliveLoop()
	})

	// OFSP: retransmit unacked packets
	t.wg.Add(1)
	utils.SafeGo("yandex.retransmit", func() {
		defer t.wg.Done()
		t.retransmitLoop()
	})

	// OFSP: evict stale dedup entries
	t.wg.Add(1)
	utils.SafeGo("yandex.dedupCleaner", func() {
		defer t.wg.Done()
		t.dedupCleanupLoop()
	})

	// One connection per document
	for i, docUrl := range t.urls {
		t.wg.Add(1)
		idx := i
		url := docUrl
		utils.SafeGo(fmt.Sprintf("yandex.connect[%d]", idx), func() {
			defer t.wg.Done()
			t.connectToDocForIndex(idx, url, 0)
		})
	}

	return nil
}

func (t *YandexDocsTransport) Stop() error {
	t.cancel()
	t.wg.Wait()
	return t.BaseTransport.Stop()
}

// ---------------------------------------------------------------------
// Send — public entry point.
//
// If `data` is a raw IP packet, it gets wrapped in an OFSP envelope,
// registered in the pending map, and routed to an alive session.
// If `data` is already an envelope (ACK / retransmit / requeue from
// writerLoop), it's routed as-is without re-wrapping.
//
// Critical TCP packets (SYN/FIN/RST) are sent to 2 distinct sessions.
// ---------------------------------------------------------------------
func (t *YandexDocsTransport) Send(data []byte) error {
	if !t.IsConnected() {
		t.RecordError()
		return fmt.Errorf("transport not connected")
	}

	// Already wrapped (ACK, retransmit, requeue): route as-is.
	if isEnvelope(data) {
		return t.sendEnvelope(data)
	}

	// Wrap raw IP packet.
	critical := isCriticalPacket(data)
	env := t.wrapPacket(data, critical)

	seq := binary.BigEndian.Uint64(env[3:11])
	t.pending.Store(seq, &pendingEntry{
		envelope: env,
		sentAt:   time.Now(),
		attempts: 0,
		critical: critical,
	})

	return t.sendEnvelope(env)
}

// sendEnvelope routes an already-wrapped envelope to one (or two, for
// critical) alive session's WriteQueue. Non-blocking: returns error
// immediately if every queue is full.
func (t *YandexDocsTransport) sendEnvelope(env []byte) error {
	t.sessionMu.RLock()
	sessions := t.sessions
	t.sessionMu.RUnlock()

	n := len(sessions)
	if n == 0 {
		t.RecordError()
		return fmt.Errorf("no sessions")
	}

	var flags byte
	if len(env) > 2 {
		flags = env[2]
	}
	critical := flags&flagCritical != 0
	isACK := flags&flagIsACK != 0

	needed := 1
	if critical {
		needed = 2
	}
	if n < needed {
		needed = n
	}

	sent := 0
	start := int(t.rrCounter.Add(1)) % n
	for i := 0; i < n && sent < needed; i++ {
		idx := (start + i) % n
		sess := sessions[idx]
		if sess == nil || sess.Conn == nil || !sess.Alive.Load() {
			continue
		}
		select {
		case sess.WriteQueue <- env:
			t.RecordSend(len(env))
			sent++
		default:
			// queue full, try next
		}
	}

	if sent == 0 {
		t.RecordError()
		return fmt.Errorf("all write queues full")
	}
	if critical && sent < 2 && !isACK {
		utils.Debugf("[YDOCS] critical packet partial redundancy: %d/2", sent)
	}
	return nil
}

func isEnvelope(data []byte) bool {
	return len(data) >= envHdrLen && data[0] == envMagic && data[1] == envVersion
}

// wrapPacket creates an OFSP envelope around a raw IP packet.
func (t *YandexDocsTransport) wrapPacket(data []byte, critical bool) []byte {
	seq := t.seqCounter.Add(1)
	flags := flagNeedsACK
	if critical {
		flags |= flagCritical
	}
	env := make([]byte, envHdrLen+len(data))
	env[0] = envMagic
	env[1] = envVersion
	env[2] = flags
	binary.BigEndian.PutUint64(env[3:11], seq)
	copy(env[envHdrLen:], data)
	return env
}

// wrapACK builds an ACK envelope for the given inbound sequence number.
func (t *YandexDocsTransport) wrapACK(seq uint64) []byte {
	env := make([]byte, envHdrLen)
	env[0] = envMagic
	env[1] = envVersion
	env[2] = flagIsACK
	binary.BigEndian.PutUint64(env[3:11], seq)
	return env
}

// wrapKeepAlive builds a keep-alive envelope (no payload, needs-ACK).
func (t *YandexDocsTransport) wrapKeepAlive() []byte {
	env := make([]byte, envHdrLen)
	env[0] = envMagic
	env[1] = envVersion
	env[2] = flagNeedsACK | flagKeepAlive
	binary.BigEndian.PutUint64(env[3:11], t.seqCounter.Add(1))
	return env
}

// isCriticalPacket returns true iff `data` is an IPv4 or IPv6 packet
// whose TCP segment has SYN, FIN, or RST set. These are sent redundantly.
func isCriticalPacket(data []byte) bool {
	if len(data) < 20 {
		return false
	}
	version := data[0] >> 4
	var proto byte
	var payloadStart int

	switch version {
	case 4:
		ihl := int(data[0]&0x0F) * 4
		if ihl < 20 || len(data) < ihl {
			return false
		}
		proto = data[9]
		payloadStart = ihl
	case 6:
		if len(data) < 40 {
			return false
		}
		nextHeader := data[6]
		payloadStart = 40
	walkV6:
		for {
			switch nextHeader {
			case 0, 43, 44, 60: // hop-by-hop, routing, fragment, dst opts
				if len(data) < payloadStart+8 {
					return false
				}
				nextHeader = data[payloadStart]
				extLen := int(data[payloadStart+1])*8 + 8
				if extLen < 8 {
					return false
				}
				payloadStart += extLen
			case 6, 17, 1, 58: // TCP, UDP, ICMP, ICMPv6
				proto = nextHeader
				break walkV6
			default:
				break walkV6
			}
		}
	default:
		return false
	}

	if proto != 6 { // TCP
		return false
	}
	if len(data) < payloadStart+14 {
		return false
	}
	const fin, syn, rst = 0x01, 0x02, 0x04
	return data[payloadStart+13]&(fin|syn|rst) != 0
}

func (t *YandexDocsTransport) connectToDocForIndex(idx int, url string, attempt int) {
	if !t.IsRunning() {
		return
	}

	utils.Debugf("[YDOCS] connectToDocForIndex[%d] attempt %d ...", idx, attempt)

	defer func() {
		if r := recover(); r != nil {
			utils.Debugf("[PANIC] recovered in yandex.connect[%d]: %v", idx, r)
		}
	}()

	select {
	case <-t.ctx.Done():
		return
	default:
	}

	t.sessionMu.RLock()
	existingSession := t.sessions[idx]
	t.sessionMu.RUnlock()

	var userID string
	if existingSession != nil {
		userID = existingSession.UserID
	} else {
		suffix := fmt.Sprintf("%03d-%02d", t.userCounter.Add(1)%1000, idx)
		userID = t.baseUserID + suffix
	}

	info, err := t.fetchDocInfo(url, userID)
	if err != nil {
		utils.Debugf("[YDOCS][%d] fetchDocInfo failed: %v", idx, err)
		t.scheduleReconnectForIndex(idx, url, attempt)
		return
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		NetDialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	headers.Set("Origin", info.Origin)
	headers.Set("Cookie", info.CookieStr)
	headers.Set("Host", info.Host)

	utils.Debugf("[YDOCS][%d] WebSocket dial %s", idx, info.WsURL)
	conn, resp, err := dialer.Dial(info.WsURL, headers)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		utils.Debugf("[YDOCS][%d] WebSocket dial failed (http %d): %v", idx, status, err)
		t.scheduleReconnectForIndex(idx, url, attempt)
		return
	}
	utils.Debugf("[YDOCS][%d] WebSocket connected to %s", idx, info.Host)

	writeQueue := make(chan []byte, t.GetConfig().MaxQueueSize)
	if existingSession != nil {
		writeQueue = existingSession.WriteQueue
	}

	session := &DocSession{
		Info:       info,
		Conn:       conn,
		WriteQueue: writeQueue,
		UserID:     userID,
	}
	session.Alive.Store(true)

	t.sessionMu.Lock()
	t.sessions[idx] = session
	t.updateConnectedStatus()
	t.sessionMu.Unlock()

	// Writer loop persists across reconnects (started only once per idx).
	if existingSession == nil {
		utils.SafeGo(fmt.Sprintf("yandex.writer[%d]", idx), func() {
			t.writerLoopForIndex(idx)
		})
	}

	// Auth
	auth1 := fmt.Sprintf(`40{"token":"%s"}`, info.Token)
	session.safeWrite(websocket.TextMessage, []byte(auth1))

	authData := map[string]interface{}{
		"type": "auth", "docid": info.DocID, "token": "fghhfgsjdgfjs",
		"user": map[string]interface{}{"id": userID}, "editorType": 0,
		"lastOtherSaveTime": -1, "permissions": info.Permissions,
		"openCmd": info.OpenCmd, "coEditingMode": "fast", "jwtOpen": info.Token,
	}
	messagePart, _ := json.Marshal([]interface{}{"message", authData})
	session.safeWrite(websocket.TextMessage, []byte(fmt.Sprintf("42%s", string(messagePart))))

	connectedAt := time.Now()
	for t.IsRunning() {
		select {
		case <-t.ctx.Done():
			session.Alive.Store(false)
			conn.Close()
			return
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			utils.Debugf("[YDOCS][%d] Read error: %v", idx, err)
			session.Alive.Store(false)
			t.sessionMu.Lock()
			t.updateConnectedStatus()
			t.sessionMu.Unlock()
			t.RecordError()

			// Phase 1: reroute anything still queued for this dead session.
			go t.drainDeadSession(idx)

			next := attempt
			if time.Since(connectedAt) > 15*time.Second {
				next = -1
			}
			t.scheduleReconnectForIndex(idx, url, next)
			return
		}
		t.handleMessage(session, message)
	}
}

// updateConnectedStatus sets connected flag based on alive sessions.
// Must be called with sessionMu held.
func (t *YandexDocsTransport) updateConnectedStatus() {
	aliveCount := 0
	total := len(t.sessions)
	for _, s := range t.sessions {
		if s != nil && s.Alive.Load() {
			aliveCount++
		}
	}
	t.SetConnected(aliveCount > 0)
	utils.Debugf("[YDOCS] alive sessions: %d/%d", aliveCount, total)
}

// drainDeadSession pulls any still-queued packets from a session that
// just died and reroutes them via sendEnvelope to an alive session.
// Prevents silent loss of packets that were queued but not yet written.
func (t *YandexDocsTransport) drainDeadSession(deadIdx int) {
	t.sessionMu.RLock()
	deadSess := t.sessions[deadIdx]
	t.sessionMu.RUnlock()

	if deadSess == nil {
		return
	}

	drained := 0
	for {
		select {
		case packet := <-deadSess.WriteQueue:
			time.Sleep(2 * time.Millisecond) // let other sessions settle
			if err := t.sendEnvelope(packet); err != nil {
				utils.Debugf("[YDOCS] drain[%d]: reroute failed: %v", deadIdx, err)
			}
			drained++
			if drained > 10000 {
				break
			}
		default:
			if drained > 0 {
				utils.Debugf("[YDOCS] drained %d packets from dead session %d", drained, deadIdx)
			}
			return
		}
	}
}

func (t *YandexDocsTransport) writerLoopForIndex(idx int) {
	for t.IsRunning() {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		t.sessionMu.RLock()
		session := t.sessions[idx]
		t.sessionMu.RUnlock()

		if session == nil || session.Conn == nil || !session.Alive.Load() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		select {
		case packet := <-session.WriteQueue:
			payload := base64.StdEncoding.EncodeToString(packet)
			msg := fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, payload)

			start := time.Now()
			if err := session.safeWrite(websocket.TextMessage, []byte(msg)); err != nil {
				utils.Debugf("[YDOCS][%d] Write error: %v", idx, err)
				session.Alive.Store(false)
				t.sessionMu.Lock()
				t.updateConnectedStatus()
				t.sessionMu.Unlock()
				t.RecordError()

				// Phase 1: the packet we just pulled from the queue was
				// NOT sent — reroute it to another alive session via
				// sendEnvelope (which detects already-wrapped envelope
				// and skips re-wrapping).
				go func(p []byte) {
					time.Sleep(5 * time.Millisecond)
					if err := t.sendEnvelope(p); err != nil {
						utils.Debugf("[YDOCS][%d] requeue failed: %v", idx, err)
					}
				}(packet)

				// And drain any further queued packets for this dead session.
				go t.drainDeadSession(idx)
			} else {
				rtt := time.Since(start)
				if rtt > 500*time.Millisecond {
					utils.Debugf("[YDOCS][%d] Slow write: %v", idx, rtt)
				}
			}
		case <-t.ctx.Done():
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (t *YandexDocsTransport) keepAliveLoop() {
	ticker := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.sessionMu.RLock()
			sessions := t.sessions
			t.sessionMu.RUnlock()

			for _, session := range sessions {
				if session != nil && session.Conn != nil && session.Alive.Load() {
					// OFSP keep-alive: an envelope with no payload. The
					// receiver ACKs it, giving us free per-session RTT
					// visibility for future weighted Round-Robin.
					select {
					case session.WriteQueue <- t.wrapKeepAlive():
					default:
					}
				}
			}
		}
	}
}

// ---------------------------------------------------------------------
// OFSP: retransmit loop
// ---------------------------------------------------------------------

// retransmitLoop scans the pending map every 100ms and retransmits any
// packet that hasn't been ACKed within its (exponentially growing) RTO.
// After maxRetransmitAttempts (8) the packet is dropped and counted as
// an error — the upper-layer TCP stack will recover from the loss.
func (t *YandexDocsTransport) retransmitLoop() {
	ticker := time.NewTicker(retransmitTick)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case now := <-ticker.C:
			t.pending.Range(func(key, value interface{}) bool {
				seq := key.(uint64)
				entry := value.(*pendingEntry)

				entry.mu.Lock()
				defer entry.mu.Unlock()

				age := now.Sub(entry.sentAt)
				rto := initialRTO * time.Duration(1<<uint(entry.attempts))
				if rto > maxRTO {
					rto = maxRTO
				}
				if age < rto {
					return true
				}

				if entry.attempts >= maxRetransmitAttempts {
					utils.Debugf("[YDOCS] dropping packet seq=%d after %d attempts",
						seq, entry.attempts)
					t.pending.Delete(seq)
					t.RecordError()
					return true
				}

				if err := t.sendEnvelope(entry.envelope); err != nil {
					utils.Debugf("[YDOCS] retransmit seq=%d failed: %v", seq, err)
					t.pending.Delete(seq)
					t.RecordError()
					return true
				}
				entry.attempts++
				entry.sentAt = now
				utils.Debugf("[YDOCS] retransmit seq=%d attempt=%d", seq, entry.attempts)
				return true
			})
		}
	}
}

// dedupCleanupLoop evicts stale entries from the inbound dedup map.
func (t *YandexDocsTransport) dedupCleanupLoop() {
	ticker := time.NewTicker(dedupCleanupTick)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case now := <-ticker.C:
			t.receivedSeqs.Range(func(key, value interface{}) bool {
				e := value.(*dedupEntry)
				if now.Sub(e.seenAt) > dedupTTL {
					t.receivedSeqs.Delete(key)
				}
				return true
			})
		}
	}
}

// ---------------------------------------------------------------------
// Inbound handling
// ---------------------------------------------------------------------

func (t *YandexDocsTransport) handleMessage(session *DocSession, data []byte) {
	text := string(data)

	if session == nil || !session.Alive.Load() {
		return
	}

	// Socket.IO ping/pong
	if text == "2" {
		session.safeWrite(websocket.TextMessage, []byte("3"))
		return
	}
	if text == "3" {
		return
	}

	// Yandex ban signal
	if strings.Contains(text, "disconnectReason") {
		re := regexp.MustCompile(`"disconnectReason":\s*(\d+)`)
		matches := re.FindStringSubmatch(text)
		if len(matches) > 1 {
			if reason, err := strconv.Atoi(matches[1]); err == nil {
				session.LastDisconnectReason.Store(int32(reason))
				utils.Debugf("[YDOCS][%s] disconnect reason: %d", session.UserID, reason)
				if reason == 4007 {
					utils.Debugf("[YDOCS][%s] session banned (4007), extended backoff on reconnect",
						session.UserID)
				}
			}
		}
		return
	}

	// Legacy keep-alive marker (from older peers without OFSP)
	if strings.Contains(text, "---KA---") {
		return
	}

	if strings.Contains(text, "saveChanges") || strings.Contains(text, "cursor") {
		base64Str := t.extractBase64String(text)
		if base64Str == "" {
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			utils.Debugf("[YDOCS][%s] Base64 decode error: %v", session.UserID, err)
			return
		}

		// OFSP envelope? Parse flags, dedup, ACK, deliver payload.
		if isEnvelope(decoded) {
			t.handleEnvelope(session, decoded)
			return
		}

		// Legacy: raw IP packet (older peer). Pass through unchanged.
		t.RecordReceive(len(decoded))
		t.CallReceive(decoded)
	}
}

// handleEnvelope processes an inbound OFSP envelope:
//   - is-ACK  → remove matching outbound entry from pending
//   - dedup   → drop already-seen non-ACK seqs (still send ACK though)
//   - keep-alive → drop payload, just a liveness probe
//   - otherwise  → deliver payload (raw IP packet) to the tunnel
func (t *YandexDocsTransport) handleEnvelope(session *DocSession, env []byte) {
	if len(env) < envHdrLen {
		return
	}
	flags := env[2]
	seq := binary.BigEndian.Uint64(env[3:11])
	payload := env[envHdrLen:]

	// Inbound ACK: drop matching pending entry.
	if flags&flagIsACK != 0 {
		t.pending.Delete(seq)
		return
	}

	// Dedup check for inbound data packets.
	entry := &dedupEntry{seenAt: time.Now()}
	if _, loaded := t.receivedSeqs.LoadOrStore(seq, entry); loaded {
		// Duplicate. The sender likely missed our first ACK, so resend it.
		if flags&flagNeedsACK != 0 {
			_ = t.sendEnvelope(t.wrapACK(seq))
		}
		return
	}

	// Send ACK synchronously (sendEnvelope is non-blocking; if all
	// queues are full we simply lose the ACK and the sender will
	// retransmit, which we'll dedup).
	if flags&flagNeedsACK != 0 {
		_ = t.sendEnvelope(t.wrapACK(seq))
	}

	// Keep-alive: no payload, just a liveness probe. Count as receive.
	if flags&flagKeepAlive != 0 {
		t.RecordReceive(len(env))
		return
	}

	// Real data: deliver the raw IP packet to the tunnel.
	t.RecordReceive(len(payload))
	t.CallReceive(payload)
}

func (t *YandexDocsTransport) extractBase64String(response string) string {
	if strings.Contains(response, "saveChanges") {
		marker := `"excelAdditionalInfo":"`
		left := strings.Index(response, marker) + len(marker)
		if left < len(marker) {
			return ""
		}
		right := strings.Index(response[left:], `"`)
		if right == -1 {
			return ""
		}
		return response[left : left+right]
	}

	re := regexp.MustCompile(`"cursor":"[^;]+;([^"]+)"`)
	matches := re.FindStringSubmatch(response)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (t *YandexDocsTransport) scheduleReconnectForIndex(idx int, url string, attempt int) {
	next := attempt + 1
	if !t.IsRunning() || next >= t.GetConfig().MaxReconnectAttempts {
		return
	}

	d := reconnectBackoff(next)

	t.sessionMu.RLock()
	session := t.sessions[idx]
	t.sessionMu.RUnlock()
	if session != nil && session.LastDisconnectReason.Load() == 4007 {
		d *= 2
		if d > 5*time.Minute {
			d = 5 * time.Minute
		}
		utils.Debugf("[YDOCS][%d] banned session, extended backoff: %v (attempt %d)", idx, d, next)
	}

	utils.Debugf("[YDOCS][%d] reconnecting in %v (attempt %d)", idx, d, next)

	select {
	case <-time.After(d):
	case <-t.ctx.Done():
		return
	}

	if !t.IsRunning() {
		return
	}

	t.RecordReconnect()
	t.connectToDocForIndex(idx, url, next)
}

func reconnectBackoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	shift := n - 1
	if shift > 5 {
		shift = 5
	}
	d := 500 * time.Millisecond * time.Duration(1<<uint(shift))
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	d += time.Duration(rand.Int63n(int64(d/2) + 1))
	return d
}

func (t *YandexDocsTransport) fetchDocInfo(url, userID string) (YandexDocsInfo, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects (login required? doc not public?)")
			}
			return nil
		},
		Timeout: 15 * time.Second,
	}

	utils.Debugf("[YDOCS] fetchDocInfo GET %s", url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	defer resp.Body.Close()

	htmlBytes, _ := io.ReadAll(resp.Body)
	html := string(htmlBytes)
	utils.Debugf("[YDOCS] response status=%d finalURL=%s body=%dB",
		resp.StatusCode, resp.Request.URL.String(), len(html))

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		hint := "no client-config script"
		if strings.Contains(html, "passport") ||
			strings.Contains(strings.ToLower(html), "login") {
			hint = "looks like a login page (doc not public?)"
		}
		return YandexDocsInfo{}, fmt.Errorf(
			"config not found: %s (status %d, final %s)",
			hint, resp.StatusCode, resp.Request.URL.String())
	}

	var config map[string]interface{}
	if err := json.Unmarshal([]byte(matches[1]), &config); err != nil {
		return YandexDocsInfo{}, fmt.Errorf("client-config is not valid JSON: %w", err)
	}

	officeAction, ok := config["officeActionData"].(map[string]interface{})
	if !ok || officeAction == nil {
		return YandexDocsInfo{}, fmt.Errorf("officeActionData missing - will reconnect")
	}

	editorConfigRaw, ok := officeAction["editor_config"].(map[string]interface{})
	if !ok || editorConfigRaw == nil {
		return YandexDocsInfo{}, fmt.Errorf("editor_config nil - will reconnect")
	}

	balancerURL, ok := officeAction["balancer_url"].(string)
	if !ok || balancerURL == "" {
		return YandexDocsInfo{}, fmt.Errorf("officeActionData.balancer_url missing - will reconnect")
	}
	host := strings.TrimPrefix(balancerURL, "https://")

	document, ok := editorConfigRaw["document"].(map[string]interface{})
	if !ok || document == nil {
		return YandexDocsInfo{}, fmt.Errorf("editor_config.document missing - will reconnect")
	}

	token, ok := editorConfigRaw["token"].(string)
	if !ok || token == "" {
		return YandexDocsInfo{}, fmt.Errorf("editor_config.token missing - will reconnect")
	}

	docKey, ok := document["key"].(string)
	if !ok || docKey == "" {
		return YandexDocsInfo{}, fmt.Errorf("editor_config.document.key missing - will reconnect")
	}

	perms, _ := document["permissions"].(map[string]interface{})
	if perms == nil {
		perms = make(map[string]interface{})
	}

	return YandexDocsInfo{
		CookieStr:   strings.Join(cookies, "; "),
		Token:       token,
		DocID:       docKey,
		Origin:      balancerURL,
		Host:        host,
		WsURL:       fmt.Sprintf("wss://%s/2024.1.1-375/doc/%s/c/?EIO=4&transport=websocket", host, docKey),
		Permissions: perms,
		OpenCmd: map[string]interface{}{
			"c":      "open",
			"id":     docKey,
			"userid": userID,
			"format": document["fileType"],
			"url":    document["url"],
			"title":  document["title"],
			"lcid":   25,
		},
	}, nil
}

func randUserID() string {
	return fmt.Sprintf("%010d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1000000000))
}
