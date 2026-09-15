package yandex

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	Info       YandexDocsInfo
	Conn       *websocket.Conn
	WriteQueue chan []byte
	UserID     string
	writeMu    sync.Mutex
}

// writeTimeout bounds a single WebSocket write.
//
// Without it a write blocks indefinitely once the kernel send buffer fills, and
// it does so while holding writeMu. That mutex is shared with the pong replies
// the read loop sends, so a wedged write stops inbound reads as well and the
// whole stream goes silent with no error anywhere. Failing after a bounded wait
// is strictly better: the frame is lost either way, but the error path can mark
// the stream down and let the reconnect logic take over.
const writeTimeout = 10 * time.Second

func (s *DocSession) safeWrite(messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.Conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return s.Conn.WriteMessage(messageType, data)
}

// Liveness probes ride on cursor index 18 — the same index as data, which is
// the only path proven to be relayed by the server. An earlier version used
// index 19 and probes silently never arrived, which is indistinguishable from a
// dead peer. They are told apart by their marker, exactly as the original
// ---KA--- keep-alive was.
//
// Probes must be intercepted before the cursor regex in extractBase64String,
// otherwise the marker text would be handed to the base64 decoder. The markers
// contain '-', which is not in the base64 alphabet, so a payload can never be
// mistaken for one.
const (
	yandexProbeCursor = "18"
	yandexPingMarker  = "---OFXPING---"
	yandexPongMarker  = "---OFXPONG---"
)

type YandexDocsTransport struct {
	*transport.BaseTransport

	url     string
	session *DocSession

	userCounter atomic.Int32
	baseUserID  string

	live *transport.LivenessTracker

	// Hot-path counters. Without them a full write queue and a failing socket
	// were both completely invisible: the first only logged at debug level, the
	// second was a silent drop.
	framesSent   atomic.Uint64
	writeErrors  atomic.Uint64
	sendTimeouts atomic.Uint64
	badFrames    atomic.Uint64
}

func NewYandexDocsTransport(url string, config transport.TransportConfig) *YandexDocsTransport {
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config),
		url:           url,
	}
	if config.LivenessProbe {
		t.live = transport.NewLivenessTracker(transport.DefaultProbeInterval)
	} else {
		t.live = transport.NewDisabledLivenessTracker()
	}
	t.baseUserID = randUserID()
	return t
}

// FrameStats implements transport.FrameCounter, so the status line can show
// what the write path is doing without --debug.
func (t *YandexDocsTransport) FrameStats() (frames, writeErrors, sendTimeouts, badFrames uint64) {
	return t.framesSent.Load(), t.writeErrors.Load(), t.sendTimeouts.Load(), t.badFrames.Load()
}

// probeMessage builds a cursor message carrying a liveness probe.
func probeMessage(marker string, nonce uint64) []byte {
	return []byte(fmt.Sprintf(`42["message",{"type":"cursor","cursor":"%s;%s%d"}]`,
		yandexProbeCursor, marker, nonce))
}

// parseProbe extracts the nonce from a message carrying marker. The markers
// contain '-' and 'F', which are not in the base64 alphabet, so a payload can
// never be mistaken for a probe.
func parseProbe(text, marker string) (uint64, bool) {
	i := strings.Index(text, marker)
	if i < 0 {
		return 0, false
	}
	rest := text[i+len(marker):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	nonce, err := strconv.ParseUint(rest[:end], 10, 64)
	if err != nil {
		return 0, false
	}
	return nonce, true
}

// A data frame is a Socket.IO cursor message carrying the base64 payload:
//
//	42["message",{"type":"cursor","cursor":"18;<base64>"}]
//
// Split into prefix/suffix so the frame can be assembled with append into a
// reused buffer. The old writerLoop built it with fmt.Sprintf and then converted
// the string back to []byte — two extra allocations per packet, on the hot path.
var (
	cursorFramePrefix = []byte(`42["message",{"type":"cursor","cursor":"18;`)
	cursorFrameSuffix = []byte(`"}]`)
)

// appendCursorFrame appends one data frame to dst and returns the result.
func appendCursorFrame(dst, base64Payload []byte) []byte {
	dst = append(dst, cursorFramePrefix...)
	dst = append(dst, base64Payload...)
	return append(dst, cursorFrameSuffix...)
}

// cursorPayloadRe pulls the base64 payload out of a cursor message.
//
// Package-level on purpose: this used to be compiled inside
// extractBase64String, which meant a full RE2 program build for every single
// received data message.
var cursorPayloadRe = regexp.MustCompile(`"cursor":"[^;]+;([^"]+)"`)

func (t *YandexDocsTransport) Start() error {
	if err := t.BaseTransport.Start(); err != nil {
		return err
	}

	t.baseUserID = randUserID()
	utils.SafeGo("yandex.keepAlive", t.keepAliveLoop)
	t.connectToDoc(0)

	return nil
}

// sendTimeout bounds how long Send waits for room in the write queue.
//
// The wait matters. WritePackets hands packets to us synchronously from
// gVisor's sender, so blocking here throttles that sender and lets its
// congestion window stabilise. The old non-blocking send simply dropped the
// packet, and gVisor could not see the drop: it believed the packet had been
// transmitted and only learned otherwise when an RTO expired, which it then
// read as congestion. Under load that is self-reinforcing — the faster the
// sender goes, the more it loses.
//
// It must stay short. ACKs from the receive path travel the same queue, so a
// long block here would stall the WebSocket reader and make things worse.
const sendTimeout = 25 * time.Millisecond

func (t *YandexDocsTransport) Send(data []byte) error {
	if !t.IsConnected() {
		return fmt.Errorf("transport not connected")
	}

	t.Mu.RLock()
	session := t.session
	t.Mu.RUnlock()

	if session == nil {
		return fmt.Errorf("no active session")
	}

	// Fast path: room available, no timer needed.
	select {
	case session.WriteQueue <- data:
		t.RecordSend(len(data))
		return nil
	default:
	}

	// Queue is full. Wait a bounded time for the writer to drain it rather
	// than dropping the packet on the floor.
	timer := time.NewTimer(sendTimeout)
	defer timer.Stop()

	select {
	case session.WriteQueue <- data:
		t.RecordSend(len(data))
		return nil
	case <-timer.C:
		t.sendTimeouts.Add(1)
		return fmt.Errorf("write queue full after %v", sendTimeout)
	}
}

func (t *YandexDocsTransport) connectToDoc(attempt int) {
	if !t.IsRunning() {
		return
	}

	utils.Debugf("[YDOCS] connectToDoc attempt ...")

	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.Debugf("[PANIC] recovered in yandex.connect: %v", r)
			}
		}()
		t.Mu.Lock()
		existingSession := t.session
		t.Mu.Unlock()

		var userID string
		if existingSession != nil {
			userID = existingSession.UserID
		} else {
			suffix := fmt.Sprintf("%03d", t.userCounter.Add(1)%1000)
			userID = t.baseUserID + suffix
		}

		info, err := t.fetchDocInfo(t.url, userID)
		if err != nil {
			utils.Debugf("[YDOCS] fetchDocInfo failed: %v", err)
			t.scheduleReconnect(attempt)
			return
		}

		// Hard TCP dial timeout so a stuck connect/DNS to the balancer host
		// can't hang the whole transport (HandshakeTimeout alone proved
		// insufficient on iOS).
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

		utils.Debugf("[YDOCS] WebSocket dial %s", info.WsURL)
		conn, resp, err := dialer.Dial(info.WsURL, headers)
		if err != nil {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			utils.Debugf("[YDOCS] WebSocket dial failed (http %d): %v", status, err)
			t.scheduleReconnect(attempt)
			return
		}
		utils.Debugf("[YDOCS] WebSocket connected to %s", info.Host)

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

		t.Mu.Lock()
		t.session = session
		t.SetConnected(true)
		t.Mu.Unlock()

		if existingSession == nil {
			utils.SafeGo("yandex.writer", t.writerLoop)
		}

		// Auth - use safeWrite
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
			_, message, err := conn.ReadMessage()
			if err != nil {
				utils.Debugf("[YDOCS] Read error: %v", err)
				t.SetConnected(false)
				// If the session was healthy for a while, treat the next
				// connect as fresh (attempt -1 -> next attempt 0) so backoff
				// doesn't keep growing across normal long-lived reconnects.
				next := attempt
				if time.Since(connectedAt) > 15*time.Second {
					next = -1
				}
				t.scheduleReconnect(next)
				return
			}
			t.handleMessage(session, message)
		}
	}()
}

func (t *YandexDocsTransport) writerLoop() {
	// Reused across iterations. This goroutine is the only writer of both
	// buffers, so no synchronisation is needed; safeWrite has returned by the
	// time we touch them again.
	var b64 []byte
	var frame []byte

	// Blocking wait with a timeout instead of polling with Sleep. The old loop
	// slept a full 10 ms whenever the queue happened to be empty, so a packet
	// arriving just after that check waited out the whole sleep.
	idle := time.NewTimer(10 * time.Millisecond)
	defer idle.Stop()

	for t.IsRunning() {
		t.Mu.Lock()
		session := t.session
		t.Mu.Unlock()

		if session == nil || session.Conn == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		select {
		case packet := <-session.WriteQueue:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(10 * time.Millisecond)

			// AppendEncode writes the base64 straight into the reused buffer:
			// no intermediate string, no []byte(string) conversion.
			b64 = base64.StdEncoding.AppendEncode(b64[:0], packet)
			frame = appendCursorFrame(frame[:0], b64)

			if err := session.safeWrite(websocket.TextMessage, frame); err != nil {
				utils.Debugf("[YDOCS] Write error: %v", err)
				// Send already told the caller the packet was accepted, so it
				// is gone. Mark the stream down so MultiStream stops routing
				// to it, instead of silently losing 1/N of all traffic.
				t.writeErrors.Add(1)
				t.SetConnected(false)
			} else {
				t.framesSent.Add(1)
			}

		case <-idle.C:
			// Nothing to send. Loop round to pick up a session replacement.
			idle.Reset(10 * time.Millisecond)
		}
	}
}

func (t *YandexDocsTransport) keepAliveLoop() {
	ticker := time.NewTicker(t.live.Interval())
	defer ticker.Stop()

	// Legacy keep-alive that older peers send; they never answer a probe, so
	// it is ignored rather than fed to the packet parser.
	legacyKeepAlive := `42["message",{"type":"cursor","cursor":"18;---KA---"}]`

	var suspectLogged bool

	for t.IsRunning() {
		<-ticker.C
		t.Mu.Lock()
		session := t.session
		t.Mu.Unlock()

		if session == nil || session.Conn == nil {
			continue
		}

		if t.live.Disabled() {
			if err := session.safeWrite(websocket.TextMessage, []byte(legacyKeepAlive)); err != nil {
				utils.Debugf("[YDOCS] Keep-alive failed: %v", err)
				t.SetConnected(false)
			}
			continue
		}

		if nonce := t.live.NextProbe(); nonce != 0 {
			if err := session.safeWrite(websocket.TextMessage, probeMessage(yandexPingMarker, nonce)); err != nil {
				utils.Debugf("[YDOCS] Probe write failed: %v", err)
				t.SetConnected(false)
				continue
			}
			utils.Debugf("[YDOCS] probe sent nonce=%d", nonce)
		}

		// An unanswered probe is NOT treated as a dead stream.
		//
		// The socket is demonstrably open: the read loop is still running and
		// the writer still accepts frames. All we actually know is that the peer
		// stopped answering. Marking the stream disconnected here caused a real
		// outage — nothing restores a disconnected stream, so a tunnel that was
		// still carrying 40 KB/s went permanently one-way. A stream delivering
		// validated frames is alive whatever the probe says, and MultiStream now
		// merely prefers a stream that answers.
		if t.live.Suspect() {
			if !suspectLogged {
				suspectLogged = true
				sent, misses, pongs, late := t.live.ProbeStats()
				frames := t.live.ValidFrames()

				// Zero frames AND zero pongs means nobody is on the other end of
				// this document — a completely different problem from a slow or
				// half-broken channel. The log is the only place that
				// distinction is visible, so spell it out.
				hint := ""
				if frames == 0 && pongs == 0 {
					hint = " — no frames and no probe answers at all: is the other end running, and on this same document?"
				}

				// Deliberately not a Debugf: this verdict has to be visible
				// without --debug, and it fires once per episode.
				log.Printf("[YDOCS] probe unanswered (%d sent, %d missed, %d pongs, %d late) — "+
					"last frame %v ago, %d frames; keeping the stream in rotation%s",
					sent, misses, pongs, late,
					t.live.RxAge().Round(time.Millisecond), frames, hint)
			}
		} else {
			suspectLogged = false
		}
	}
}

// Healthy implements transport.HealthReporter: the peer answered our last probe.
func (t *YandexDocsTransport) Healthy() bool { return t.live.Healthy() }

// Alive implements transport.HealthReporter: the stream is carrying traffic,
// by probe or by validated frames.
func (t *YandexDocsTransport) Alive() bool { return t.live.Alive() }

// ProbeStats implements transport.HealthReporter.
func (t *YandexDocsTransport) ProbeStats() (sent, misses, pongs, latePongs uint64) {
	return t.live.ProbeStats()
}

// RxAge implements transport.HealthReporter.
func (t *YandexDocsTransport) RxAge() time.Duration { return t.live.RxAge() }

func (t *YandexDocsTransport) handleMessage(session *DocSession, data []byte) {
	text := string(data)

	// Legacy keep-alive from an older peer: ignore.
	if strings.Contains(text, "---KA---") {
		return
	}

	// Liveness probes. Handled before the cursor regex below, which would
	// otherwise treat the marker as a base64 payload.
	if nonce, ok := parseProbe(text, yandexPingMarker); ok {
		if session != nil && session.Conn != nil {
			if err := session.safeWrite(websocket.TextMessage, probeMessage(yandexPongMarker, nonce)); err != nil {
				utils.Debugf("[YDOCS] pong write failed: %v", err)
			} else {
				utils.Debugf("[YDOCS] probe received nonce=%d -> pong", nonce)
			}
		}
		return
	}
	if nonce, ok := parseProbe(text, yandexPongMarker); ok {
		utils.Debugf("[YDOCS] pong received nonce=%d rtt=%v", nonce, t.live.RTT().Round(time.Millisecond))
		t.live.OnPong(nonce)
		// A pong proves the socket works, so it also clears any earlier
		// write-error verdict.
		t.SetConnected(true)
		return
	}

	// Socket.IO ping - respond with pong (use safeWrite)
	if text == "2" {
		if session != nil && session.Conn != nil {
			session.safeWrite(websocket.TextMessage, []byte("3"))
		}
		return
	}
	if text == "3" {
		return
	}

	if strings.Contains(text, "saveChanges") || strings.Contains(text, "cursor") {
		base64Str := t.extractBase64String(text)
		if base64Str == "" {
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(base64Str)
		if err != nil {
			utils.Debugf("[YDOCS] Base64 decode error: %v", err)
			return
		}

		// Only count this as evidence of life once it is recognisably one of
		// our frames. A shared document carries other people's editor traffic,
		// and counting that would let a dead stream look alive forever.
		if !transport.LooksLikeFrame(decoded) {
			t.badFrames.Add(1)
			utils.Debugf("[YDOCS] ignoring %d-byte cursor payload that is not an OpenFlux frame",
				len(decoded))
			return
		}

		t.live.OnValidFrame()
		t.RecordReceive(len(decoded))
		t.CallReceive(decoded)
	}
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

	matches := cursorPayloadRe.FindStringSubmatch(response)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (t *YandexDocsTransport) scheduleReconnect(attempt int) {
	next := attempt + 1
	if !t.IsRunning() || next >= t.GetConfig().MaxReconnectAttempts {
		return
	}

	// Back off before retrying so a server that closes us immediately doesn't
	// turn into a tight connect/close loop (previously reconnect was instant).
	d := reconnectBackoff(next)
	utils.Debugf("[YDOCS] reconnecting in %v (attempt %d)", d, next)
	time.Sleep(d)
	if !t.IsRunning() {
		return
	}

	t.RecordReconnect()
	t.connectToDoc(next)
}

// reconnectBackoff returns an exponential backoff with jitter, capped at 15s.
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
	// add up to +50% jitter
	d += time.Duration(rand.Int63n(int64(d/2) + 1))
	return d
}

func (t *YandexDocsTransport) fetchDocInfo(url, userID string) (YandexDocsInfo, error) {
	client := &http.Client{
		// Cap redirects so an auth/login redirect loop fails fast instead of
		// hanging until the timeout (a private doc redirects to passport).
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
	utils.Debugf("[YDOCS] response status=%d finalURL=%s body=%dB", resp.StatusCode, resp.Request.URL.String(), len(html))

	var cookies []string
	for _, c := range resp.Cookies() {
		cookies = append(cookies, fmt.Sprintf("%s=%s", c.Name, c.Value))
	}

	re := regexp.MustCompile(`<script[^>]*id="client-config"[^>]*>(.*?)</script>`)
	matches := re.FindStringSubmatch(html)
	if len(matches) < 2 {
		// Help diagnose: is this a login page, a new-editor page, etc.?
		hint := "no client-config script"
		if strings.Contains(html, "passport") || strings.Contains(strings.ToLower(html), "login") {
			hint = "looks like a login page (doc not public?)"
		}
		return YandexDocsInfo{}, fmt.Errorf("config not found: %s (status %d, final %s)", hint, resp.StatusCode, resp.Request.URL.String())
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
