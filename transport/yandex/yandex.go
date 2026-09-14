package yandex

import (
	"context"
	"encoding/base64"
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

// Send sends data through one of the alive sessions using Round-Robin.
func (t *YandexDocsTransport) Send(data []byte) error {
	if !t.IsConnected() {
		t.RecordError()
		return fmt.Errorf("transport not connected")
	}

	t.sessionMu.RLock()
	sessions := t.sessions
	t.sessionMu.RUnlock()

	n := len(sessions)
	if n == 0 {
		t.RecordError()
		return fmt.Errorf("no sessions")
	}

	start := int(t.rrCounter.Add(1)) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		sess := sessions[idx]
		if sess == nil || sess.Conn == nil || !sess.Alive.Load() {
			continue
		}
		select {
		case sess.WriteQueue <- data:
			t.RecordSend(len(data))
			return nil
		default:
			// queue full, try next
		}
	}

	t.RecordError()
	return fmt.Errorf("all write queues full")
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
// just died and reroutes them via Send to an alive session.
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
			time.Sleep(2 * time.Millisecond)
			if err := t.Send(packet); err != nil {
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

				// Reroute the packet we just pulled from the queue
				go func(p []byte) {
					time.Sleep(5 * time.Millisecond)
					if err := t.Send(p); err != nil {
						utils.Debugf("[YDOCS][%d] requeue failed: %v", idx, err)
					}
				}(packet)

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
					// Simple keep-alive marker
					select {
					case session.WriteQueue <- []byte("---KA---"):
					default:
					}
				}
			}
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

	// Keep-alive marker
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

		// Pass through to upper layers (OFSP/Batched will handle it)
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
