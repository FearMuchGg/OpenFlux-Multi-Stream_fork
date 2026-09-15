package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	godebug "runtime/debug"
	"strconv"
	"strings"
	"time"

	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/cupsonline"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var (
	globalDocUrl string
	maxToken     string
	maxUid       string
	localIP      string
)

func main() {
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node")
	client := flag.Bool("client", false, "Run as client")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, vyandex, oneme, cupsonline)")
	mode := flag.String("mode", "proxy", "Exit-node mode: proxy (default, works everywhere) or raw (Linux only, needs root)")
	flag.StringVar(&globalDocUrl, "url", "http://#",
		"Document URL(s) for Yandex.Docs transport. Comma-separated list enables multi-stream (issue #50): "+
			"each TCP connection is pinned to one document so its packet order is preserved, new connections "+
			"are spread across the rest, and the tunnel survives any single document going dead. "+
			"A plain single URL keeps the legacy single-channel behavior.")
	flag.StringVar(&maxToken, "maxToken", "", "MAX Web token. If u use MAX transport")
	flag.StringVar(&maxUid, "maxUid", "", "MAX call user id. If u use MAX transport")
	flag.StringVar(&localIP, "local-ip", "", "Egress IP for exit node (raw mode only, scoped RST drop)")
	encryptionKeyFile := flag.String("encryption-key-file", "",
		"Optional: encrypt the transport with AES-256-GCM using a shared secret read from this file. "+
			"Both peers must use the same secret; unset means unencrypted, unchanged behavior")
	statusEvery := flag.Duration("multistream-status", 0,
		"If >0 and --url has multiple URLs, log per-stream connection state on this interval (e.g. 5s). "+
			"No-op with a single URL.")
	livenessProbe := flag.Bool("liveness-probe", true,
		"Probe every document with a ping/pong so a dead channel is taken out of rotation promptly. "+
			"Both peers must run a build that answers probes; disable this to interoperate with an older peer, "+
			"at the cost of slower failure detection.")
	tcpMaxRetries := flag.Uint64("tcp-max-retries", 64,
		"Failed retransmission probes gVisor allows before aborting a tunnelled TCP connection. "+
			"gVisor's default is 15, which tears every connection down after roughly 5-10 minutes of total outage.")
	statsInterval := flag.Duration("stats-interval", 10*time.Second,
		"How often to print the [STATS] line: packet and byte rates, retransmits, established connections. "+
			"Printed without --debug; set 0 to disable.")
	pprofAddr := flag.String("pprof", "",
		"Optional address to serve Go pprof on, e.g. 127.0.0.1:6060. Use it to tell CPU burn apart from a slow channel.")
	gcPercent := flag.Int("gc-percent", 0,
		"Override GOGC. 0 keeps the Go default (100). Lower means more frequent collection; on a small VPS "+
			"set GOMEMLIMIT in the environment instead, which caps memory without throttling on allocation rate.")
	logInFile := flag.Bool("log-in-file", false,
		"Also write logs to a timestamped file next to the executable (output still goes to stderr too). "+
			"One file per run, so the log of the run that failed is not overwritten by the restart.")
	flag.Parse()

	// Before anything else that logs, so the file captures the whole run.
	if *logInFile {
		path, err := utils.LogToFile()
		if err != nil {
			log.Printf("--log-in-file: could not open a log file: %v", err)
		} else {
			log.Printf("logging to %s", path)
		}
	}

	if *tcpMaxRetries > 0 {
		tunnel.TCPMaxRetries = *tcpMaxRetries
	}
	if *statsInterval > 0 {
		tunnel.StatsInterval = *statsInterval
	}
	if *gcPercent > 0 {
		godebug.SetGCPercent(*gcPercent)
	}
	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof on http://%s/debug/pprof/", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("pprof server: %v", err)
			}
		}()
	}

	exitMode, err := tunnel.ParseExitMode(*mode)
	if err != nil {
		log.Fatalf("--mode: %v", err)
	}

	if *exitNode && exitMode == tunnel.ExitModeRaw && localIP != "" {
		tunnel.SetLocalIP(localIP)
	}

	// The exit node used to force GOGC=20 to keep the heap tight on a tiny VPS.
	// That collects roughly five times as often as the Go default, and against
	// this project's per-packet allocation rate it costs real throughput.
	// GOMEMLIMIT is the right knob for capping memory without paying on every
	// allocation, so the forced value is gone; --gc-percent brings it back if a
	// particular VPS turns out to need it.

	if !*exitNode && !*client {
		flag.Usage()
		os.Exit(1)
	}

	if *debug {
		utils.EnableDebug()
	}

	log.Printf("=== Universal Bypass Tool ===")
	log.Printf("Mode: %s", map[bool]string{true: "EXIT NODE", false: "CLIENT"}[*exitNode])
	log.Printf("Transport: %s", *transportType)
	if *exitNode {
		log.Printf("Exit mode: %s", exitMode.String())
	}

	config := transport.DefaultConfig()
	config.LivenessProbe = *livenessProbe
	var inner transport.Transport

	// Split --url on commas so a single flag can carry N documents. Only used
	// for Yandex-family transports here; cupsonline already carries multi-room
	// state inside its base64-encoded URL, and oneme has its own auth path.
	yandexURLs := splitURLs(globalDocUrl)

	switch *transportType {
	case "vyandex":
		inner = buildYandexInner(yandexURLs, config, true)
	case "yandex":
		inner = buildYandexInner(yandexURLs, config, false)
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		inner = oneme.NewOneMeTransport(*exitNode, maxToken, uidint, config)
	case "cupsonline":
		if len(yandexURLs) > 1 {
			log.Fatalf("cupsonline: multi-URL at CLI level is not supported (already multi-room); pass one base64 URL")
		}
		inner = cupsonline.NewCupsonlineTransport(globalDocUrl, config, !*exitNode)
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	// Keep our own handle on the multi-stream wrapper. The encryption branch
	// below reassigns `inner`, and type-asserting on it afterwards used to make
	// --multistream-status silently do nothing whenever encryption was enabled.
	var multi *transport.MultiStreamTransport
	if ms, ok := inner.(*transport.MultiStreamTransport); ok {
		multi = ms
	}

	if *encryptionKeyFile != "" {
		secretBytes, err := os.ReadFile(*encryptionKeyFile)
		if err != nil {
			log.Fatalf("Read encryption key file: %v", err)
		}
		context := *transportType
		if globalDocUrl != "" {
			context = globalDocUrl
		}
		encrypted, err := transport.NewEncryptedTransport(inner, strings.TrimSpace(string(secretBytes)), context, *exitNode)
		if err != nil {
			log.Fatalf("Configure encrypted transport: %v", err)
		}
		inner = encrypted
		log.Printf("Transport encryption: AES-256-GCM enabled")
	}

	trans := transport.NewCompressedTransport(inner)

	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	// A multi-stream setup starts whatever documents it can and keeps running
	// on the survivors, so only a total failure is fatal above.
	//
	// Connections settle asynchronously, so report after they have had a
	// moment. The old check ran immediately and always printed "0 of 2 up",
	// which is alarming and wrong — the WebSockets had not even dialled yet.
	if multi != nil {
		total := len(multi.Streams())
		go func() {
			time.Sleep(3 * time.Second)
			up := countConnected(multi)
			switch {
			case up == 0:
				log.Printf("Multi-stream: no document connected after 3s — check the URLs and this host's network")
			case up < total:
				log.Printf("Multi-stream: %d of %d documents up; the tunnel continues on the rest", up, total)
			}
		}()
	}

	// Optional per-stream status logger for multi-stream setups.
	if multi != nil && *statusEvery > 0 {
		go multistreamStatusLoop(multi, *statusEvery)
	}

	tun := tunnel.NewTCPTunnelMode(trans, *exitNode, exitMode)

	if *exitNode {
		if exitMode == tunnel.ExitModeRaw {
			log.Printf("Running as EXIT NODE (raw mode)")
			if localIP != "" {
				log.Printf("! Run: sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s %s -j DROP", localIP)
			} else {
				log.Printf("! Kernel RSTs would tear down tunnel connections. Prefer a scoped rule:")
				log.Printf("!   assign a dedicated alias IP, run with --local-ip <ip>, then:")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -s <ip> -j DROP")
				log.Printf("! Host-wide fallback (drops ALL outbound RST; makes closed ports look filtered):")
				log.Printf("!   sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP")
			}
		} else {
			log.Printf("Running as EXIT NODE (proxy mode)")
		}
		select {}
	} else {
		log.Printf("Running as CLIENT (SOCKS5 on %s)", *socksAddr)
		socks5Server := socks5.NewSOCKS5Server(*socksAddr, tun)
		log.Fatal(socks5Server.Start())
	}
}

// splitURLs splits a comma-separated --url flag, trimming whitespace and
// dropping empties. A plain single URL yields a length-1 slice; a fully-empty
// value yields a length-1 slice with an empty string so callers can pass
// urls[0] without a nil-slice check (the transport itself will fail cleanly).
func splitURLs(raw string) []string {
	if raw == "" {
		return []string{""}
	}
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

// buildYandexInner returns either a single YandexDocsTransport (N=1, exactly
// the legacy behavior) or a MultiStreamTransport fanning out over N of them.
// useVolga picks the alternate engine.io-based vyandex path.
func buildYandexInner(urls []string, config transport.TransportConfig, useVolga bool) transport.Transport {
	mk := func(u string) transport.Transport {
		if useVolga {
			return yandex.NewYandexVolgaTransport(u, config)
		}
		return yandex.NewYandexDocsTransport(u, config)
	}
	if len(urls) <= 1 {
		return mk(urls[0])
	}
	inners := make([]transport.Transport, 0, len(urls))
	for _, u := range urls {
		inners = append(inners, mk(u))
	}

	// Retry-buffer limits come from the transport config so memory-tight
	// targets (the iOS Network Extension) can ask for something smaller.
	msCfg := transport.DefaultMultiStreamConfig()
	if config.MultiStreamBufferBytes > 0 {
		msCfg.MaxBufferBytes = config.MultiStreamBufferBytes
	}
	if config.MultiStreamMaxPackets > 0 {
		msCfg.MaxBufferPackets = config.MultiStreamMaxPackets
	}

	// Fingerprint every document so the two ends can be compared at a glance.
	// A mismatch is otherwise completely silent: each side connects happily to
	// its own documents and simply never hears the other, which looks exactly
	// like a tunnel that is broken for some other reason.
	log.Printf("Multi-stream: %d documents", len(urls))
	for i, u := range urls {
		log.Printf("Multi-stream: document #%d %s", i, docFingerprint(u))
	}
	return transport.NewMultiStreamTransportWithConfig(inners, msCfg)
}

// docFingerprint is a short stable digest of a document URL.
//
// The URL itself is credential-ish and ends up in logs that get copied around,
// but without some identifier a client and an exit node pointed at different
// documents are indistinguishable from a tunnel that is simply broken.
func docFingerprint(u string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(u)))
	return fmt.Sprintf("#%x", sum[:4])
}

// countConnected reports how many inner streams currently report themselves up.
func countConnected(ms *transport.MultiStreamTransport) int {
	up := 0
	for _, s := range ms.Streams() {
		if s.IsConnected() {
			up++
		}
	}
	return up
}

func yesNo(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// multistreamStatusLoop prints one status line per interval, useful for
// watching a multi-stream setup during tests and stability checks.
//
// QUAR means MultiStream itself has taken the stream out of rotation after
// repeated send failures — its own verdict, independent of what the stream
// claims about itself. buf/replayed/dropped show what the retry buffer is
// doing while documents come and go.
func multistreamStatusLoop(ms *transport.MultiStreamTransport, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for range tick.C {
		streams := ms.Streams()
		parts := make([]string, 0, len(streams))
		up := 0
		for i, s := range streams {
			st := s.Stats()
			state := "DOWN"
			switch {
			case ms.Quarantined(i):
				state = "QUAR"
			case st.Connected:
				state = "UP"
				up++
			}
			part := fmt.Sprintf("s%d=%s(rx=%d,tx=%d,rc=%d)",
				i, state, st.PacketsRecv, st.PacketsSent, st.Reconnects)

			// Liveness, in two parts on purpose. "probe=sent/miss/pong/late" is
			// whether the peer answers; "alive" is whether the stream is
			// carrying traffic at all. A stream can be perfectly alive while
			// the probe fails, and a non-zero "late" count is the signature of
			// a probe that is delivered but slower than its timeout.
			if hr, ok := s.(transport.HealthReporter); ok {
				sent, misses, pongs, late := hr.ProbeStats()
				part += fmt.Sprintf(" probe=%d/%d/%d/%d alive=%s rxAge=%v",
					sent, misses, pongs, late,
					yesNo(hr.Alive()), hr.RxAge().Round(time.Second))
			}

			// A transport's write-path health was previously invisible without
			// --debug: queue-full gives-ups and socket write errors both showed
			// up only as a lost packet somewhere downstream.
			if fc, ok := s.(transport.FrameCounter); ok {
				frames, writeErrs, timeouts, bad := fc.FrameStats()
				part += fmt.Sprintf(" frames=%d werr=%d qfull=%d badframe=%d",
					frames, writeErrs, timeouts, bad)
			}
			parts = append(parts, part)
		}
		bufPackets, bufBytes := ms.Buffered()
		droppedOld, droppedFull := ms.Dropped()
		log.Printf("[MULTI] up=%d/%d buf=%d/%dB replayed=%d dropped=%d(old)/%d(full) %s",
			up, len(streams), bufPackets, bufBytes, ms.Replayed(),
			droppedOld, droppedFull, strings.Join(parts, " "))
	}
}
