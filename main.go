package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	godebug "runtime/debug"
	"strconv"
	"strings"

	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/cupsonline"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var (
	globalDocUrls string
	maxToken      string
	maxUid        string
	localIP       string
)

func main() {
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node")
	client := flag.Bool("client", false, "Run as client")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, vyandex, oneme, cupsonline)")
	mode := flag.String("mode", "proxy", "Exit-node mode: proxy (default, works everywhere) or raw (Linux only, needs root)")
	flag.StringVar(&globalDocUrls, "urls", "http://#", "Document URLs (comma-separated). If u use Yandex.Docs transport")
	flag.StringVar(&maxToken, "maxToken", "", "MAX Web token. If u use MAX transport")
	flag.StringVar(&maxUid, "maxUid", "", "MAX call user id. If u use MAX transport")
	flag.StringVar(&localIP, "local-ip", "", "Egress IP for exit node (raw mode only, scoped RST drop)")
	encryptionKeyFile := flag.String("encryption-key-file", "",
		"Optional: encrypt the transport with AES-256-GCM using a shared secret read from this file. "+
			"Both peers must use the same secret; unset means unencrypted, unchanged behavior")
	flag.Parse()

	exitMode, err := tunnel.ParseExitMode(*mode)
	if err != nil {
		log.Fatalf("--mode: %v", err)
	}

	if *exitNode && exitMode == tunnel.ExitModeRaw && localIP != "" {
		tunnel.SetLocalIP(localIP)
	}

	// The exit node often runs on a tiny VPS; keep the heap tight under load
	// (GC aggressively). Set GOMEMLIMIT in the environment for a hard soft-cap.
	if *exitNode {
		godebug.SetGCPercent(20)
	}

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
	var inner transport.Transport

	switch *transportType {
	case "vyandex":
		// For backward compatibility, vyandex still uses single URL
		urls := strings.Split(globalDocUrls, ",")
		for i := range urls {
			urls[i] = strings.TrimSpace(urls[i])
		}
		// Filter out empty strings and invalid URLs
		var validUrls []string
		for _, u := range urls {
			if u == "" || u == "http://#" {
				continue
			}
			if _, err := url.Parse(u); err == nil {
				validUrls = append(validUrls, u)
			}
		}
		if len(validUrls) == 0 {
			log.Fatalf("No valid URLs provided for vyandex transport")
		}
		if len(validUrls) > 1 {
			log.Printf("WARNING: vyandex uses only the first URL; %d additional URL(s) ignored", len(validUrls)-1)
		}
		inner = yandex.NewYandexVolgaTransport(validUrls[0], config)
	case "yandex":
		urls := strings.Split(globalDocUrls, ",")
		for i := range urls {
			urls[i] = strings.TrimSpace(urls[i])
		}
		// Filter out empty strings and invalid URLs
		var validUrls []string
		for _, u := range urls {
			if u == "" || u == "http://#" {
				continue
			}
			parsed, err := url.Parse(u)
			if err != nil || parsed.Scheme == "" || parsed.Host == "" {
				log.Printf("WARNING: skipping invalid URL: %q", u)
				continue
			}
			validUrls = append(validUrls, u)
		}
		if len(validUrls) == 0 {
			log.Fatalf("No valid URLs provided for yandex transport")
		}
		log.Printf("Yandex transport: %d document(s) configured", len(validUrls))
		inner = yandex.NewYandexDocsTransport(validUrls, config)
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		inner = oneme.NewOneMeTransport(*exitNode, maxToken, uidint, config)
	case "cupsonline":
		inner = cupsonline.NewCupsonlineTransport(globalDocUrls, config, !*exitNode)
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	if *encryptionKeyFile != "" {
		secretBytes, err := os.ReadFile(*encryptionKeyFile)
		if err != nil {
			log.Fatalf("Read encryption key file: %v", err)
		}
		context := *transportType
		if globalDocUrls != "" && globalDocUrls != "http://#" {
			context = globalDocUrls
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
