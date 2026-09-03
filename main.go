package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	packetSize       = 188
	historyMaxEvents = 8192
	historyKeep      = 4096
)

type config struct {
	listen            string
	frigateURL        string
	go2rtcURL         string
	preferredQuality  string
	discoveryInterval time.Duration
	maxCache          int
	idleTimeout       time.Duration
	relayURL          string
	identityFile      string
	frigateAuthURL    string
}

type packetEvent struct {
	seq  uint64
	data []byte
}

type streamMetrics struct {
	SourceStream  string `json:"source_stream"`
	Connected     bool   `json:"connected"`
	Codec         string `json:"codec"`
	CacheBytes    int    `json:"cache_bytes"`
	Keyframes     uint64 `json:"keyframes"`
	Reconnects    uint64 `json:"reconnects"`
	Clients       int64  `json:"clients"`
	LastPacketMS  *int64 `json:"last_packet_ms,omitempty"`
	KeyframeAgeMS *int64 `json:"keyframe_age_ms,omitempty"`
}

type streamCache struct {
	name        string
	sourceNames []string
	go2rtcURL   string
	maxCache    int
	idleTimeout time.Duration
	client      *http.Client
	ctx         context.Context
	cancel      context.CancelFunc

	mu           sync.RWMutex
	sourceIndex  int
	cache        []byte
	history      []packetEvent
	sequence     uint64
	connected    bool
	codec        string
	keyframes    uint64
	reconnects   uint64
	lastPacket   time.Time
	lastKeyframe time.Time
	wakeup       chan struct{}
	clients      atomic.Int64
}

type tsParser struct {
	pmtPID               uint16
	videoPID             uint16
	codec                string
	pat                  []byte
	pmt                  []byte
	candidate            []byte
	tail                 []byte
	currentPESIsKeyframe bool
}

type cameraConfig struct {
	Enabled *bool `json:"enabled"`
	Live    struct {
		Streams map[string]string `json:"streams"`
	} `json:"live"`
}

type frigateConfig struct {
	Cameras map[string]cameraConfig `json:"cameras"`
}

type streamManager struct {
	cfg     config
	client  *http.Client
	mu      sync.RWMutex
	streams map[string]*streamCache
}

type deadlineConn struct {
	net.Conn
	timeout time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func main() {
	cfg := loadConfig()
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		runHealthcheck(cfg.listen)
		return
	}

	manager := newStreamManager(cfg)
	if err := manager.sync(context.Background()); err != nil {
		log.Fatalf("initial Frigate camera discovery failed: %v", err)
	}
	go manager.refreshLoop(context.Background())
	var relay *relayController
	var pairing *pairingManager
	if cfg.relayURL != "" {
		identity, err := loadOrCreateIdentity(cfg.identityFile)
		if err != nil {
			log.Fatalf("edge identity: %v", err)
		}
		relay = newRelayController(cfg.relayURL, cfg.go2rtcURL, identity, manager)
		pairing = newPairingManager(identity)
		go relay.run(context.Background())
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		total, ready, connected := manager.counts()
		result := map[string]any{
			"status": "ok", "cameras": total, "ready": ready, "connected": connected,
		}
		if relay != nil {
			result["relay_connected"] = relay.connected.Load()
			// Public identifier only: lets paired clients detect a reinstalled
			// sidecar without exposing the root secret or client token.
			result["device_id"] = relay.identity.DeviceID
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, manager.metrics())
	})
	mux.HandleFunc("GET /stream/", func(w http.ResponseWriter, r *http.Request) {
		camera, ok := cameraFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		stream := manager.get(camera)
		if stream == nil {
			http.NotFound(w, r)
			return
		}
		stream.serve(w, r)
	})
	mux.HandleFunc("GET /webrtc", func(w http.ResponseWriter, request *http.Request) {
		if relay == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "relay disabled"})
			return
		}
		relay.serveLocalWebRTC(w, request)
	})
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		if relay == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "relay disabled"})
			return
		}
		if !validateFrigateAuthorization(r.Context(), cfg.frigateAuthURL, r.Header.Get("Authorization")) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"deviceId":    relay.identity.DeviceID,
			"clientToken": relay.identity.ClientToken,
			"relayUrl":    relay.httpRelayURL(),
		})
	})
	mux.HandleFunc("GET /pairing", func(w http.ResponseWriter, r *http.Request) {
		if pairing == nil {
			http.Error(w, "relay disabled", http.StatusServiceUnavailable)
			return
		}
		pairing.servePage(w, r)
	})
	mux.HandleFunc("POST /pair/claim", func(w http.ResponseWriter, r *http.Request) {
		if pairing == nil || relay == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "relay disabled"})
			return
		}
		if !validateFrigateAuthorization(r.Context(), cfg.frigateAuthURL, r.Header.Get("Authorization")) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		var request struct {
			Code string `json:"code"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || !pairing.claim(request.Code) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid or expired pairing code"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"deviceId":    relay.identity.DeviceID,
			"clientToken": relay.identity.ClientToken,
			"relayUrl":    relay.httpRelayURL(),
		})
	})

	server := &http.Server{
		Addr:              cfg.listen,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	log.Printf("fricam-edge listening on %s; auto-discovery=%s quality=%s", cfg.listen, cfg.discoveryInterval, cfg.preferredQuality)
	log.Fatal(server.ListenAndServe())
}

func cameraFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, "/stream/") || !strings.HasSuffix(path, ".ts") {
		return "", false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(path, "/stream/"), ".ts")
	if name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

func loadConfig() config {
	maxMiB := mustIntEnv("MAX_CACHE_MIB", 16, 1, 128)
	discoverySeconds := mustIntEnv("DISCOVERY_INTERVAL_SEC", 30, 5, 3600)
	idleSeconds := mustIntEnv("SOURCE_IDLE_TIMEOUT_SEC", 15, 5, 120)
	return config{
		listen:            env("LISTEN_ADDR", "127.0.0.1:8099"),
		frigateURL:        strings.TrimRight(env("FRIGATE_URL", "http://127.0.0.1:5000"), "/"),
		go2rtcURL:         strings.TrimRight(env("GO2RTC_URL", "http://127.0.0.1:1984"), "/"),
		preferredQuality:  env("LIVE_QUALITY", "HD"),
		discoveryInterval: time.Duration(discoverySeconds) * time.Second,
		maxCache:          maxMiB * 1024 * 1024,
		idleTimeout:       time.Duration(idleSeconds) * time.Second,
		relayURL:          strings.TrimRight(os.Getenv("EDGE_RELAY_URL"), "/"),
		identityFile:      env("EDGE_IDENTITY_FILE", "/data/identity.json"),
		frigateAuthURL:    strings.TrimRight(env("FRIGATE_AUTH_URL", "https://127.0.0.1:8971"), "/"),
	}
}

func validateFrigateAuthorization(ctx context.Context, authURL, authorization string) bool {
	if authorization == "" || (!strings.HasPrefix(authorization, "Bearer ") && !strings.HasPrefix(authorization, "Basic ")) {
		return false
	}
	parsedAuthURL, err := url.Parse(authURL)
	if err != nil || !isPrivateAuthHost(parsedAuthURL.Hostname()) {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL+"/api/config", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", authorization)
	if strings.HasPrefix(authorization, "Bearer ") {
		req.Header.Set("Cookie", "frigate_token="+strings.TrimPrefix(authorization, "Bearer "))
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// This connection never leaves the Frigate host. Port 8971 commonly uses
	// a locally generated certificate whose hostname cannot match loopback.
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} //nolint:gosec
	client := &http.Client{
		Transport: transport,
		Timeout:   3 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func isPrivateAuthHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func mustIntEnv(key string, fallback, min, max int) int {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < min || value > max {
		log.Fatalf("%s must be between %d and %d", key, min, max)
	}
	return value
}

func runHealthcheck(listen string) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		os.Exit(1)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health")
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Fricam-Edge", "1")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}

func newHTTPClient(idleTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &deadlineConn{Conn: conn, timeout: idleTimeout}, nil
		},
		MaxIdleConns:        32,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		MaxConnsPerHost:     0,
		MaxIdleConnsPerHost: 8,
	}
	return &http.Client{Transport: transport}
}

func newStreamManager(cfg config) *streamManager {
	return &streamManager{
		cfg: cfg, client: newHTTPClient(cfg.idleTimeout), streams: make(map[string]*streamCache),
	}
}

func (m *streamManager) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.discoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.sync(ctx); err != nil {
				log.Printf("camera discovery failed; keeping current streams: %v", err)
			}
		}
	}
}

func (m *streamManager) sync(ctx context.Context) error {
	desired, err := discoverCameraStreams(ctx, m.client, m.cfg.frigateURL, m.cfg.preferredQuality)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, stream := range m.streams {
		sources, exists := desired[name]
		if !exists || !equalStrings(stream.sourceNames, sources) {
			stream.stop()
			delete(m.streams, name)
			log.Printf("camera cache removed: %s", name)
		}
	}
	for name, sources := range desired {
		if _, exists := m.streams[name]; exists {
			continue
		}
		stream := newStreamCache(name, sources, m.cfg)
		m.streams[name] = stream
		stream.start()
		log.Printf("camera cache added: %s -> %s", name, strings.Join(sources, ","))
	}
	return nil
}

func (m *streamManager) get(name string) *streamCache {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streams[name]
}

func (m *streamManager) metrics() map[string]streamMetrics {
	m.mu.RLock()
	items := make(map[string]*streamCache, len(m.streams))
	for name, stream := range m.streams {
		items[name] = stream
	}
	m.mu.RUnlock()
	result := make(map[string]streamMetrics, len(items))
	for name, stream := range items {
		result[name] = stream.metrics()
	}
	return result
}

func (m *streamManager) counts() (total, ready, connected int) {
	metrics := m.metrics()
	for _, value := range metrics {
		total++
		if value.CacheBytes > 0 {
			ready++
		}
		if value.Connected {
			connected++
		}
	}
	return
}

func discoverCameraStreams(ctx context.Context, client *http.Client, frigateURL, preferredQuality string) (map[string][]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, frigateURL+"/api/config", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Frigate returned %s", resp.Status)
	}
	var cfg frigateConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, err
	}
	return selectCameraStreams(cfg, preferredQuality), nil
}

func selectCameraStreams(cfg frigateConfig, preferredQuality string) map[string][]string {
	result := make(map[string][]string)
	for name, camera := range cfg.Cameras {
		if camera.Enabled != nil && !*camera.Enabled {
			continue
		}
		var candidates []string
		for label, source := range camera.Live.Streams {
			if strings.EqualFold(label, preferredQuality) {
				candidates = appendUnique(candidates, source)
				break
			}
		}
		labels := make([]string, 0, len(camera.Live.Streams))
		for label := range camera.Live.Streams {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		for _, label := range labels {
			candidates = appendUnique(candidates, camera.Live.Streams[label])
		}
		candidates = appendUnique(candidates, name)
		result[name] = candidates
	}
	return result
}

func appendUnique(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func newStreamCache(name string, sources []string, cfg config) *streamCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &streamCache{
		name: name, sourceNames: append([]string(nil), sources...), go2rtcURL: cfg.go2rtcURL,
		maxCache: cfg.maxCache, idleTimeout: cfg.idleTimeout, client: newHTTPClient(cfg.idleTimeout),
		ctx: ctx, cancel: cancel, wakeup: make(chan struct{}),
	}
}

func (s *streamCache) start() {
	go s.run()
}

func (s *streamCache) stop() {
	s.cancel()
}

func (s *streamCache) sourceName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sourceNames[s.sourceIndex]
}

func (s *streamCache) run() {
	for {
		if err := s.consume(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("camera %s stream %s disconnected: %v", s.name, s.sourceName(), err)
		}
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		s.mu.Lock()
		s.connected = false
		s.reconnects++
		if len(s.sourceNames) > 1 {
			s.sourceIndex = (s.sourceIndex + 1) % len(s.sourceNames)
			s.history = s.history[:0]
			log.Printf("camera %s falling back to stream %s", s.name, s.sourceNames[s.sourceIndex])
		}
		s.signalLocked()
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *streamCache) consume() error {
	source := s.sourceName()
	query := url.Values{"src": []string{source}}
	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, s.go2rtcURL+"/api/stream.ts?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("go2rtc returned %s", resp.Status)
	}

	s.mu.Lock()
	s.connected = true
	s.signalLocked()
	s.mu.Unlock()

	parser := &tsParser{}
	packet := make([]byte, packetSize)
	for {
		if _, err := io.ReadFull(resp.Body, packet); err != nil {
			return err
		}
		copyPacket := append([]byte(nil), packet...)
		keyframe, codec, err := parser.feed(copyPacket)
		if err != nil {
			continue
		}
		now := time.Now()
		s.mu.Lock()
		s.lastPacket = now
		if codec != "" {
			s.codec = codec
		}
		if keyframe {
			s.cache = append(s.cache[:0], parser.candidate...)
			if len(s.cache) > s.maxCache {
				s.cache = append([]byte(nil), s.cache[len(s.cache)-s.maxCache:]...)
			}
			s.keyframes++
			s.lastKeyframe = now
		} else if len(s.cache) > 0 && len(s.cache)+packetSize <= s.maxCache {
			s.cache = append(s.cache, copyPacket...)
		}
		s.sequence++
		s.history = append(s.history, packetEvent{seq: s.sequence, data: copyPacket})
		if len(s.history) > historyMaxEvents {
			copy(s.history, s.history[len(s.history)-historyKeep:])
			s.history = s.history[:historyKeep]
		}
		// Wake HTTP consumers in small transport batches instead of once per
		// 188-byte packet. PUSI keeps PES/keyframe boundaries responsive while
		// the bounded fallback prevents sparse streams from stalling.
		if packet[1]&0x40 != 0 || s.sequence%32 == 0 {
			s.signalLocked()
		}
		s.mu.Unlock()
	}
}

func (s *streamCache) signalLocked() {
	close(s.wakeup)
	s.wakeup = make(chan struct{})
}

func (s *streamCache) metrics() streamMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := streamMetrics{
		SourceStream: s.sourceNames[s.sourceIndex], Connected: s.connected, Codec: s.codec,
		CacheBytes: len(s.cache), Keyframes: s.keyframes, Reconnects: s.reconnects,
		Clients: s.clients.Load(),
	}
	now := time.Now()
	if !s.lastPacket.IsZero() {
		value := now.Sub(s.lastPacket).Milliseconds()
		result.LastPacketMS = &value
	}
	if !s.lastKeyframe.IsZero() {
		value := now.Sub(s.lastKeyframe).Milliseconds()
		result.KeyframeAgeMS = &value
	}
	return result
}

func (s *streamCache) h264Bootstrap() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.codec != "h264" || len(s.cache) == 0 {
		return nil
	}
	return extractFirstVideoPES(s.cache)
}

// extractFirstVideoPES returns the Annex-B access unit at the start of the
// cached GOP. streamCache.cache always starts with PAT/PMT followed by an IDR
// PES, so this avoids waiting for the camera's next multi-second GOP remotely.
func extractFirstVideoPES(ts []byte) []byte {
	parser := &tsParser{}
	var accessUnit []byte
	started := false
	for offset := 0; offset+packetSize <= len(ts); offset += packetSize {
		packet := ts[offset : offset+packetSize]
		_, _, _ = parser.feed(packet)
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		if parser.videoPID == 0 || pid != parser.videoPID {
			continue
		}
		pusi := packet[1]&0x40 != 0
		if started && pusi {
			break
		}
		payload, ok := tsPayload(packet)
		if !ok {
			continue
		}
		if !started {
			if !pusi || len(payload) < 9 || payload[0] != 0 || payload[1] != 0 || payload[2] != 1 {
				continue
			}
			headerEnd := 9 + int(payload[8])
			if headerEnd >= len(payload) {
				continue
			}
			payload = payload[headerEnd:]
			started = true
		}
		accessUnit = append(accessUnit, payload...)
	}
	if !containsIDR(accessUnit, "h264") {
		return nil
	}
	return accessUnit
}

func (m *streamManager) h264Bootstrap(requested string) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for camera, stream := range m.streams {
		if requested == camera {
			return stream.h264Bootstrap()
		}
		for _, source := range stream.sourceNames {
			if requested == source {
				return stream.h264Bootstrap()
			}
		}
	}
	return nil
}

func (s *streamCache) serve(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	var snapshot []byte
	var cursor uint64
	var wakeup chan struct{}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for len(snapshot) == 0 {
		s.mu.RLock()
		snapshot = append(snapshot[:0], s.cache...)
		cursor = s.sequence
		wakeup = s.wakeup
		s.mu.RUnlock()
		if len(snapshot) > 0 {
			break
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			http.Error(w, "waiting for first keyframe", http.StatusServiceUnavailable)
			return
		case <-wakeup:
		}
	}
	s.clients.Add(1)
	defer s.clients.Add(-1)

	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Fricam-Bootstrap", "cached-gop")
	w.Header().Set("X-Fricam-Edge", "1")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(snapshot); err != nil {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-wakeup:
		case <-time.After(5 * time.Second):
		}
		s.mu.RLock()
		var packets []byte
		if len(s.history) > 0 {
			first := s.history[0].seq
			start := 0
			if cursor >= first {
				start = int(cursor-first) + 1
			}
			if start < len(s.history) {
				for _, event := range s.history[start:] {
					packets = append(packets, event.data...)
				}
				cursor = s.history[len(s.history)-1].seq
			}
		}
		wakeup = s.wakeup
		stale := !s.lastPacket.IsZero() && time.Since(s.lastPacket) > 20*time.Second
		disconnected := !s.connected
		s.mu.RUnlock()
		if len(packets) > 0 {
			if _, err := w.Write(packets); err != nil {
				return
			}
			flusher.Flush()
		} else if stale || disconnected {
			return
		}
	}
}

func (p *tsParser) feed(packet []byte) (bool, string, error) {
	if len(packet) != packetSize || packet[0] != 0x47 {
		return false, p.codec, errors.New("invalid MPEG-TS packet")
	}
	pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
	pusi := packet[1]&0x40 != 0
	payload, ok := tsPayload(packet)
	if !ok {
		return false, p.codec, nil
	}
	if pid == 0 {
		p.pat = append(p.pat[:0], packet...)
		if section, ok := psiSection(payload, pusi); ok {
			p.pmtPID = parsePAT(section)
		}
	}
	if p.pmtPID != 0 && pid == p.pmtPID {
		p.pmt = append(p.pmt[:0], packet...)
		if section, ok := psiSection(payload, pusi); ok {
			p.videoPID, p.codec = parsePMT(section)
		}
	}
	if p.videoPID == 0 || pid != p.videoPID {
		if len(p.candidate) > 0 {
			p.candidate = append(p.candidate, packet...)
		}
		return false, p.codec, nil
	}
	if pusi {
		p.candidate = append(append(p.candidate[:0], p.pat...), p.pmt...)
		p.tail = p.tail[:0]
		p.currentPESIsKeyframe = false
	}
	p.candidate = append(p.candidate, packet...)
	probe := append(append([]byte(nil), p.tail...), payload...)
	keyframe := !p.currentPESIsKeyframe && containsIDR(probe, p.codec)
	if keyframe {
		p.currentPESIsKeyframe = true
	}
	if len(probe) > 4 {
		p.tail = append(p.tail[:0], probe[len(probe)-4:]...)
	} else {
		p.tail = append(p.tail[:0], probe...)
	}
	return keyframe, p.codec, nil
}

func tsPayload(packet []byte) ([]byte, bool) {
	control := (packet[3] >> 4) & 0x3
	if control != 1 && control != 3 {
		return nil, false
	}
	offset := 4
	if control == 3 {
		offset += 1 + int(packet[4])
	}
	if offset >= len(packet) {
		return nil, false
	}
	return packet[offset:], true
}

func psiSection(payload []byte, pusi bool) ([]byte, bool) {
	if !pusi || len(payload) < 2 {
		return nil, false
	}
	offset := 1 + int(payload[0])
	if offset+3 > len(payload) {
		return nil, false
	}
	length := 3 + (int(payload[offset+1]&0x0f) << 8) + int(payload[offset+2])
	if offset+length > len(payload) {
		return nil, false
	}
	return payload[offset : offset+length], true
}

func parsePAT(section []byte) uint16 {
	if len(section) < 12 || section[0] != 0 {
		return 0
	}
	sectionLength := (int(section[1]&0x0f) << 8) | int(section[2])
	end := 3 + sectionLength - 4
	for i := 8; i+4 <= end && i+4 <= len(section); i += 4 {
		program := uint16(section[i])<<8 | uint16(section[i+1])
		if program != 0 {
			return uint16(section[i+2]&0x1f)<<8 | uint16(section[i+3])
		}
	}
	return 0
}

func parsePMT(section []byte) (uint16, string) {
	if len(section) < 16 || section[0] != 2 {
		return 0, ""
	}
	sectionLength := (int(section[1]&0x0f) << 8) | int(section[2])
	end := 3 + sectionLength - 4
	infoLength := (int(section[10]&0x0f) << 8) | int(section[11])
	for i := 12 + infoLength; i+5 <= end && i+5 <= len(section); {
		streamType := section[i]
		pid := uint16(section[i+1]&0x1f)<<8 | uint16(section[i+2])
		descriptorLength := (int(section[i+3]&0x0f) << 8) | int(section[i+4])
		switch streamType {
		case 0x1b:
			return pid, "h264"
		case 0x24:
			return pid, "h265"
		}
		i += 5 + descriptorLength
	}
	return 0, ""
}

func containsIDR(data []byte, codec string) bool {
	for i := 0; i+3 < len(data); i++ {
		start := -1
		if data[i] == 0 && data[i+1] == 0 && data[i+2] == 1 {
			start = i + 3
		} else if i+4 < len(data) && data[i] == 0 && data[i+1] == 0 && data[i+2] == 0 && data[i+3] == 1 {
			start = i + 4
		}
		if start < 0 || start >= len(data) {
			continue
		}
		switch codec {
		case "h264":
			if data[start]&0x1f == 5 {
				return true
			}
		case "h265":
			typeID := (data[start] >> 1) & 0x3f
			if typeID == 19 || typeID == 20 || typeID == 21 {
				return true
			}
		}
	}
	return false
}
