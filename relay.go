package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const clientTokenContext = "fricam-edge-client-v1"
const proxyTokenContext = "fricam-edge-proxy-v1:"

var identityRandomRead = rand.Read
var identityReadFile = os.ReadFile
var identityMkdirAll = os.MkdirAll
var identityWriteFile = os.WriteFile

const relayPongTimeout = 65 * time.Second

var relayPingInterval = 30 * time.Second

var relayConnectAttempt = func(relay *relayController, ctx context.Context) error { return relay.connect(ctx) }
var relayRetryDelay = time.Second

type edgeIdentity struct {
	DeviceID    string `json:"device_id"`
	RootSecret  string `json:"root_secret"`
	ClientToken string `json:"-"`
}

func deriveIdentity(root string) edgeIdentity {
	digest := sha256.Sum256([]byte(root))
	mac := hmac.New(sha256.New, []byte(root))
	_, _ = mac.Write([]byte(clientTokenContext))
	return edgeIdentity{
		DeviceID:    base64.RawURLEncoding.EncodeToString(digest[:]),
		RootSecret:  root,
		ClientToken: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	}
}

func loadOrCreateIdentity(path string) (edgeIdentity, error) {
	if raw, err := identityReadFile(path); err == nil {
		var stored edgeIdentity
		if json.Unmarshal(raw, &stored) == nil && stored.RootSecret != "" {
			return deriveIdentity(stored.RootSecret), nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return edgeIdentity{}, err
	}
	secret := make([]byte, 32)
	if _, err := identityRandomRead(secret); err != nil {
		return edgeIdentity{}, err
	}
	identity := deriveIdentity(base64.RawURLEncoding.EncodeToString(secret))
	if err := identityMkdirAll(filepath.Dir(path), 0700); err != nil {
		return edgeIdentity{}, err
	}
	raw, _ := json.Marshal(struct {
		DeviceID   string `json:"device_id"`
		RootSecret string `json:"root_secret"`
	}{identity.DeviceID, identity.RootSecret})
	if err := identityWriteFile(path, raw, 0600); err != nil {
		return edgeIdentity{}, err
	}
	return identity, nil
}

type relayController struct {
	relayURL     string
	go2rtcURL    string
	identity     edgeIdentity
	manager      *streamManager
	connected    atomic.Bool
	pingInterval time.Duration
	writeControl func(*websocket.Conn, int, []byte, time.Time) error

	mu       sync.Mutex
	conn     *websocket.Conn
	writeMu  sync.Mutex
	sessions map[string]*relayMediaSession
	proxies  map[string]context.CancelFunc
}

type relayEnvelope struct {
	Type      string          `json:"type"`
	SessionID string          `json:"sessionId,omitempty"`
	Camera    string          `json:"camera,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type relayMediaSession struct {
	id      string
	camera  string
	source  string
	ctx     context.Context
	conn    *websocket.Conn
	talk    *talkBridge
	view    *viewBridge
	cancel  context.CancelFunc
	writeMu sync.Mutex
}

const edgeMaxSignalBytes = 128 * 1024

var startTalkBridge = runTalkBridge
var startViewBridge = runViewBridge

func runTalkBridge(bridge *talkBridge) error { return bridge.start() }
func runViewBridge(bridge *viewBridge) error { return bridge.start() }

var localWebRTCUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func newRelayController(relayURL, go2rtcURL string, identity edgeIdentity, manager *streamManager) *relayController {
	return &relayController{
		relayURL: strings.TrimRight(relayURL, "/"), go2rtcURL: strings.TrimRight(go2rtcURL, "/"),
		identity: identity, manager: manager, sessions: make(map[string]*relayMediaSession),
		proxies:      make(map[string]context.CancelFunc),
		pingInterval: relayPingInterval, writeControl: (*websocket.Conn).WriteControl,
	}
}

func (r *relayController) httpRelayURL() string {
	return strings.NewReplacer("wss://", "https://", "ws://", "http://").Replace(r.relayURL)
}

func (r *relayController) run(ctx context.Context) {
	delay := relayRetryDelay
	for ctx.Err() == nil {
		if err := relayConnectAttempt(r, ctx); err != nil && ctx.Err() == nil {
			log.Printf("edge relay disconnected: %v", err)
		}
		if r.connected.Swap(false) {
			delay = relayRetryDelay
		}
		r.closeSessions()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
}

func (r *relayController) connect(ctx context.Context) error {
	endpoint := fmt.Sprintf("%s/edge/connect/%s", r.relayURL, r.identity.DeviceID)
	header := http.Header{
		"Authorization":         []string{"Bearer " + r.identity.RootSecret},
		"X-Fricam-Edge-Version": []string{version},
	}
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err != nil {
		if response != nil {
			return fmt.Errorf("relay returned %s", response.Status)
		}
		return err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(relayPongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(relayPongTimeout))
	})
	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()
	r.connected.Store(true)
	log.Printf("edge relay connected; device=%s", r.identity.DeviceID[:8])
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		ticker := time.NewTicker(r.pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.writeMu.Lock()
				err := r.writeControl(conn, websocket.PingMessage, nil, time.Now().Add(5*time.Second))
				r.writeMu.Unlock()
				if err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	for {
		messageType, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.TextMessage {
			continue
		}
		var message relayEnvelope
		if json.Unmarshal(raw, &message) != nil {
			continue
		}
		switch message.Type {
		case "session/open":
			r.openSession(ctx, message.SessionID, message.Camera)
		case "session/close":
			r.closeSession(message.SessionID)
		case "signal":
			r.writeLocalSignal(message.SessionID, message.Payload)
		case "proxy/message":
			r.openProxy(ctx, message.SessionID, message.Payload)
		case "proxy/close":
			r.closeProxy(message.SessionID)
		}
	}
}

type proxyRequest struct {
	Method   string            `json:"method"`
	Path     string            `json:"path"`
	Headers  map[string]string `json:"headers"`
	Body     string            `json:"body,omitempty"`
	IssuedAt int64             `json:"issuedAt"`
}

func validProxyPath(path string) bool {
	if !strings.HasPrefix(path, "/api/") || strings.Contains(path, "..") || len(path) > 2048 {
		return false
	}
	return !strings.HasPrefix(path, "/api/login") && !strings.HasPrefix(path, "/api/config/save")
}

func (r *relayController) openProxy(parent context.Context, id string, payload json.RawMessage) {
	var input proxyRequest
	plain, decryptErr := decryptProxyPayload(r.identity.ClientToken, payload)
	now := time.Now().Unix()
	if id == "" || decryptErr != nil || json.Unmarshal(plain, &input) != nil ||
		!validProxyPath(input.Path) || input.IssuedAt < now-60 || input.IssuedAt > now+60 {
		r.sendProxyControl(id, map[string]any{"type": "error", "status": 400})
		return
	}
	method := strings.ToUpper(input.Method)
	if method != http.MethodGet && method != http.MethodHead && method != http.MethodPost && method != http.MethodPut && method != http.MethodDelete {
		r.sendProxyControl(id, map[string]any{"type": "error", "status": 405})
		return
	}
	body, err := base64.RawStdEncoding.DecodeString(input.Body)
	if err != nil || len(body) > 256*1024 {
		r.sendProxyControl(id, map[string]any{"type": "error", "status": 413})
		return
	}
	r.closeProxy(id)
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	r.proxies[id] = cancel
	r.mu.Unlock()
	go r.runProxy(ctx, id, method, input.Path, input.Headers, body)
}

func (r *relayController) runProxy(ctx context.Context, id, method, path string, headers map[string]string, body []byte) {
	defer r.closeProxy(id)
	request, err := http.NewRequestWithContext(ctx, method, r.manager.cfg.frigateURL+path, strings.NewReader(string(body)))
	if err != nil {
		return
	}
	for _, name := range []string{"Accept", "Content-Type", "Range", "If-None-Match", "If-Modified-Since"} {
		if value := headers[name]; value != "" {
			request.Header.Set(name, value)
		}
	}
	response, err := r.manager.client.Do(request)
	if err != nil {
		r.sendProxyControl(id, map[string]any{"type": "error", "status": 502})
		return
	}
	defer response.Body.Close()
	r.sendProxyControl(id, map[string]any{"type": "response", "status": response.StatusCode, "headers": map[string]string{
		"Content-Type": response.Header.Get("Content-Type"), "Content-Length": response.Header.Get("Content-Length"),
		"Content-Range": response.Header.Get("Content-Range"), "ETag": response.Header.Get("ETag"),
	}})
	buffer := make([]byte, 256*1024)
	for {
		n, readErr := response.Body.Read(buffer)
		if n > 0 && !r.sendProxyBinary(id, buffer[:n]) {
			return
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return
		}
	}
	r.sendProxyControl(id, map[string]any{"type": "end"})
}

func proxyAEAD(clientToken string) (cipher.AEAD, error) {
	key := sha256.Sum256([]byte(proxyTokenContext + clientToken))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func encryptProxyBytes(clientToken string, plain []byte) ([]byte, error) {
	aead, err := proxyAEAD(clientToken)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plain, nil), nil
}

func decryptProxyPayload(clientToken string, raw json.RawMessage) ([]byte, error) {
	var encoded string
	if json.Unmarshal(raw, &encoded) != nil {
		return nil, errors.New("invalid encrypted proxy payload")
	}
	sealed, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	aead, err := proxyAEAD(clientToken)
	if err != nil || len(sealed) < aead.NonceSize() {
		return nil, errors.New("invalid encrypted proxy payload")
	}
	return aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], nil)
}

func (r *relayController) sendProxyControl(id string, value any) {
	plain, _ := json.Marshal(value)
	sealed, err := encryptProxyBytes(r.identity.ClientToken, plain)
	if err != nil {
		return
	}
	payload, _ := json.Marshal(base64.RawStdEncoding.EncodeToString(sealed))
	r.send(relayEnvelope{Type: "proxy/message", SessionID: id, Payload: payload})
}

func (r *relayController) sendProxyBinary(id string, payload []byte) bool {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil || len(id) != 36 {
		return false
	}
	sealed, err := encryptProxyBytes(r.identity.ClientToken, payload)
	if err != nil {
		return false
	}
	frame := make([]byte, 36+len(sealed))
	copy(frame, id)
	copy(frame[36:], sealed)
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, frame) == nil
}

func (r *relayController) closeProxy(id string) {
	r.mu.Lock()
	cancel := r.proxies[id]
	delete(r.proxies, id)
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *relayController) openSession(parent context.Context, id, camera string) {
	if id == "" || camera == "" {
		return
	}
	source, ok := r.manager.signalingSource(camera)
	if !ok {
		r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"camera unavailable"}`)})
		return
	}
	r.closeSession(id)
	ctx, cancel := context.WithCancel(parent)
	session := &relayMediaSession{id: id, camera: camera, source: source, ctx: ctx, cancel: cancel}
	r.mu.Lock()
	r.sessions[id] = session
	r.mu.Unlock()
}

func (r *relayController) openLocalSignaling(session *relayMediaSession) bool {
	query := url.Values{"src": []string{session.source}}
	endpoint := strings.NewReplacer("http://", "ws://", "https://", "wss://").Replace(r.go2rtcURL) +
		"/api/ws?" + query.Encode()
	conn, response, err := websocket.DefaultDialer.DialContext(session.ctx, endpoint, nil)
	if err != nil {
		if response != nil {
			log.Printf("go2rtc signaling for %s returned %s", session.camera, response.Status)
		}
		return false
	}
	session.conn = conn
	go r.readLocalSignals(session)
	return true
}

// signalingSource limits WebRTC access to configured camera streams and their
// conventional dedicated "_talk" backchannel stream. This lets the Android
// client select the same stream locally and remotely without turning Edge into
// an unrestricted proxy for every go2rtc producer.
func (m *streamManager) signalingSource(requested string) (string, bool) {
	if !validSignalingSource(requested) {
		return "", false
	}
	if stream := m.get(requested); stream != nil {
		return stream.sourceName(), true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	base := strings.TrimSuffix(requested, "_talk")
	for camera, stream := range m.streams {
		if requested == camera || (strings.HasSuffix(requested, "_talk") && base == camera) {
			return requested, true
		}
		for _, source := range stream.sourceNames {
			if requested == source || (strings.HasSuffix(requested, "_talk") && base == source) {
				return requested, true
			}
		}
	}
	return "", false
}

func validSignalingSource(value string) bool {
	if value == "" || len(value) > 128 || strings.ContainsAny(value, "/\\?#") {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func privateRemoteAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() {
		return true
	}
	// Tailscale and other CGNAT overlays use 100.64.0.0/10.
	return ip.To4() != nil && ip.To4()[0] == 100 && ip.To4()[1]&0xc0 == 0x40
}

func (r *relayController) authorizeLocalClient(request *http.Request) bool {
	const prefix = "Bearer "
	authorization := request.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, prefix) || !privateRemoteAddress(request.RemoteAddr) {
		return false
	}
	provided := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
	return len(provided) == len(r.identity.ClientToken) &&
		subtle.ConstantTimeCompare([]byte(provided), []byte(r.identity.ClientToken)) == 1
}

// serveLocalWebRTC is the LAN half of the unified Edge talk transport. It
// forwards only bounded, valid SDP/ICE JSON to loopback go2rtc; media remains
// encrypted WebRTC and never passes through this HTTP handler.
func (r *relayController) serveLocalWebRTC(w http.ResponseWriter, request *http.Request) {
	if !r.authorizeLocalClient(request) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	source, ok := r.manager.signalingSource(request.URL.Query().Get("src"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream unavailable"})
		return
	}
	query := url.Values{"src": []string{source}}
	endpoint := strings.NewReplacer("http://", "ws://", "https://", "wss://").Replace(r.go2rtcURL) +
		"/api/ws?" + query.Encode()
	local, response, err := websocket.DefaultDialer.DialContext(request.Context(), endpoint, nil)
	if err != nil {
		if response != nil {
			log.Printf("local Edge signaling returned %s", response.Status)
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "stream unavailable"})
		return
	}
	defer local.Close()
	client, err := localWebRTCUpgrader.Upgrade(w, request, http.Header{"X-Fricam-Edge": []string{"1"}})
	if err != nil {
		return
	}
	defer client.Close()
	client.SetReadLimit(edgeMaxSignalBytes)
	local.SetReadLimit(edgeMaxSignalBytes)
	errors := make(chan error, 2)
	go proxyWebRTCSignals(local, client, true, errors)
	go proxyWebRTCSignals(client, local, false, errors)
	<-errors
}

func proxyWebRTCSignals(destination, source *websocket.Conn, sanitize bool, result chan<- error) {
	for {
		messageType, payload, err := source.ReadMessage()
		if err != nil {
			result <- err
			return
		}
		if messageType != websocket.TextMessage || !json.Valid(payload) {
			result <- errors.New("invalid signaling message")
			return
		}
		if sanitize {
			var ok bool
			payload, ok = sanitizeSignalForGo2RTC(payload)
			if !ok {
				result <- errors.New("rejected signaling message")
				return
			}
		}
		if err := destination.WriteMessage(websocket.TextMessage, payload); err != nil {
			result <- err
			return
		}
	}
}

func (r *relayController) readLocalSignals(session *relayMediaSession) {
	defer r.closeSession(session.id)
	for {
		_, payload, err := session.conn.ReadMessage()
		if err != nil {
			return
		}
		if !json.Valid(payload) {
			continue
		}
		if kind, candidateType := signalMetadata(payload); kind != "" {
			log.Printf("edge signal go2rtc->client camera=%s type=%s candidate=%s", session.camera, kind, candidateType)
		}
		r.send(relayEnvelope{SessionID: session.id, Payload: json.RawMessage(payload)})
	}
}

func (r *relayController) writeLocalSignal(id string, payload json.RawMessage) {
	if !json.Valid(payload) {
		return
	}
	payload, ok := sanitizeSignalForGo2RTC(payload)
	if !ok {
		log.Printf("edge signal rejected camera session=%s", id)
		return
	}
	r.mu.Lock()
	session := r.sessions[id]
	r.mu.Unlock()
	if session != nil {
		if kind, candidateType := signalMetadata(payload); kind != "" {
			log.Printf("edge signal client->go2rtc camera=%s type=%s candidate=%s", session.camera, kind, candidateType)
		}
		session.writeMu.Lock()
		var control struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}
		if json.Unmarshal(payload, &control) == nil && session.view != nil {
			switch control.Type {
			case "edge/pause":
				session.view.setPaused(true)
				session.writeMu.Unlock()
				return
			case "edge/resume":
				session.view.setPaused(false)
				session.writeMu.Unlock()
				return
			}
		}
		if session.talk != nil {
			session.talk.addClientSignal(payload)
			session.writeMu.Unlock()
			return
		}
		if session.view != nil {
			session.view.addClientSignal(payload)
			session.writeMu.Unlock()
			return
		}
		if offer, ok := parseTalkBridgeOffer(payload); ok && strings.HasSuffix(session.source, "_talk") {
			bridge := newTalkBridge(session.ctx, r.go2rtcURL, session.source, offer, func(signal json.RawMessage) {
				r.send(relayEnvelope{SessionID: id, Payload: signal})
			})
			session.talk = bridge
			session.writeMu.Unlock()
			go func() {
				if err := startTalkBridge(bridge); err != nil {
					log.Printf("edge talk bridge camera=%s failed: %v", session.camera, err)
					r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"talk unavailable"}`)})
					r.closeSession(id)
					return
				}
				log.Printf("edge talk bridge connected camera=%s", session.camera)
			}()
			return
		}
		if offer, ok := parseViewBridgeOffer(payload); ok {
			bridge := newViewBridge(session.ctx, r.go2rtcURL, session.source, func() [][]byte {
				return r.manager.h264Bootstrap(session.source)
			}, offer, func(signal json.RawMessage) {
				r.send(relayEnvelope{SessionID: id, Payload: signal})
			})
			session.view = bridge
			session.writeMu.Unlock()
			go func() {
				if err := startViewBridge(bridge); err != nil {
					log.Printf("edge view bridge camera=%s failed: %v", session.camera, err)
					r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"stream unavailable"}`)})
					r.closeSession(id)
					return
				}
				log.Printf("edge view bridge connected camera=%s", session.camera)
			}()
			return
		}
		if session.conn == nil && !r.openLocalSignaling(session) {
			session.writeMu.Unlock()
			r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"stream unavailable"}`)})
			return
		}
		_ = writeWebSocketMessage(session.conn, websocket.TextMessage, payload)
		session.writeMu.Unlock()
	}
}

func signalMetadata(payload []byte) (string, string) {
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(payload, &signal) != nil || !strings.HasPrefix(signal.Type, "webrtc/") {
		if signal.Type != "webrtc" {
			return "", ""
		}
		var value struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(signal.Value, &value) != nil {
			return "", ""
		}
		return value.Type, "-"
	}
	candidateType := "-"
	if signal.Type == "webrtc/candidate" {
		var value string
		if json.Unmarshal(signal.Value, &value) != nil {
			return "", ""
		}
		fields := strings.Fields(value)
		for index := 0; index+1 < len(fields); index++ {
			if fields[index] == "typ" {
				candidateType = fields[index+1]
				break
			}
		}
	}
	return strings.TrimPrefix(signal.Type, "webrtc/"), candidateType
}

type edgeICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

var allowedCloudflareICEURLs = map[string]bool{
	"stun:stun.cloudflare.com:3478":                true,
	"stun:stun.cloudflare.com:53":                  true,
	"turn:turn.cloudflare.com:3478?transport=udp":  true,
	"turn:turn.cloudflare.com:3478?transport=tcp":  true,
	"turns:turn.cloudflare.com:5349?transport=tcp": true,
	"turn:turn.cloudflare.com:53?transport=udp":    true,
	"turn:turn.cloudflare.com:80?transport=tcp":    true,
	"turns:turn.cloudflare.com:443?transport=tcp":  true,
}

func sanitizeSignalForGo2RTC(payload []byte) ([]byte, bool) {
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(payload, &signal) != nil || signal.Type != "webrtc" {
		return payload, true
	}
	var offer struct {
		Type       string          `json:"type"`
		SDP        string          `json:"sdp"`
		ICEServers []edgeICEServer `json:"ice_servers"`
	}
	if json.Unmarshal(signal.Value, &offer) != nil || offer.Type != "offer" ||
		offer.SDP == "" || len(offer.SDP) > 96*1024 || len(offer.ICEServers) == 0 || len(offer.ICEServers) > 2 {
		return nil, false
	}
	for _, server := range offer.ICEServers {
		if len(server.URLs) == 0 || len(server.URLs) > 6 {
			return nil, false
		}
		for _, candidateURL := range server.URLs {
			if !allowedCloudflareICEURLs[candidateURL] {
				return nil, false
			}
		}
		if server.Username != "" || server.Credential != "" {
			if !isPrintableASCII(server.Username, 16, 128) || !isPrintableASCII(server.Credential, 16, 128) {
				return nil, false
			}
		}
	}
	clean, err := json.Marshal(struct {
		Type  string      `json:"type"`
		Value interface{} `json:"value"`
	}{Type: "webrtc", Value: offer})
	return clean, err == nil
}

func isPrintableASCII(value string, minimum, maximum int) bool {
	if len(value) < minimum || len(value) > maximum {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func (r *relayController) send(message relayEnvelope) {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	_ = conn.WriteJSON(message)
}

func (r *relayController) closeSession(id string) {
	r.mu.Lock()
	session := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()
	if session != nil {
		session.cancel()
		session.writeMu.Lock()
		if session.conn != nil {
			_ = session.conn.Close()
		}
		if session.talk != nil {
			packets, bytes := session.talk.forwardedMedia()
			log.Printf("edge talk bridge closed camera=%s packets=%d bytes=%d", session.camera, packets, bytes)
			session.talk.close()
		}
		if session.view != nil {
			session.view.close()
		}
		session.writeMu.Unlock()
	}
}

func (r *relayController) closeSessions() {
	r.mu.Lock()
	sessions := r.sessions
	proxies := r.proxies
	r.sessions = make(map[string]*relayMediaSession)
	r.proxies = make(map[string]context.CancelFunc)
	r.conn = nil
	r.mu.Unlock()
	for _, cancel := range proxies {
		cancel()
	}
	for _, session := range sessions {
		session.cancel()
		session.writeMu.Lock()
		if session.conn != nil {
			_ = session.conn.Close()
		}
		if session.talk != nil {
			packets, bytes := session.talk.forwardedMedia()
			log.Printf("edge talk bridge closed camera=%s packets=%d bytes=%d", session.camera, packets, bytes)
			session.talk.close()
		}
		if session.view != nil {
			packets, bytes := session.view.forwardedMedia()
			log.Printf("edge view bridge closed camera=%s packets=%d bytes=%d", session.camera, packets, bytes)
			session.view.close()
		}
		session.writeMu.Unlock()
	}
}
