package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	if raw, err := os.ReadFile(path); err == nil {
		var stored edgeIdentity
		if json.Unmarshal(raw, &stored) == nil && stored.RootSecret != "" {
			return deriveIdentity(stored.RootSecret), nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return edgeIdentity{}, err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return edgeIdentity{}, err
	}
	identity := deriveIdentity(base64.RawURLEncoding.EncodeToString(secret))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return edgeIdentity{}, err
	}
	raw, err := json.Marshal(struct {
		DeviceID   string `json:"device_id"`
		RootSecret string `json:"root_secret"`
	}{identity.DeviceID, identity.RootSecret})
	if err != nil {
		return edgeIdentity{}, err
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		return edgeIdentity{}, err
	}
	return identity, nil
}

type relayController struct {
	relayURL  string
	go2rtcURL string
	identity  edgeIdentity
	manager   *streamManager
	connected atomic.Bool

	mu       sync.Mutex
	conn     *websocket.Conn
	writeMu  sync.Mutex
	sessions map[string]*relayMediaSession
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
	conn    *websocket.Conn
	cancel  context.CancelFunc
	writeMu sync.Mutex
}

func newRelayController(relayURL, go2rtcURL string, identity edgeIdentity, manager *streamManager) *relayController {
	return &relayController{
		relayURL: strings.TrimRight(relayURL, "/"), go2rtcURL: strings.TrimRight(go2rtcURL, "/"),
		identity: identity, manager: manager, sessions: make(map[string]*relayMediaSession),
	}
}

func (r *relayController) httpRelayURL() string {
	return strings.NewReplacer("wss://", "https://", "ws://", "http://").Replace(r.relayURL)
}

func (r *relayController) run(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		if err := r.connect(ctx); err != nil && ctx.Err() == nil {
			log.Printf("edge relay disconnected: %v", err)
		}
		if r.connected.Swap(false) {
			delay = time.Second
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
	header := http.Header{"Authorization": []string{"Bearer " + r.identity.RootSecret}}
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err != nil {
		if response != nil {
			return fmt.Errorf("relay returned %s", response.Status)
		}
		return err
	}
	defer conn.Close()
	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()
	r.connected.Store(true)
	log.Printf("edge relay connected; device=%s", r.identity.DeviceID[:8])
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
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
		}
	}
}

func (r *relayController) openSession(parent context.Context, id, camera string) {
	if id == "" || camera == "" {
		return
	}
	stream := r.manager.get(camera)
	if stream == nil {
		r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"camera unavailable"}`)})
		return
	}
	r.closeSession(id)
	ctx, cancel := context.WithCancel(parent)
	query := url.Values{"src": []string{stream.sourceName()}}
	endpoint := strings.NewReplacer("http://", "ws://", "https://", "wss://").Replace(r.go2rtcURL) +
		"/api/ws?" + query.Encode()
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		cancel()
		if response != nil {
			log.Printf("go2rtc signaling for %s returned %s", camera, response.Status)
		}
		r.send(relayEnvelope{SessionID: id, Payload: json.RawMessage(`{"type":"error","value":"stream unavailable"}`)})
		return
	}
	session := &relayMediaSession{id: id, camera: camera, conn: conn, cancel: cancel}
	r.mu.Lock()
	r.sessions[id] = session
	r.mu.Unlock()
	go r.readLocalSignals(session)
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
		_ = session.conn.WriteMessage(websocket.TextMessage, payload)
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
		_ = session.conn.Close()
		session.writeMu.Unlock()
	}
}

func (r *relayController) closeSessions() {
	r.mu.Lock()
	sessions := r.sessions
	r.sessions = make(map[string]*relayMediaSession)
	r.conn = nil
	r.mu.Unlock()
	for _, session := range sessions {
		session.cancel()
		session.writeMu.Lock()
		_ = session.conn.Close()
		session.writeMu.Unlock()
	}
}
