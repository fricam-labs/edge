package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

func boolPtr(value bool) *bool { return &value }

func TestContainsIDR(t *testing.T) {
	tests := []struct {
		name  string
		codec string
		data  []byte
		want  bool
	}{
		{"h264 IDR", "h264", []byte{0, 0, 1, 0x65}, true},
		{"h264 P-frame", "h264", []byte{0, 0, 1, 0x41}, false},
		{"h265 IDR", "h265", []byte{0, 0, 0, 1, 19 << 1}, true},
		{"h265 non-IDR", "h265", []byte{0, 0, 1, 1 << 1}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := containsIDR(test.data, test.codec); got != test.want {
				t.Fatalf("containsIDR() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCachedBootstrapIsNeverBorrowedFromAnotherQuality(t *testing.T) {
	if cachedSourceMatches("front_sub", "front", "front_main") {
		t.Fatal("sub request matched the main-stream cache")
	}
	if !cachedSourceMatches("front_main", "front", "front_main") {
		t.Fatal("main request did not match its cache")
	}
	if !cachedSourceMatches("front", "front", "front_main") {
		t.Fatal("canonical camera request did not match its configured cache")
	}
}

func TestH264RTPStartsIDR(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"single IDR", []byte{0x65, 0x01}, true},
		{"single P-frame", []byte{0x41, 0x01}, false},
		{"FU-A IDR start", []byte{0x7c, 0x85, 0x01}, true},
		{"FU-A IDR continuation", []byte{0x7c, 0x05, 0x01}, false},
		{"STAP-A containing IDR", []byte{0x78, 0, 2, 0x67, 0x01, 0, 2, 0x65, 0x01}, true},
		{"malformed STAP-A", []byte{0x78, 0, 8, 0x65}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := h264RTPStartsIDR(test.payload); got != test.want {
				t.Fatalf("h264RTPStartsIDR() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestEdgePeerConfigurationsKeepEdgeOnAllICE(t *testing.T) {
	servers := []edgeICEServer{{URLs: []string{"turns:turn.cloudflare.com:443?transport=tcp"}}}
	if got := edgeViewPeerConfiguration(servers).ICETransportPolicy; got != webrtc.ICETransportPolicyAll {
		t.Fatalf("view ICE policy = %v, want ALL", got)
	}
	if got := edgeTalkPeerConfiguration(servers).ICETransportPolicy; got != webrtc.ICETransportPolicyAll {
		t.Fatalf("talk ICE policy = %v, want ALL", got)
	}
}

func TestSelectCameraStreams(t *testing.T) {
	front := cameraConfig{Enabled: boolPtr(true)}
	front.Live.Streams = map[string]string{"Grid": "front_sub", "HD": "front_main"}
	garage := cameraConfig{Enabled: boolPtr(false)}
	garage.Live.Streams = map[string]string{"HD": "garage_main"}
	garden := cameraConfig{Enabled: boolPtr(true)}
	cfg := frigateConfig{Cameras: map[string]cameraConfig{
		"front": front, "garage": garage, "garden": garden,
	}}
	got := selectCameraStreams(cfg, "hd")
	if !equalStrings(got["front"], []string{"front_main", "front_sub", "front"}) {
		t.Fatalf("unexpected front candidates: %#v", got["front"])
	}
	if !equalStrings(got["garden"], []string{"garden"}) {
		t.Fatalf("unexpected garden candidates: %#v", got["garden"])
	}
	if _, exists := got["garage"]; exists {
		t.Fatal("disabled camera was selected")
	}
}

func TestTSPayloadWithoutAdaptation(t *testing.T) {
	packet := make([]byte, packetSize)
	packet[0] = 0x47
	packet[3] = 0x10
	payload, ok := tsPayload(packet)
	if !ok || len(payload) != packetSize-4 {
		t.Fatalf("unexpected payload: ok=%v len=%d", ok, len(payload))
	}
}

func TestCameraFromPath(t *testing.T) {
	name, ok := cameraFromPath("/stream/kapi_onu.ts")
	if !ok || name != "kapi_onu" {
		t.Fatalf("cameraFromPath() = %q, %v", name, ok)
	}
	for _, path := range []string{"/stream/.ts", "/stream/kapi_onu", "/stream/a/b.ts"} {
		if _, ok := cameraFromPath(path); ok {
			t.Fatalf("cameraFromPath(%q) unexpectedly succeeded", path)
		}
	}
}

func TestDeriveIdentityIsStableAndSeparated(t *testing.T) {
	identity := deriveIdentity("test-root-secret")
	if len(identity.DeviceID) != 43 || len(identity.ClientToken) != 43 {
		t.Fatalf("unexpected identity lengths: device=%d client=%d", len(identity.DeviceID), len(identity.ClientToken))
	}
	if identity.DeviceID == identity.ClientToken || identity.ClientToken == identity.RootSecret {
		t.Fatal("derived identity values are not separated")
	}
	if deriveIdentity(identity.RootSecret) != identity {
		t.Fatal("identity derivation is not stable")
	}
	if _, err := base64.RawURLEncoding.DecodeString(identity.DeviceID); err != nil {
		t.Fatalf("device id is not base64url: %v", err)
	}
}

func TestIdentityPersistsWithPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.json")
	first, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("persisted identity changed")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("identity permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestValidateFrigateAuthorizationStaysOnPrivateHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/config" || r.Header.Get("Authorization") != "Bearer secret" ||
			r.Header.Get("Cookie") != "frigate_token=secret" {
			http.Error(w, "bad auth forwarding", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if !validateFrigateAuthorization(context.Background(), server.URL, "Bearer secret") {
		t.Fatal("valid private Frigate authorization was rejected")
	}
	if validateFrigateAuthorization(context.Background(), "https://example.com", "Bearer secret") {
		t.Fatal("authorization could be forwarded to a public host")
	}
	if validateFrigateAuthorization(context.Background(), server.URL, "") {
		t.Fatal("empty authorization was accepted")
	}
}

func TestValidateFrigateAuthorizationRejectsRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/accepted", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	if validateFrigateAuthorization(context.Background(), server.URL, "Bearer secret") {
		t.Fatal("redirected authorization check was accepted")
	}
}

func TestPairingCodeIsSingleUseAndExpires(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	manager := newPairingManager(deriveIdentity("pairing-root"))
	manager.now = func() time.Time { return now }
	payload, err := manager.issue("http://192.168.1.20:8099")
	if err != nil {
		t.Fatal(err)
	}
	if payload.Type != "fricam-edge-pair" || payload.Version != 1 || payload.DeviceID != manager.identity.DeviceID {
		t.Fatalf("unexpected pairing payload: %#v", payload)
	}
	if !manager.claim(payload.Code) || manager.claim(payload.Code) {
		t.Fatal("pairing code was not exactly single-use")
	}

	expiring, err := manager.issue("http://192.168.1.20:8099")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(pairingCodeTTL + time.Second)
	if manager.claim(expiring.Code) {
		t.Fatal("expired pairing code was accepted")
	}
}

func TestPairingPageOriginMustBePrivate(t *testing.T) {
	privateRequest := httptest.NewRequest(http.MethodGet, "http://192.168.1.20:8099/pairing", nil)
	if origin, ok := pairingOrigin(privateRequest); !ok || origin != "http://192.168.1.20:8099" {
		t.Fatalf("private origin = %q, %v", origin, ok)
	}
	publicRequest := httptest.NewRequest(http.MethodGet, "https://edge.example.com/pairing", nil)
	if _, ok := pairingOrigin(publicRequest); ok {
		t.Fatal("public pairing origin was accepted")
	}
}

func TestPairingPageKeepsTechnicalIdentityOutOfTheUI(t *testing.T) {
	manager := newPairingManager(deriveIdentity("pairing-page-root"))
	request := httptest.NewRequest(http.MethodGet, "http://192.168.1.20:8099/pairing", nil)
	recorder := httptest.NewRecorder()
	manager.servePage(recorder, request)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, "Scan to connect") {
		t.Fatalf("pairing page status=%d", recorder.Code)
	}
	if strings.Contains(body, "Device ") || strings.Contains(body, manager.identity.DeviceID[:8]) {
		t.Fatal("pairing page exposed the technical device identifier")
	}
}

func TestSignalMetadataDoesNotExposeCandidateAddress(t *testing.T) {
	kind, candidateType := signalMetadata([]byte(`{"type":"webrtc/candidate","value":"candidate:1 1 udp 1 203.0.113.2 5000 typ relay"}`))
	if kind != "candidate" || candidateType != "relay" {
		t.Fatalf("signal metadata = %q, %q", kind, candidateType)
	}
	if kind, candidateType = signalMetadata([]byte(`{"type":"webrtc/offer","value":"secret-sdp"}`)); kind != "offer" || candidateType != "-" {
		t.Fatalf("offer metadata = %q, %q", kind, candidateType)
	}
	if kind, candidateType = signalMetadata([]byte(`{"type":"webrtc","value":{"type":"offer","sdp":"secret-sdp"}}`)); kind != "offer" || candidateType != "-" {
		t.Fatalf("v2 offer metadata = %q, %q", kind, candidateType)
	}
}

func TestV2OfferOnlyAllowsCloudflareICEServers(t *testing.T) {
	credential := strings.Repeat("a", 64)
	valid := []byte(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0","ice_servers":[{"urls":["stun:stun.cloudflare.com:3478"]},{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	if _, ok := sanitizeSignalForGo2RTC(valid); !ok {
		t.Fatal("valid Cloudflare V2 offer was rejected")
	}
	malicious := []byte(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0","ice_servers":[{"urls":["turn:192.168.1.1:3478"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	if _, ok := sanitizeSignalForGo2RTC(malicious); ok {
		t.Fatal("non-Cloudflare ICE server was accepted")
	}
}

func TestTalkBridgeOfferRequiresAudioOnlySendOnlyV2Offer(t *testing.T) {
	credential := strings.Repeat("a", 64)
	payload := []byte(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 8\r\na=sendonly\r\n","ice_servers":[{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	clean, ok := sanitizeSignalForGo2RTC(payload)
	if !ok {
		t.Fatal("valid talk offer was rejected by sanitizer")
	}
	if _, ok := parseTalkBridgeOffer(clean); !ok {
		t.Fatal("valid audio-only sendonly offer did not select the talk bridge")
	}

	for _, sdp := range []string{
		"v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 8\r\na=recvonly\r\n",
		"v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 8\r\na=sendonly\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
	} {
		candidate := []byte(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":%q,"ice_servers":[{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, sdp, credential, credential))
		if _, ok := parseTalkBridgeOffer(candidate); ok {
			t.Fatalf("non-talk SDP selected the bridge: %q", sdp)
		}
	}
}

func TestViewBridgeOfferRequiresVideo(t *testing.T) {
	credential := strings.Repeat("a", 64)
	payload := []byte(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 102\r\na=recvonly\r\n","warm_paused":true,"ice_servers":[{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	offer, ok := parseViewBridgeOffer(payload)
	if !ok {
		t.Fatal("valid video offer was rejected")
	}
	if !offer.WarmPaused {
		t.Fatal("warm-paused flag was not preserved")
	}
	audioOnly := []byte(strings.Replace(string(payload), "m=video", "m=audio", 1))
	if _, ok := parseViewBridgeOffer(audioOnly); ok {
		t.Fatal("audio-only offer was accepted as a view bridge")
	}
}

func TestSignalingSourceAllowsOnlyConfiguredStreamsAndTalkVariants(t *testing.T) {
	manager := &streamManager{streams: map[string]*streamCache{
		"front": {sourceNames: []string{"front_main", "front_sub"}},
	}}
	tests := map[string]string{
		"front":           "front_main",
		"front_main":      "front_main",
		"front_talk":      "front_talk",
		"front_main_talk": "front_main_talk",
	}
	for requested, expected := range tests {
		actual, ok := manager.signalingSource(requested)
		if !ok || actual != expected {
			t.Fatalf("signalingSource(%q) = %q, %v; want %q", requested, actual, ok, expected)
		}
	}
	for _, requested := range []string{"other", "other_talk", "front/main", "front?src=other", ""} {
		if source, ok := manager.signalingSource(requested); ok {
			t.Fatalf("signalingSource(%q) unexpectedly allowed %q", requested, source)
		}
	}
}

func TestLocalWebRTCAuthorizationRequiresPrivateNetworkAndClientToken(t *testing.T) {
	identity := deriveIdentity("local-talk-root")
	relay := &relayController{identity: identity}
	request := httptest.NewRequest(http.MethodGet, "http://192.168.1.20:8099/webrtc?src=front", nil)
	request.RemoteAddr = "192.168.1.50:4242"
	request.Header.Set("Authorization", "Bearer "+identity.ClientToken)
	if !relay.authorizeLocalClient(request) {
		t.Fatal("valid paired LAN client was rejected")
	}
	request.RemoteAddr = "100.100.10.20:4242"
	if !relay.authorizeLocalClient(request) {
		t.Fatal("valid paired Tailscale client was rejected")
	}
	request.RemoteAddr = "203.0.113.20:4242"
	if relay.authorizeLocalClient(request) {
		t.Fatal("public client was accepted")
	}
	request.RemoteAddr = "192.168.1.50:4242"
	request.Header.Set("Authorization", "Bearer wrong")
	if relay.authorizeLocalClient(request) {
		t.Fatal("invalid client token was accepted")
	}
}

func TestLocalWebRTCProxiesPairedSignalingToGo2RTC(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	go2rtc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/ws" || request.URL.Query().Get("src") != "front_talk" {
			http.Error(w, "wrong stream", http.StatusBadRequest)
			return
		}
		connection, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		messageType, payload, err := connection.ReadMessage()
		if err != nil || messageType != websocket.TextMessage || !strings.Contains(string(payload), "webrtc/offer") {
			return
		}
		_ = connection.WriteJSON(map[string]string{"type": "webrtc/answer", "value": "v=0"})
	}))
	defer go2rtc.Close()

	identity := deriveIdentity("proxy-talk-root")
	relay := &relayController{
		go2rtcURL: go2rtc.URL,
		identity:  identity,
		manager: &streamManager{streams: map[string]*streamCache{
			"front": {sourceNames: []string{"front_main"}},
		}},
	}
	edge := httptest.NewServer(http.HandlerFunc(relay.serveLocalWebRTC))
	defer edge.Close()

	endpoint := "ws" + strings.TrimPrefix(edge.URL, "http") + "/webrtc?src=front_talk"
	header := http.Header{"Authorization": []string{"Bearer " + identity.ClientToken}}
	client, response, err := websocket.DefaultDialer.Dial(endpoint, header)
	if err != nil {
		if response != nil {
			t.Fatalf("Edge WebSocket returned %s: %v", response.Status, err)
		}
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.WriteJSON(map[string]string{"type": "webrtc/offer", "value": "v=0"}); err != nil {
		t.Fatal(err)
	}
	var answer map[string]string
	if err := client.ReadJSON(&answer); err != nil {
		t.Fatal(err)
	}
	if answer["type"] != "webrtc/answer" || answer["value"] != "v=0" {
		t.Fatalf("unexpected answer: %#v", answer)
	}
}
