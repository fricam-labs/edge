package main

import (
	"context"
	"encoding/json"
	"github.com/pion/webrtc/v4"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSafeWarmPlansNeverStartVideoTranscoding(t *testing.T) {
	var cfg frigateConfig
	if err := json.Unmarshal([]byte(`{"cameras":{"front":{"live":{"streams":{"HD":"front_webrtc","Grid":"front_sub"}}}},"go2rtc":{"streams":{"front":"rtsp://camera/main","front_webrtc":"ffmpeg:front#video=h264#hardware","front_sub":"rtsp://camera/sub"}}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	desired, plans := makeStreamPlans(cfg, "HD", "safe")
	if len(desired["front"]) != 2 || desired["front"][0] != "front_sub" {
		t.Fatalf("warm=%v", desired)
	}
	if plans["front"].Detail != "front_webrtc" {
		t.Fatal(plans)
	}
	cfg.Go2RTC.Streams["front_sub"] = json.RawMessage(`"ffmpeg:front#video=h264#width=640"`)
	desired, _ = makeStreamPlans(cfg, "HD", "safe")
	if len(desired["front"]) != 1 || desired["front"][0] != "front" {
		t.Fatal(desired)
	}
	delete(cfg.Go2RTC.Streams, "front")
	desired, plans = makeStreamPlans(cfg, "HD", "safe")
	if len(desired) != 0 || plans["front"].Reason == "" {
		t.Fatal(desired, plans)
	}
}

func TestCopyOnlySourceResolvesAliasesConservatively(t *testing.T) {
	streams := map[string]json.RawMessage{
		"native": json.RawMessage(`"rtsp://camera/live"`),
		"copy":   json.RawMessage(`"ffmpeg:native#video=copy"`),
		"encode": json.RawMessage(`"ffmpeg:native#video=h264"`),
		"hidden": json.RawMessage(`"rtsp://127.0.0.1:8554/encode"`),
		"cycle":  json.RawMessage(`"ffmpeg:cycle#video=copy"`),
		"mixed":  json.RawMessage(`["rtsp://camera/live","ffmpeg:native#video=h264"]`),
	}
	for name, want := range map[string]bool{"native": true, "copy": true, "encode": false, "hidden": false, "cycle": false, "mixed": false, "missing": false} {
		if got := copyOnlySource(name, streams, map[string]bool{}); got != want {
			t.Fatalf("%s=%v", name, got)
		}
	}
}

func TestHDLeaseSharesSourceAndReleasesOnce(t *testing.T) {
	m := newStreamManager(config{maxHDStreams: 1})
	a, ok := m.acquireHD("a")
	if !ok {
		t.Fatal("first lease")
	}
	b, ok := m.acquireHD("a")
	if !ok {
		t.Fatal("shared lease")
	}
	if _, ok = m.acquireHD("b"); ok {
		t.Fatal("budget exceeded")
	}
	a()
	a()
	if _, ok = m.acquireHD("b"); ok {
		t.Fatal("released other viewer")
	}
	b()
	c, ok := m.acquireHD("b")
	if !ok {
		t.Fatal("lease leaked")
	}
	c()
}

func TestProgressiveOfferSurvivesSanitizing(t *testing.T) {
	raw := []byte(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n","ice_servers":[{"urls":["stun:stun.cloudflare.com:3478"]}],"warm_paused":true,"progressive_video":true}}`)
	clean, ok := sanitizeSignalForGo2RTC(raw)
	if !ok {
		t.Fatal("rejected")
	}
	offer, ok := parseViewBridgeOffer(clean)
	if !ok || !offer.WarmPaused || !offer.Progressive {
		t.Fatalf("flags lost: %s", clean)
	}
}

func TestHDPreparationKeepsStartupAndPauseReleasesHD(t *testing.T) {
	old := connectViewLocal
	defer func() { connectViewLocal = old }()
	entered := make(chan struct{})
	ready := make(chan struct{})
	connectViewLocal = func(b *viewBridge, ctx context.Context, v, a *webrtc.TrackLocalStaticRTP, s string) (<-chan struct{}, error) {
		close(entered)
		return ready, nil
	}
	b := newViewBridge(context.Background(), "", "low", nil, viewBridgeOffer{Progressive: true}, func(json.RawMessage) {})
	defer b.close()
	b.manager = newStreamManager(config{maxHDStreams: 1})
	b.detailSource = "hd"
	b.hdVideo, _ = newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "video-hd", "hd")
	b.audio, _ = newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA}, "audio", "audio")
	b.video, _ = newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "video", "video")
	b.startHD()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("HD did not start")
	}
	if b.hdActive.Load() {
		t.Fatal("startup stopped before decoder ACK")
	}
	close(ready)
	b.addClientSignal([]byte(`{"type":"edge/quality-ready","value":""}`))
	if !b.hdActive.Load() {
		t.Fatal("ACK ignored")
	}
	b.setPaused(true)
	b.manager.mu.RLock()
	count := len(b.manager.hdUsers)
	b.manager.mu.RUnlock()
	if count != 0 || b.hdActive.Load() {
		t.Fatal("HD retained while paused")
	}
	b.addClientSignal([]byte(`{"type":"edge/quality-ready","value":""}`))
	if b.hdActive.Load() {
		t.Fatal("late decoder ACK reactivated paused HD")
	}
}

func TestProgressiveLANRequiresPairingAndConfiguredSource(t *testing.T) {
	identity := deriveIdentity("test-root")
	r := newRelayController("", "", identity, newStreamManager(config{}))
	for _, test := range []struct {
		token, address string
		status         int
	}{
		{"", "127.0.0.1:2", http.StatusUnauthorized},
		{identity.ClientToken, "8.8.8.8:2", http.StatusUnauthorized},
		{identity.ClientToken, "127.0.0.1:2", http.StatusNotFound},
	} {
		req := httptest.NewRequest(http.MethodGet, "http://localhost/webrtc/progressive?src=missing", nil)
		req.RemoteAddr = test.address
		req.Header.Set("Authorization", "Bearer "+test.token)
		response := httptest.NewRecorder()
		r.serveProgressive(response, req)
		if response.Code != test.status {
			t.Fatalf("got %d, want %d", response.Code, test.status)
		}
	}
}

func TestDemandPrewarmDoesNotOpenAnEncoderUntilResume(t *testing.T) {
	oldRemote, oldLocal := connectViewRemote, connectViewLocal
	defer func() { connectViewRemote, connectViewLocal = oldRemote, oldLocal }()
	connected := make(chan struct{})
	close(connected)
	connectViewRemote = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP) (<-chan struct{}, error) {
		return connected, nil
	}
	var opens atomic.Int32
	entered := make(chan struct{})
	connectViewLocal = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP, string) (<-chan struct{}, error) {
		opens.Add(1)
		close(entered)
		return connected, nil
	}
	b := newViewBridge(context.Background(), "", "encoded", nil, viewBridgeOffer{Progressive: true, WarmPaused: true}, func(json.RawMessage) {})
	b.manager = newStreamManager(config{maxHDStreams: 1})
	b.detailSource = "encoded"
	b.demandBase = true
	defer b.close()
	if err := b.start(); err != nil {
		t.Fatal(err)
	}
	if opens.Load() != 0 {
		t.Fatal("paused prewarm opened encoder")
	}
	b.setPaused(false)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("resume did not acquire source")
	}
	b.setPaused(true)
	b.manager.mu.RLock()
	count := len(b.manager.hdUsers)
	b.manager.mu.RUnlock()
	if count != 0 {
		t.Fatal("paused demand source retained its lease")
	}
}
