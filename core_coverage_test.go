package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/skip2/go-qrcode"
)

type deadlineTestConn struct {
	deadlineErr error
	readErr     error
}

func TestMediaAndSocketWriteErrors(t *testing.T) {
	want := errors.New("write")
	oldRTP, oldJSON := writeLocalRTP, writeWebSocketJSON
	defer func() { writeLocalRTP = oldRTP; writeWebSocketJSON = oldJSON }()
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	writeLocalRTP = func(*webrtc.TrackLocalStaticRTP, *rtp.Packet) error { return want }
	bridge := newViewBridge(context.Background(), "", "", func() [][]byte { return [][]byte{{0, 0, 1, 0x65}} }, viewBridgeOffer{}, func(json.RawMessage) {})
	if bridge.writeBootstrap(video) {
		t.Fatal("write accepted")
	}
	bridge.close()
	writeLocalRTP = oldRTP
	server, _ := fakeGo2RTCServer(t, false)
	writeWebSocketJSON = func(*websocket.Conn, any) error { return want }
	talk := newTalkBridge(context.Background(), server.URL, "x", talkBridgeOffer{}, func(json.RawMessage) {})
	if err := talk.connectLocal(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	talk.close()
	view := newViewBridge(context.Background(), server.URL, "x", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err := view.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	view.close()
}

func TestWaitHelpersAllResults(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	failed := make(chan error, 1)
	if err := waitContext(ready, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := waitPeer(ready, failed, context.Background()); err != nil {
		t.Fatal(err)
	}
	want := errors.New("failed")
	failed = make(chan error, 1)
	failed <- want
	if err := waitPeer(make(chan struct{}), failed, context.Background()); !errors.Is(err, want) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitContext(make(chan struct{}), ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := waitPeer(make(chan struct{}), make(chan error), ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestForwardVideoRTCPDeterministic(t *testing.T) {
	old := readSenderRTCP
	defer func() { readSenderRTCP = old }()
	calls := 0
	readSenderRTCP = func(*webrtc.RTPSender) ([]rtcp.Packet, interceptor.Attributes, error) {
		calls++
		switch calls {
		case 1:
			return []rtcp.Packet{&rtcp.ReceiverReport{}}, nil, nil
		case 2:
			return []rtcp.Packet{&rtcp.PictureLossIndication{}}, nil, nil
		case 3:
			return []rtcp.Packet{&rtcp.FullIntraRequest{}}, nil, nil
		default:
			return nil, nil, io.EOF
		}
	}
	bridge := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	api, _ := mediaAPI()
	bridge.local, _ = api.NewPeerConnection(webrtc.Configuration{})
	bridge.localVideoSSRC.Store(42)
	bridge.forwardVideoRTCP(nil)
	if calls != 4 {
		t.Fatal(calls)
	}
	bridge.close()
}

func TestCacheAppendAndExistingSync(t *testing.T) {
	packet := makeTSPacket(0x120, false, []byte{1})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(packet) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &streamCache{sourceNames: []string{"x"}, go2rtcURL: server.URL, maxCache: 1024, cache: []byte{1}, client: server.Client(), ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	if err := cache.consume(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(cache.cache) != 1+packetSize {
		t.Fatal(len(cache.cache))
	}
	configServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"cameras":{"front":{"live":{"streams":{"HD":"main"}}}}}`)
	}))
	defer configServer.Close()
	m := newStreamManager(config{frigateURL: configServer.URL, go2rtcURL: "http://127.0.0.1:1", preferredQuality: "HD", idleTimeout: time.Second, maxCache: 10})
	if err := m.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := m.get("front")
	if err := m.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.get("front") != first {
		t.Fatal("existing replaced")
	}
	first.stop()
}

func TestDefaultBridgeStarters(t *testing.T) {
	if err := runTalkBridge(newTalkBridge(context.Background(), "http://127.0.0.1:1", "x", talkBridgeOffer{}, func(json.RawMessage) {})); err == nil {
		t.Fatal("talk starter")
	}
	if err := runViewBridge(newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{SDP: "bad"}, func(json.RawMessage) {})); err == nil {
		t.Fatal("view starter")
	}
}

func TestViewStartPausedAndLiveFallback(t *testing.T) {
	for _, paused := range []bool{true, false} {
		t.Run(fmt.Sprint(paused), func(t *testing.T) {
			go2rtc, tracks := fakeGo2RTCServer(t, true)
			peer, sdp := videoOfferForTest(t)
			defer peer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			bridge := newViewBridge(ctx, go2rtc.URL, "front", nil, viewBridgeOffer{SDP: sdp, WarmPaused: paused}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
			done := make(chan error, 1)
			go func() { done <- bridge.start() }()
			if !paused {
				track := <-tracks
				for seq := uint16(1); ; seq++ {
					_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 4}, Payload: []byte{0x65}})
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
						bridge.close()
						return
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			bridge.close()
		})
	}
}

func TestForwardVideoRTCPPackets(t *testing.T) {
	peer, sdp := videoOfferForTest(t)
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newViewBridge(ctx, "", "", nil, viewBridgeOffer{SDP: sdp}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	connected, err := bridge.connectRemote(ctx, video, audio)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	bridge.localVideoSSRC.Store(42)
	localAPI, _ := mediaAPI()
	bridge.local, _ = localAPI.NewPeerConnection(webrtc.Configuration{})
	_ = peer.WriteRTCP([]rtcp.Packet{&rtcp.ReceiverReport{}, &rtcp.PictureLossIndication{MediaSSRC: 1}, &rtcp.FullIntraRequest{MediaSSRC: 1}})
	time.Sleep(30 * time.Millisecond)
	bridge.close()
}

func TestRelayConnectPingAndPong(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteControl(websocket.PongMessage, []byte("ok"), time.Now().Add(time.Second))
		time.Sleep(15 * time.Millisecond)
	}))
	defer server.Close()
	relay := newRelayController("ws"+strings.TrimPrefix(server.URL, "http"), "", edgeIdentity{DeviceID: "device01", RootSecret: "root"}, &streamManager{})
	relay.pingInterval = time.Millisecond
	if err := relay.connect(context.Background()); err == nil {
		t.Fatal("expected close")
	}
}

func TestRelayConnectPingWriteError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			defer conn.Close()
			_, _, _ = conn.ReadMessage()
		}
	}))
	defer server.Close()
	relay := newRelayController("ws"+strings.TrimPrefix(server.URL, "http"), "", edgeIdentity{DeviceID: "device01", RootSecret: "root"}, &streamManager{})
	relay.pingInterval = time.Millisecond
	relay.writeControl = func(*websocket.Conn, int, []byte, time.Time) error { return errors.New("ping") }
	if err := relay.connect(context.Background()); err == nil {
		t.Fatal("expected ping close")
	}
}

func TestRelayConnectPingStopsOnContext(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		<-serverDone
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	relay := newRelayController("ws"+strings.TrimPrefix(server.URL, "http"), "", edgeIdentity{DeviceID: "device01", RootSecret: "root"}, &streamManager{})
	done := make(chan error, 1)
	go func() { done <- relay.connect(ctx) }()
	for i := 0; i < 100 && !relay.connected.Load(); i++ {
		time.Sleep(time.Millisecond)
	}
	cancel()
	time.Sleep(5 * time.Millisecond)
	close(serverDone)
	if err := <-done; err == nil {
		t.Fatal("expected socket close")
	}
}

func audioOfferForTest(t *testing.T) (*webrtc.PeerConnection, string) {
	t.Helper()
	api, err := pcmaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTrack(track); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	g := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-g
	return peer, peer.LocalDescription().SDP
}
func videoOfferForTest(t *testing.T) (*webrtc.PeerConnection, string) {
	t.Helper()
	api, err := mediaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	g := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-g
	return peer, peer.LocalDescription().SDP
}

func TestPeerDescriptionOperationErrors(t *testing.T) {
	want := errors.New("description")
	oldRemote, oldAnswer, oldLocal := peerSetRemoteDescription, peerCreateAnswer, peerSetLocalDescription
	defer func() {
		peerSetRemoteDescription = oldRemote
		peerCreateAnswer = oldAnswer
		peerSetLocalDescription = oldLocal
	}()
	audioPeer, audioSDP := audioOfferForTest(t)
	defer audioPeer.Close()
	videoPeer, videoSDP := videoOfferForTest(t)
	defer videoPeer.Close()
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	talk := newTalkBridge(context.Background(), "", "", talkBridgeOffer{SDP: audioSDP}, func(json.RawMessage) {})
	view := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{SDP: videoSDP}, func(json.RawMessage) {})
	peerSetRemoteDescription = func(*webrtc.PeerConnection, webrtc.SessionDescription) error { return want }
	if err := talk.connectRemote(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectRemote(context.Background(), video, audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	peerSetRemoteDescription = oldRemote
	peerCreateAnswer = func(*webrtc.PeerConnection, *webrtc.AnswerOptions) (webrtc.SessionDescription, error) {
		return webrtc.SessionDescription{}, want
	}
	if err := talk.connectRemote(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectRemote(context.Background(), video, audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	peerCreateAnswer = oldAnswer
	peerSetLocalDescription = func(*webrtc.PeerConnection, webrtc.SessionDescription) error { return want }
	if err := talk.connectRemote(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectRemote(context.Background(), video, audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	server, _ := fakeGo2RTCServer(t, false)
	talk.go2rtcURL = server.URL
	view.go2rtcURL = server.URL
	if err := talk.connectLocal(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	talk.close()
	view.close()
}

func TestBridgeStartConnectionErrors(t *testing.T) {
	if err := newTalkBridge(context.Background(), "http://127.0.0.1:1", "x", talkBridgeOffer{}, func(json.RawMessage) {}).start(); err == nil || !strings.Contains(err.Error(), "local backchannel") {
		t.Fatal(err)
	}
	if err := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{SDP: "bad"}, func(json.RawMessage) {}).start(); err == nil || !strings.Contains(err.Error(), "remote WebRTC") {
		t.Fatal(err)
	}
	server, _ := fakeGo2RTCServer(t, false)
	talk := newTalkBridge(context.Background(), server.URL, "x", talkBridgeOffer{SDP: "bad"}, func(json.RawMessage) {})
	if err := talk.start(); err == nil || !strings.Contains(err.Error(), "remote WebRTC") {
		t.Fatal(err)
	}
	talk.close()
}

func TestRemainingStreamBranches(t *testing.T) {
	oldTimeout := firstKeyframeTimeout
	firstKeyframeTimeout = time.Millisecond
	defer func() { firstKeyframeTimeout = oldTimeout }()
	rec := httptest.NewRecorder()
	(&streamCache{sourceNames: []string{"x"}, wakeup: make(chan struct{})}).serve(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
	parser := &tsParser{videoPID: 1, codec: "h264"}
	packet := makeTSPacket(1, false, nil)
	packet[3] = 0x30
	packet[4] = 180
	if _, _, err := parser.feed(packet); err != nil {
		t.Fatal(err)
	}
	if len(parser.tail) > 4 {
		t.Fatal(parser.tail)
	}
	if _, ok := psiSection([]byte{0, 0, 0, 9}, true); ok {
		t.Fatal("oversize PSI")
	}
	base := testTransportStream()[:2*packetSize]
	noPayload := makeTSPacket(0x101, true, nil)
	noPayload[3] = 0x20
	short := makeTSPacket(0x101, true, []byte{0, 0, 1})
	longHeader := makeTSPacket(0x101, true, []byte{0, 0, 1, 0, 0, 0, 0, 0, 250})
	continuation := makeTSPacket(0x101, false, []byte{0, 0, 1, 0x65})
	for _, extra := range [][]byte{noPayload, short, longHeader, continuation} {
		ts := append(append([]byte{}, base...), extra...)
		if extractVideoAccessUnits(ts) != nil {
			t.Fatal("invalid extraction")
		}
	}
}

func TestRemainingRelayBranches(t *testing.T) {
	if validateFrigateAuthorization(context.Background(), "http://127.0.0.1:1", "Bearer x") {
		t.Fatal("unreachable auth accepted")
	}
	oldAttempt, oldDelay := relayConnectAttempt, relayRetryDelay
	defer func() { relayConnectAttempt = oldAttempt; relayRetryDelay = oldDelay }()
	relayRetryDelay = time.Millisecond
	calls := 0
	relayConnectAttempt = func(r *relayController, ctx context.Context) error {
		calls++
		if calls == 1 {
			r.connected.Store(true)
			return errors.New("first")
		}
		return errors.New("next")
	}
	r := newRelayController("", "", edgeIdentity{}, &streamManager{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r.run(ctx)
	if calls < 2 {
		t.Fatal(calls)
	}
	sourceClient, sourceServer, closeSource := websocketPair(t)
	defer closeSource()
	_, destServer, closeDest := websocketPair(t)
	destServer.Close()
	defer closeDest()
	result := make(chan error, 1)
	go proxyWebRTCSignals(destServer, sourceServer, false, result)
	_ = sourceClient.WriteMessage(websocket.TextMessage, []byte(`{"type":"x"}`))
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("write error missing")
	}
	client, server, closePair := websocketPair(t)
	defer closePair()
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	session := &relayMediaSession{id: "x", camera: "x", source: "x", ctx: sessionCtx, cancel: sessionCancel, conn: server}
	r.sessions["x"] = session
	r.writeLocalSignal("x", json.RawMessage(`{"type":"webrtc/offer","value":"v=0"}`))
	_, payload, err := client.ReadMessage()
	if err != nil || !json.Valid(payload) {
		t.Fatal(err, string(payload))
	}
	r.closeSessions()
}

func makeTSPacket(pid uint16, pusi bool, payload []byte) []byte {
	packet := make([]byte, packetSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0], packet[1], packet[2], packet[3] = 0x47, byte(pid>>8)&0x1f, byte(pid), 0x10
	if pusi {
		packet[1] |= 0x40
	}
	copy(packet[4:], payload)
	return packet
}

func testTransportStream() []byte {
	pat := []byte{0, 0, 0xb0, 17, 0, 1, 0, 0, 0, 0, 1, 0xe1, 0x00, 0, 0, 0, 0, 0, 0, 0, 0}
	pmt := []byte{0, 2, 0xb0, 18, 0, 1, 0, 0, 0, 0xe1, 0, 0xf0, 0, 0x1b, 0xe1, 0x01, 0xf0, 0, 0, 0, 0}
	pes := []byte{0, 0, 1, 0xe0, 0, 0, 0, 0, 0, 0, 0, 1, 0x65, 1}
	return append(append(makeTSPacket(0, true, pat), makeTSPacket(0x100, true, pmt)...), makeTSPacket(0x101, true, pes)...)
}

func TestTSParserAndExtractionEndToEnd(t *testing.T) {
	ts := testTransportStream()
	parser := &tsParser{}
	var keyframe bool
	for offset := 0; offset < len(ts); offset += packetSize {
		var err error
		keyframe, _, err = parser.feed(ts[offset : offset+packetSize])
		if err != nil {
			t.Fatal(err)
		}
	}
	if !keyframe || parser.codec != "h264" || parser.videoPID != 0x101 || len(parser.candidate) == 0 {
		t.Fatalf("parser=%#v keyframe=%v", parser, keyframe)
	}
	units := extractVideoAccessUnits(ts)
	if len(units) != 1 || !containsIDR(units[0], "h264") {
		t.Fatalf("units=%d", len(units))
	}
	cache := &streamCache{sourceNames: []string{"front"}, codec: "h264", cache: ts, wakeup: make(chan struct{})}
	if len(cache.h264Bootstrap()) != 1 {
		t.Fatal("missing cache bootstrap")
	}
	cache.codec = "h265"
	if cache.h264Bootstrap() != nil {
		t.Fatal("wrong codec bootstrap")
	}
	parser.currentPESIsKeyframe = true
	if key, _, err := parser.feed(makeTSPacket(0x120, false, []byte{1})); err != nil || key {
		t.Fatal(key, err)
	}
	noPayload := makeTSPacket(0x101, false, nil)
	noPayload[3] = 0x20
	if _, _, err := parser.feed(noPayload); err != nil {
		t.Fatal(err)
	}
}

func TestConsumeErrorTrimAndCacheLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	badURL := &streamCache{sourceNames: []string{"front"}, go2rtcURL: ":", client: http.DefaultClient, ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	if err := badURL.consume(); err == nil {
		t.Fatal("bad URL")
	}
	badClient := &streamCache{sourceNames: []string{"front"}, go2rtcURL: "http://local", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("dial") })}, ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	if err := badClient.consume(); err == nil {
		t.Fatal("dial")
	}
	body := append([]byte{}, testTransportStream()...)
	packet := makeTSPacket(0x120, false, []byte{1})
	for i := 0; i < historyMaxEvents+2; i++ {
		body = append(body, packet...)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
	defer server.Close()
	cache := &streamCache{name: "front", sourceNames: []string{"front"}, go2rtcURL: server.URL, maxCache: 200, client: server.Client(), ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	if err := cache.consume(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(cache.cache) != 200 || len(cache.history) > historyKeep+32 {
		t.Fatalf("cache=%d history=%d", len(cache.cache), len(cache.history))
	}
	invalidServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(make([]byte, packetSize)) }))
	defer invalidServer.Close()
	cache.go2rtcURL, cache.client = invalidServer.URL, invalidServer.Client()
	if err := cache.consume(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

type flakyFlusher struct {
	header                 http.Header
	writes, failAt, status int
}

func (w *flakyFlusher) Header() http.Header    { return w.header }
func (w *flakyFlusher) WriteHeader(status int) { w.status = status }
func (w *flakyFlusher) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return 0, errors.New("write")
	}
	return len(p), nil
}
func (w *flakyFlusher) Flush() {}

func TestStreamServeCancellationAndWriteErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	empty := &streamCache{sourceNames: []string{"front"}, wakeup: make(chan struct{})}
	empty.serve(&flakyFlusher{header: make(http.Header)}, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	for _, failAt := range []int{1, 2} {
		s := &streamCache{sourceNames: []string{"front"}, cache: []byte("cache"), sequence: 1, connected: true, wakeup: make(chan struct{}), history: []packetEvent{{seq: 2, data: []byte("live")}}}
		reqCtx, stop := context.WithCancel(context.Background())
		writer := &flakyFlusher{header: make(http.Header), failAt: failAt}
		done := make(chan struct{})
		go func() {
			s.serve(writer, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(reqCtx))
			close(done)
		}()
		if failAt == 2 {
			for i := 0; i < 100 && s.clients.Load() == 0; i++ {
				time.Sleep(time.Millisecond)
			}
			s.mu.Lock()
			s.sequence = 2
			s.signalLocked()
			s.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			stop()
			t.Fatal("serve stuck")
		}
		stop()
	}
	closedWakeup := make(chan struct{})
	close(closedWakeup)
	s := &streamCache{sourceNames: []string{"front"}, cache: []byte("cache"), sequence: 1, connected: false, wakeup: closedWakeup}
	s.serve(&flakyFlusher{header: make(http.Header)}, httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestBridgeFactoryErrorPaths(t *testing.T) {
	want := errors.New("factory")
	oldTrack, oldMedia, oldPCMA, oldPeer := newRTPTrack, mediaAPIFactory, pcmaAPIFactory, newPeerConnection
	defer func() {
		newRTPTrack = oldTrack
		mediaAPIFactory = oldMedia
		pcmaAPIFactory = oldPCMA
		newPeerConnection = oldPeer
	}()
	newRTPTrack = func(webrtc.RTPCodecCapability, string, string, ...func(*webrtc.TrackLocalStaticRTP)) (*webrtc.TrackLocalStaticRTP, error) {
		return nil, want
	}
	if err := newTalkBridge(context.Background(), "", "", talkBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	calls := 0
	newRTPTrack = func(capability webrtc.RTPCodecCapability, id, stream string, options ...func(*webrtc.TrackLocalStaticRTP)) (*webrtc.TrackLocalStaticRTP, error) {
		calls++
		if calls == 2 {
			return nil, want
		}
		return oldTrack(capability, id, stream, options...)
	}
	if err := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	newRTPTrack = oldTrack
	track, _ := oldTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "x", "x")
	video, _ := oldTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	pcmaAPIFactory = func() (*webrtc.API, error) { return nil, want }
	talk := newTalkBridge(context.Background(), "", "", talkBridgeOffer{}, func(json.RawMessage) {})
	if err := talk.connectLocal(context.Background(), track); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := talk.connectRemote(context.Background(), track); !errors.Is(err, want) {
		t.Fatal(err)
	}
	mediaAPIFactory = func() (*webrtc.API, error) { return nil, want }
	view := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err := view.connectRemote(context.Background(), video, track); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectLocal(context.Background(), video, track, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	pcmaAPIFactory, mediaAPIFactory = oldPCMA, oldMedia
	api, _ := oldMedia()
	newPeerConnection = func(*webrtc.API, webrtc.Configuration) (*webrtc.PeerConnection, error) { return nil, want }
	if _, err := view.connectRemote(context.Background(), video, track); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := view.connectLocal(context.Background(), video, track, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	api, _ = oldPCMA()
	_ = api
	if err := talk.connectLocal(context.Background(), track); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := talk.connectRemote(context.Background(), track); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestConsumeCachesTransportStream(t *testing.T) {
	ts := testTransportStream()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(ts) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &streamCache{name: "front", sourceNames: []string{"front"}, go2rtcURL: server.URL, maxCache: len(ts) * 2, idleTimeout: time.Second, client: server.Client(), ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	if err := cache.consume(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if cache.codec != "h264" || cache.keyframes != 1 || len(cache.cache) == 0 || cache.lastKeyframe.IsZero() {
		t.Fatalf("%#v", cache.metrics())
	}
}

func TestStreamRunReconnectAndStart(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "down", http.StatusBadGateway) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cache := &streamCache{name: "front", sourceNames: []string{"main", "sub"}, go2rtcURL: server.URL, client: server.Client(), ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	cache.start()
	for i := 0; i < 200; i++ {
		cache.mu.RLock()
		reconnected := cache.reconnects > 0
		cache.mu.RUnlock()
		if reconnected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cache.stop()
	for i := 0; i < 200; i++ {
		cache.mu.RLock()
		reconnects, index := cache.reconnects, cache.sourceIndex
		cache.mu.RUnlock()
		if reconnects > 0 && index == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("stream did not reconnect/fallback")
}

func TestRelayRunCancellation(t *testing.T) {
	r := newRelayController("ws://127.0.0.1:1", "", edgeIdentity{DeviceID: "device01", RootSecret: "root"}, &streamManager{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.run(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("relay run did not stop")
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	r.run(ctx)
}

func TestServeLocalWebRTCErrors(t *testing.T) {
	identity := deriveIdentity("root")
	r := newRelayController("", "http://127.0.0.1:1", identity, &streamManager{streams: map[string]*streamCache{"front": {sourceNames: []string{"front"}}}})
	req := httptest.NewRequest(http.MethodGet, "http://localhost/webrtc?src=front", nil)
	req.RemoteAddr = "127.0.0.1:2"
	recorder := httptest.NewRecorder()
	r.serveLocalWebRTC(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatal(recorder.Code)
	}
	req.Header.Set("Authorization", "Bearer "+identity.ClientToken)
	req.URL.RawQuery = "src=missing"
	recorder = httptest.NewRecorder()
	r.serveLocalWebRTC(recorder, req)
	if recorder.Code != http.StatusNotFound {
		t.Fatal(recorder.Code)
	}
	req.URL.RawQuery = "src=front"
	recorder = httptest.NewRecorder()
	r.serveLocalWebRTC(recorder, req)
	if recorder.Code != http.StatusBadGateway {
		t.Fatal(recorder.Code)
	}
}

func TestServeLocalWebRTCUpstreamStatusAndUpgradeError(t *testing.T) {
	identity := deriveIdentity("root")
	manager := &streamManager{streams: map[string]*streamCache{"front": {sourceNames: []string{"front"}}}}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad", http.StatusBadGateway)
	}))
	r := newRelayController("", bad.URL, identity, manager)
	req := httptest.NewRequest(http.MethodGet, "http://localhost/webrtc?src=front", nil)
	req.RemoteAddr = "127.0.0.1:2"
	req.Header.Set("Authorization", "Bearer "+identity.ClientToken)
	r.serveLocalWebRTC(httptest.NewRecorder(), req)
	bad.Close()

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err == nil {
			defer conn.Close()
			_, _, _ = conn.ReadMessage()
		}
	}))
	defer upstream.Close()
	r.go2rtcURL = upstream.URL
	r.serveLocalWebRTC(httptest.NewRecorder(), req)
}

func TestWriteLocalSignalControlsAndExistingBridges(t *testing.T) {
	r := newRelayController("", "", deriveIdentity("root"), &streamManager{})
	ctx, cancel := context.WithCancel(context.Background())
	view := newViewBridge(ctx, "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	s := &relayMediaSession{id: "view", camera: "front", ctx: ctx, cancel: cancel, view: view}
	r.sessions["view"] = s
	r.writeLocalSignal("view", json.RawMessage(`{"type":"edge/pause","value":""}`))
	if !view.paused.Load() {
		t.Fatal("not paused")
	}
	r.writeLocalSignal("view", json.RawMessage(`{"type":"edge/resume","value":""}`))
	if view.paused.Load() {
		t.Fatal("not resumed")
	}
	r.writeLocalSignal("view", json.RawMessage(`{"type":"webrtc/candidate","value":"candidate:x"}`))
	if len(view.pendingCandidates) != 1 {
		t.Fatal("candidate")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	talk := newTalkBridge(ctx2, "", "", talkBridgeOffer{}, func(json.RawMessage) {})
	r.sessions["talk"] = &relayMediaSession{id: "talk", camera: "front", ctx: ctx2, cancel: cancel2, talk: talk}
	r.writeLocalSignal("talk", json.RawMessage(`{"type":"webrtc/candidate","value":"candidate:x"}`))
	if len(talk.pendingCandidates) != 1 {
		t.Fatal("talk candidate")
	}
	r.closeSession("view")
	r.closeSession("talk")
}

func (c *deadlineTestConn) Read([]byte) (int, error)         { return 7, c.readErr }
func (c *deadlineTestConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *deadlineTestConn) Close() error                     { return nil }
func (c *deadlineTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *deadlineTestConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *deadlineTestConn) SetDeadline(time.Time) error      { return nil }
func (c *deadlineTestConn) SetReadDeadline(time.Time) error  { return c.deadlineErr }
func (c *deadlineTestConn) SetWriteDeadline(time.Time) error { return nil }

func TestDeadlineConn(t *testing.T) {
	want := errors.New("deadline")
	c := &deadlineConn{Conn: &deadlineTestConn{deadlineErr: want}, timeout: time.Second}
	if _, err := c.Read(nil); !errors.Is(err, want) {
		t.Fatalf("Read error = %v", err)
	}
	want = errors.New("read")
	c.Conn = &deadlineTestConn{readErr: want}
	if n, err := c.Read(nil); n != 7 || !errors.Is(err, want) {
		t.Fatalf("Read = %d, %v", n, err)
	}
}

func TestConfigurationHelpers(t *testing.T) {
	t.Setenv("EDGE_RELAY_URL", "wss://relay.example/")
	t.Setenv("LISTEN_ADDR", "127.0.0.1:9000")
	t.Setenv("MAX_CACHE_MIB", "2")
	t.Setenv("DISCOVERY_INTERVAL_SEC", "6")
	t.Setenv("SOURCE_IDLE_TIMEOUT_SEC", "16")
	cfg := loadConfig()
	if cfg.listen != "127.0.0.1:9000" || cfg.maxCache != 2*1024*1024 || cfg.relayURL != "wss://relay.example" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	os.Unsetenv("UNIT_TEST_ENV")
	if got := env("UNIT_TEST_ENV", "fallback"); got != "fallback" {
		t.Fatal(got)
	}
	t.Setenv("UNIT_TEST_ENV", "value")
	if got := env("UNIT_TEST_ENV", "fallback"); got != "value" {
		t.Fatal(got)
	}
	if got := mustIntEnv("MAX_CACHE_MIB", 1, 1, 3); got != 2 {
		t.Fatal(got)
	}
}

func TestPrivateHostsAndAddresses(t *testing.T) {
	for _, host := range []string{"localhost", "LOCALHOST", "127.0.0.1", "10.0.0.1", "::1"} {
		if !isPrivateAuthHost(host) {
			t.Errorf("private host rejected: %s", host)
		}
	}
	for _, host := range []string{"example.com", "203.0.113.1"} {
		if isPrivateAuthHost(host) {
			t.Errorf("public host accepted: %s", host)
		}
	}
	for _, address := range []string{"127.0.0.1:2", "10.0.0.1:2", "[::1]:2", "100.64.0.1:2", "100.127.255.255"} {
		if !privateRemoteAddress(address) {
			t.Errorf("private address rejected: %s", address)
		}
	}
	for _, address := range []string{"invalid", "203.0.113.1:2", "100.128.0.1:2"} {
		if privateRemoteAddress(address) {
			t.Errorf("public address accepted: %s", address)
		}
	}
}

func TestJSONAndHTTPClient(t *testing.T) {
	r := httptest.NewRecorder()
	writeJSON(r, http.StatusCreated, map[string]string{"ok": "yes"})
	if r.Code != http.StatusCreated || r.Header().Get("X-Fricam-Edge") != "1" || !strings.Contains(r.Body.String(), `"ok":"yes"`) {
		t.Fatalf("unexpected response: %#v %s", r.Result().Header, r.Body.String())
	}
	if newHTTPClient(time.Second).Transport == nil {
		t.Fatal("missing transport")
	}
}

func TestDiscoverCameraStreams(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("case") {
		case "status":
			http.Error(w, "no", http.StatusBadGateway)
		case "json":
			_, _ = io.WriteString(w, "{")
		default:
			_, _ = io.WriteString(w, `{"cameras":{"front":{"live":{"streams":{"HD":"front_main"}}}}}`)
		}
	}))
	defer server.Close()
	got, err := discoverCameraStreams(context.Background(), server.Client(), server.URL, "HD")
	if err != nil || got["front"][0] != "front_main" {
		t.Fatalf("got=%v err=%v", got, err)
	}
	for _, response := range []*http.Response{
		{StatusCode: http.StatusBadGateway, Status: "502 Bad Gateway", Body: io.NopCloser(strings.NewReader("no"))},
		{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{"))},
	} {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return response, nil })}
		if _, err := discoverCameraStreams(context.Background(), client, server.URL, "HD"); err == nil {
			t.Fatal("expected error")
		}
	}
	badClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("dial") })}
	if _, err := discoverCameraStreams(context.Background(), badClient, server.URL, "HD"); err == nil {
		t.Fatal("expected transport error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCollectionHelpers(t *testing.T) {
	values := appendUnique(nil, "")
	values = appendUnique(values, "a")
	values = appendUnique(values, "a")
	if !equalStrings(values, []string{"a"}) || equalStrings(values, nil) || equalStrings(values, []string{"b"}) {
		t.Fatal(values)
	}
}

func TestStreamCacheStateAndMetrics(t *testing.T) {
	cfg := config{go2rtcURL: "http://127.0.0.1", maxCache: 1024, idleTimeout: time.Second}
	s := newStreamCache("front", []string{"front_main"}, cfg)
	defer s.stop()
	if s.sourceName() != "front_main" {
		t.Fatal(s.sourceName())
	}
	s.mu.Lock()
	s.connected, s.codec, s.cache = true, "h264", []byte{1}
	s.keyframes, s.reconnects = 2, 3
	s.lastPacket, s.lastKeyframe = time.Now(), time.Now()
	oldWakeup := s.wakeup
	s.signalLocked()
	s.mu.Unlock()
	select {
	case <-oldWakeup:
	default:
		t.Fatal("wakeup not signaled")
	}
	m := s.metrics()
	if !m.Connected || m.Codec != "h264" || m.LastPacketMS == nil || m.KeyframeAgeMS == nil {
		t.Fatalf("%#v", m)
	}
	manager := &streamManager{streams: map[string]*streamCache{"front": s}}
	if total, ready, connected := manager.counts(); total != 1 || ready != 1 || connected != 1 {
		t.Fatalf("%d %d %d", total, ready, connected)
	}
	if manager.get("front") != s || manager.metrics()["front"].Keyframes != 2 {
		t.Fatal("manager state")
	}
	if manager.h264Bootstrap("missing") != nil {
		t.Fatal("unexpected bootstrap")
	}
}

func TestTSHelpers(t *testing.T) {
	bad := make([]byte, packetSize)
	if _, _, err := (&tsParser{}).feed(bad); err == nil {
		t.Fatal("invalid packet accepted")
	}
	packet := make([]byte, packetSize)
	packet[0], packet[3] = 0x47, 0x20
	if _, ok := tsPayload(packet); ok {
		t.Fatal("payload accepted")
	}
	packet[3], packet[4] = 0x30, 250
	if _, ok := tsPayload(packet); ok {
		t.Fatal("adaptation overflow accepted")
	}
	for _, test := range []struct {
		payload  []byte
		pusi, ok bool
	}{
		{[]byte{0}, false, false}, {[]byte{0}, true, false},
		{[]byte{0, 0, 0, 1, 0}, true, true}, {[]byte{9, 0, 1, 0}, true, false},
	} {
		_, ok := psiSection(test.payload, test.pusi)
		if ok != test.ok {
			t.Errorf("psiSection(%v)=%v", test.payload, ok)
		}
	}
	pat := []byte{0, 0xb0, 17, 0, 1, 0, 0, 0, 0, 1, 0xe1, 0x00, 0, 0, 0, 0, 0, 0, 0, 0}
	if got := parsePAT(pat); got != 0x100 {
		t.Fatalf("PAT=%x", got)
	}
	if parsePAT(nil) != 0 || parsePAT([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) != 0 {
		t.Fatal("invalid PAT")
	}
	pmt := []byte{2, 0xb0, 18, 0, 1, 0, 0, 0, 0xe1, 0, 0xf0, 0, 0x1b, 0xe1, 0x01, 0xf0, 0, 0, 0, 0, 0}
	if pid, codec := parsePMT(pmt); pid != 0x101 || codec != "h264" {
		t.Fatalf("PMT=%x %s", pid, codec)
	}
	pmt[12] = 0x24
	if _, codec := parsePMT(pmt); codec != "h265" {
		t.Fatal(codec)
	}
	if pid, codec := parsePMT(nil); pid != 0 || codec != "" {
		t.Fatal("invalid PMT")
	}
	if got := extractVideoAccessUnits(make([]byte, packetSize-1)); got != nil {
		t.Fatal(got)
	}
}

func TestRelayValidationHelpers(t *testing.T) {
	r := newRelayController("wss://relay.example/", "http://go2rtc/", deriveIdentity("root"), &streamManager{})
	if r.httpRelayURL() != "https://relay.example" || len(r.sessions) != 0 {
		t.Fatal("relay constructor")
	}
	for _, value := range []string{"front", strings.Repeat("a", 128)} {
		if !validSignalingSource(value) {
			t.Errorf("rejected %q", value)
		}
	}
	for _, value := range []string{"", strings.Repeat("a", 129), "a/b", "a b", "é"} {
		if validSignalingSource(value) {
			t.Errorf("accepted %q", value)
		}
	}
	for _, test := range []struct {
		value    string
		min, max int
		want     bool
	}{
		{"abcd", 4, 4, true}, {"abc", 4, 5, false}, {"abcdef", 1, 5, false}, {"a b", 1, 5, false},
	} {
		if got := isPrintableASCII(test.value, test.min, test.max); got != test.want {
			t.Errorf("printable %q=%v", test.value, got)
		}
	}
}

func TestSignalSanitizingAndMetadataEdges(t *testing.T) {
	plain := []byte(`{"type":"webrtc/offer","value":"x"}`)
	if got, ok := sanitizeSignalForGo2RTC(plain); !ok || string(got) != string(plain) {
		t.Fatal("plain signal")
	}
	for _, payload := range [][]byte{
		[]byte(`{"type":"webrtc","value":{}}`),
		[]byte(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0","ice_servers":[]}}`),
		[]byte(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0","ice_servers":[{"urls":[]}]}}`),
	} {
		if _, ok := sanitizeSignalForGo2RTC(payload); ok {
			t.Errorf("accepted %s", payload)
		}
	}
	for _, payload := range [][]byte{[]byte("{"), []byte(`{"type":"x"}`), []byte(`{"type":"webrtc","value":{}}`), []byte(`{"type":"webrtc/candidate","value":3}`)} {
		if kind, _ := signalMetadata(payload); kind != "" {
			t.Errorf("metadata %s=%s", payload, kind)
		}
	}
}

func TestOfferParsersAndBridgeState(t *testing.T) {
	for _, payload := range [][]byte{[]byte("{"), []byte(`{"type":"x"}`), []byte(`{"type":"webrtc","value":{}}`)} {
		if _, ok := parseTalkBridgeOffer(payload); ok {
			t.Fatal("talk accepted")
		}
		if _, ok := parseViewBridgeOffer(payload); ok {
			t.Fatal("view accepted")
		}
	}
	if audioSendOnlySDP("m=audio 9\na=recvonly") || audioSendOnlySDP("a=sendonly") {
		t.Fatal("invalid audio SDP")
	}
	talk := newTalkBridge(context.Background(), "http://go2rtc", "front_talk", talkBridgeOffer{}, func(json.RawMessage) {})
	if talk.source != "front_talk" {
		t.Fatal("talk bridge")
	}
	talk.packetsForwarded.Store(2)
	talk.bytesForwarded.Store(3)
	if p, b := talk.forwardedMedia(); p != 2 || b != 3 {
		t.Fatal(p, b)
	}
	talk.addClientSignal([]byte("{"))
	talk.addClientSignal([]byte(`{"type":"x"}`))
	talk.addClientSignal([]byte(`{"type":"webrtc/candidate","value":""}`))
	talk.addClientSignal([]byte(`{"type":"webrtc/candidate","value":"a=candidate:x"}`))
	if len(talk.pendingCandidates) != 1 {
		t.Fatal(talk.pendingCandidates)
	}
	talk.sendSignal("x", "y")
	talk.close()
	talk.close()

	view := newViewBridge(context.Background(), "http://go2rtc", "front", nil, viewBridgeOffer{WarmPaused: true}, func(json.RawMessage) {})
	if !view.paused.Load() {
		t.Fatal("warm pause")
	}
	view.packetsForwarded.Store(4)
	view.bytesForwarded.Store(5)
	if p, b := view.forwardedMedia(); p != 4 || b != 5 {
		t.Fatal(p, b)
	}
	view.addClientSignal([]byte("{"))
	view.addClientSignal([]byte(`{"type":"x"}`))
	view.addClientSignal([]byte(`{"type":"webrtc/candidate","value":""}`))
	view.addClientSignal([]byte(`{"type":"webrtc/candidate","value":"a=candidate:x"}`))
	if len(view.pendingCandidates) != 1 {
		t.Fatal(view.pendingCandidates)
	}
	view.sendSignal("x", "y")
	view.setPaused(true)
	view.video, _ = newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "resume", "test")
	view.setPaused(false)
	view.close()
	view.close()
}

func TestWebRTCHelpers(t *testing.T) {
	if _, err := pcmaAPI(); err != nil {
		t.Fatal(err)
	}
	if _, err := mediaAPI(); err != nil {
		t.Fatal(err)
	}
	servers := edgeICEServers([]edgeICEServer{{URLs: []string{"stun:x"}, Username: "u", Credential: "p"}})
	if len(servers) != 1 || servers[0].CredentialType != webrtc.ICECredentialTypePassword {
		t.Fatal(servers)
	}
	for _, payload := range [][]byte{nil, []byte{0x61}, []byte{0x78, 0, 0}, []byte{0x7c}} {
		_ = h264RTPStartsIDR(payload)
	}
}

func TestViewForwardLocalPacketSkipsPausedAndNonIDR(t *testing.T) {
	bridge := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	packet := &rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x61}}
	ready := false
	var once sync.Once
	first := make(chan struct{})
	bridge.paused.Store(true)
	if !bridge.forwardLocalPacket(webrtc.RTPCodecTypeVideo, video, packet, &ready, &once, first) {
		t.Fatal("paused packet stopped forwarding loop")
	}
	bridge.paused.Store(false)
	close(bridge.remoteReady)
	bridge.waitForVideoIDR.Store(true)
	if !bridge.forwardLocalPacket(webrtc.RTPCodecTypeVideo, video, packet, &ready, &once, first) || !bridge.waitForVideoIDR.Load() {
		t.Fatal("non-IDR packet was forwarded")
	}
	failedWrite := func(*webrtc.TrackLocalStaticRTP, *rtp.Packet) error { return errors.New("forward") }
	bridge.writeRTP = failedWrite
	bridge.waitForVideoIDR.Store(false)
	if bridge.forwardLocalPacket(webrtc.RTPCodecTypeVideo, video, packet, &ready, &once, first) {
		t.Fatal("view write error accepted")
	}
	talk := newTalkBridge(context.Background(), "", "", talkBridgeOffer{}, func(json.RawMessage) {})
	talk.writeRTP = failedWrite
	if talk.forwardRemotePacket(video, packet) {
		t.Fatal("talk write error accepted")
	}
	bridge.close()
}

func TestCandidateQueueAndFlush(t *testing.T) {
	var sent atomic.Bool
	var mu sync.Mutex
	var pending []string
	var delivered []string
	handler := candidateHandler(&sent, &mu, &pending, func(value string) error {
		delivered = append(delivered, value)
		return nil
	})
	handler(nil)
	candidate := &webrtc.ICECandidate{
		Foundation: "1", Priority: 1, Address: "127.0.0.1", Protocol: webrtc.ICEProtocolUDP,
		Port: 9, Typ: webrtc.ICECandidateTypeHost, Component: 1,
	}
	handler(candidate)
	if len(pending) != 1 {
		t.Fatal(pending)
	}
	sent.Store(true)
	handler(candidate)
	if len(delivered) != 1 {
		t.Fatal(delivered)
	}
	if err := flushCandidates(pending, func(value string) error {
		delivered = append(delivered, value)
		return nil
	}); err != nil || len(delivered) != 2 {
		t.Fatal(err, delivered)
	}
	want := errors.New("candidate")
	if err := flushCandidates(pending, func(string) error { return want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := newTalkBridge(context.Background(), "", "", talkBridgeOffer{}, func(json.RawMessage) {}).sendCandidate("candidate"); err != nil {
		t.Fatal(err)
	}
	if err := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).sendCandidate("candidate"); err != nil {
		t.Fatal(err)
	}
}

func TestIdentityInvalidAndFilesystemErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")
	if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateIdentity(path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateIdentity(dir); err == nil {
		t.Fatal("directory read accepted")
	}
	blocked := filepath.Join(path, "child")
	if _, err := loadOrCreateIdentity(blocked); err == nil {
		t.Fatal("mkdir error expected")
	}
}

func TestPairingOriginAndForbiddenPage(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://localhost/pairing", nil)
	request.Host = "[::1]:8099"
	request.TLS = &tls.ConnectionState{}
	if origin, ok := pairingOrigin(request); !ok || origin != "https://[::1]:8099" {
		t.Fatalf("%q %v", origin, ok)
	}
	r := httptest.NewRecorder()
	newPairingManager(deriveIdentity("root")).servePage(r, httptest.NewRequest(http.MethodGet, "http://example.com/pairing", nil))
	if r.Code != http.StatusForbidden {
		t.Fatal(r.Code)
	}
}

func TestStreamServePaths(t *testing.T) {
	s := &streamCache{sourceNames: []string{"front"}, wakeup: make(chan struct{}), connected: true}
	r := httptest.NewRequest(http.MethodGet, "/stream/front.ts", nil)
	plain := &nonFlushingWriter{header: make(http.Header)}
	s.serve(plain, r)
	if plain.status != http.StatusInternalServerError {
		t.Fatal(plain.status)
	}

	s.cache = []byte("bootstrap")
	s.sequence = 1
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream/front.ts", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { s.serve(recorder, req); close(done) }()
	for i := 0; i < 100 && s.clients.Load() == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	s.mu.Lock()
	s.sequence = 2
	s.history = []packetEvent{{seq: 1, data: []byte("old")}, {seq: 2, data: []byte("live")}}
	s.lastPacket = time.Now()
	s.signalLocked()
	s.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("serve did not stop")
	}
	if !strings.Contains(recorder.Body.String(), "bootstrap") || !strings.Contains(recorder.Body.String(), "live") {
		t.Fatal(recorder.Body.String())
	}
}

type nonFlushingWriter struct {
	header http.Header
	status int
}

func (w *nonFlushingWriter) Header() http.Header         { return w.header }
func (w *nonFlushingWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *nonFlushingWriter) WriteHeader(status int)      { w.status = status }

func TestStreamConsumeStatusAndSuccessUntilEOF(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("src") == "bad" {
			http.Error(w, "bad", http.StatusBadGateway)
			return
		}
		packet := make([]byte, packetSize)
		packet[0], packet[3] = 0x47, 0x10
		_, _ = w.Write(packet)
	}))
	defer server.Close()
	newCache := func(source string) *streamCache {
		ctx, cancel := context.WithCancel(context.Background())
		return &streamCache{name: "x", sourceNames: []string{source}, go2rtcURL: server.URL, maxCache: 1024, client: server.Client(), ctx: ctx, cancel: cancel, wakeup: make(chan struct{})}
	}
	bad := newCache("bad")
	defer bad.stop()
	if err := bad.consume(); err == nil || !strings.Contains(err.Error(), "502") {
		t.Fatal(err)
	}
	good := newCache("good")
	defer good.stop()
	if err := good.consume(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !good.connected || good.sequence != 1 || len(good.history) != 1 {
		t.Fatalf("%#v", good)
	}
}

func TestManagerSyncLifecycleAndRefreshCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"cameras":{}}`)
	}))
	defer server.Close()
	ctx, cancelStream := context.WithCancel(context.Background())
	old := &streamCache{sourceNames: []string{"old"}, ctx: ctx, cancel: cancelStream, wakeup: make(chan struct{})}
	m := &streamManager{cfg: config{frigateURL: server.URL, preferredQuality: "HD", discoveryInterval: time.Millisecond}, client: server.Client(), streams: map[string]*streamCache{"old": old}}
	if err := m.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.streams) != 0 {
		t.Fatal(m.streams)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("old stream not stopped")
	}
	refreshCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.refreshLoop(refreshCtx); close(done) }()
	time.Sleep(3 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("refresh did not stop")
	}
}

func TestRefreshLoopKeepsStreamsOnDiscoveryError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad", http.StatusBadGateway)
	}))
	defer server.Close()
	m := newStreamManager(config{frigateURL: server.URL, discoveryInterval: time.Millisecond, idleTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	m.refreshLoop(ctx)
}

func TestRelaySessionsWithoutConnections(t *testing.T) {
	manager := &streamManager{streams: map[string]*streamCache{"front": {sourceNames: []string{"front_main"}}}}
	r := newRelayController("ws://relay", "http://go2rtc", deriveIdentity("root"), manager)
	r.openSession(context.Background(), "", "front")
	r.openSession(context.Background(), "one", "missing")
	r.openSession(context.Background(), "one", "front")
	if r.sessions["one"] == nil {
		t.Fatal("session missing")
	}
	r.writeLocalSignal("one", json.RawMessage("{"))
	r.writeLocalSignal("missing", json.RawMessage(`{"type":"x"}`))
	r.send(relayEnvelope{Type: "x"})
	r.closeSession("missing")
	r.closeSession("one")
	if len(r.sessions) != 0 {
		t.Fatal(r.sessions)
	}
	r.openSession(context.Background(), "two", "front")
	r.closeSessions()
	if len(r.sessions) != 0 || r.conn != nil {
		t.Fatal("sessions not cleared")
	}
}

func TestRelayConnectDispatchesMessages(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer root" || req.Header.Get("X-Fricam-Edge-Version") != version {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, raw := range []string{
			`{`,
			`{"type":"session/open","sessionId":"one","camera":"front"}`,
			`{"type":"signal","sessionId":"one","payload":{"type":"invalid"}}`,
			`{"type":"session/close","sessionId":"one"}`,
		} {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(raw))
		}
	}))
	defer server.Close()
	r := newRelayController("ws"+strings.TrimPrefix(server.URL, "http"), "http://go2rtc", edgeIdentity{DeviceID: "device01", RootSecret: "root"}, &streamManager{streams: map[string]*streamCache{"front": {sourceNames: []string{"front"}}}})
	err := r.connect(context.Background())
	if err == nil || !r.connected.Load() {
		t.Fatalf("connect err=%v connected=%v", err, r.connected.Load())
	}

	bad := newRelayController("ws"+strings.TrimPrefix(server.URL, "http")+"/missing", "", edgeIdentity{DeviceID: "x", RootSecret: "bad"}, &streamManager{})
	if err := bad.connect(context.Background()); err == nil {
		t.Fatal("expected handshake error")
	}
	bad.relayURL = "ws://127.0.0.1:1"
	if err := bad.connect(context.Background()); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestOpenLocalSignalingReadsUntilClose(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/ws" {
			http.Error(w, "missing", http.StatusBadGateway)
			return
		}
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte("not-json"))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"webrtc/candidate","value":"candidate:1 1 udp 1 1.1.1.1 1 typ host"}`))
	}))
	defer server.Close()
	r := newRelayController("", server.URL, deriveIdentity("root"), &streamManager{})
	ctx, cancel := context.WithCancel(context.Background())
	s := &relayMediaSession{id: "one", camera: "front", source: "front", ctx: ctx, cancel: cancel}
	r.sessions["one"] = s
	if !r.openLocalSignaling(s) {
		t.Fatal("open failed")
	}
	for i := 0; i < 100; i++ {
		r.mu.Lock()
		exists := r.sessions["one"] != nil
		r.mu.Unlock()
		if !exists {
			break
		}
		time.Sleep(time.Millisecond)
	}
	r.mu.Lock()
	exists := r.sessions["one"] != nil
	r.mu.Unlock()
	if exists {
		t.Fatal("session was not closed")
	}

	r.go2rtcURL = server.URL + "/missing"
	s = &relayMediaSession{ctx: context.Background(), camera: "front", source: "front"}
	if r.openLocalSignaling(s) {
		t.Fatal("unexpected signaling success")
	}
	r.go2rtcURL = "http://127.0.0.1:1"
	if r.openLocalSignaling(s) {
		t.Fatal("unexpected dial success")
	}
}

func websocketPair(t *testing.T) (*websocket.Conn, *websocket.Conn, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			accepted <- conn
		}
	}))
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	peer := <-accepted
	return client, peer, func() { client.Close(); peer.Close(); server.Close() }
}

func TestProxyWebRTCSignalsValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      int
		payload  []byte
		sanitize bool
	}{
		{"binary", websocket.BinaryMessage, []byte(`{}`), false},
		{"json", websocket.TextMessage, []byte(`{`), false},
		{"sanitize", websocket.TextMessage, []byte(`{"type":"webrtc","value":{}}`), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceClient, sourceServer, closeSource := websocketPair(t)
			defer closeSource()
			destClient, destServer, closeDest := websocketPair(t)
			defer closeDest()
			result := make(chan error, 1)
			go proxyWebRTCSignals(destServer, sourceServer, tc.sanitize, result)
			if err := sourceClient.WriteMessage(tc.typ, tc.payload); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("nil error")
				}
			case <-time.After(time.Second):
				t.Fatal("timeout")
			}
			_ = destClient
		})
	}
	sourceClient, sourceServer, closeSource := websocketPair(t)
	defer closeSource()
	destClient, destServer, closeDest := websocketPair(t)
	defer closeDest()
	result := make(chan error, 1)
	go proxyWebRTCSignals(destServer, sourceServer, false, result)
	if err := sourceClient.WriteMessage(websocket.TextMessage, []byte(`{"type":"x"}`)); err != nil {
		t.Fatal(err)
	}
	_, payload, err := destClient.ReadMessage()
	if err != nil || string(payload) != `{"type":"x"}` {
		t.Fatal(string(payload), err)
	}
	sourceClient.Close()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func applyBridgeSignal(t *testing.T, peer *webrtc.PeerConnection, payload json.RawMessage) {
	t.Helper()
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(payload, &signal); err != nil {
		t.Fatal(err)
	}
	switch signal.Type {
	case "webrtc":
		var value struct{ Type, SDP string }
		if err := json.Unmarshal(signal.Value, &value); err != nil {
			t.Fatal(err)
		}
		if err := peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: value.SDP}); err != nil {
			t.Fatal(err)
		}
	case "webrtc/candidate":
		var candidate string
		if err := json.Unmarshal(signal.Value, &candidate); err != nil {
			t.Fatal(err)
		}
		if err := peer.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTalkBridgeRemoteWebRTC(t *testing.T) {
	api, err := pcmaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTrack(track); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var bridge *talkBridge
	bridge = newTalkBridge(ctx, "", "", talkBridgeOffer{SDP: peer.LocalDescription().SDP, ICEServers: []edgeICEServer{{URLs: []string{"stun:127.0.0.1:9"}}}}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
	bridge.writeRTP = func(*webrtc.TrackLocalStaticRTP, *rtp.Packet) error { return errors.New("forward") }
	bridge.pendingCandidates = []webrtc.ICECandidateInit{{Candidate: "candidate:1 1 udp 1 127.0.0.1 9 typ host"}}
	localTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "local", "test")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- bridge.connectRemote(ctx, localTrack) }()
	for i := 0; i < 500 && peer.ConnectionState() != webrtc.PeerConnectionStateConnected; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if err := track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, SequenceNumber: 1, Timestamp: 1, SSRC: 1}, Payload: []byte{1, 2}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	bridge.addClientSignal([]byte(`{"type":"webrtc/candidate","value":"candidate:1 1 udp 1 127.0.0.1 9 typ host"}`))
	bridge.close()
	time.Sleep(20 * time.Millisecond)
}

func TestViewBridgeRemoteAndBootstrap(t *testing.T) {
	api, err := mediaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	_, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	if err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newViewBridge(ctx, "", "", func() [][]byte { return [][]byte{{0, 0, 1, 0x65, 1}} }, viewBridgeOffer{SDP: peer.LocalDescription().SDP}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
	bridge.pendingCandidates = []webrtc.ICECandidateInit{{Candidate: "candidate:1 1 udp 1 127.0.0.1 9 typ host"}}
	video, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "video", "test")
	if err != nil {
		t.Fatal(err)
	}
	audio, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	connected, err := bridge.connectRemote(ctx, video, audio)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !bridge.writeBootstrap(video) || !bridge.bootstrapWritten.Load() {
		t.Fatal("bootstrap not written")
	}
	bridge.addClientSignal([]byte(`{"type":"webrtc/candidate","value":"candidate:1 1 udp 1 127.0.0.1 9 typ host"}`))
	bridge.setPaused(true)
	bridge.setPaused(false)
	bridge.close()
	time.Sleep(20 * time.Millisecond)
	none := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if none.writeBootstrap(video) {
		t.Fatal("nil bootstrap")
	}
	none.bootstrap = func() [][]byte { return nil }
	if none.writeBootstrap(video) {
		t.Fatal("empty bootstrap")
	}
	none.close()
}

func fakeGo2RTCServer(t *testing.T, withMedia bool) (*httptest.Server, <-chan *webrtc.TrackLocalStaticRTP) {
	t.Helper()
	videoTracks := make(chan *webrtc.TrackLocalStaticRTP, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var peersMu sync.Mutex
	var peers []*webrtc.PeerConnection
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ws" {
			http.Error(w, "missing", http.StatusBadGateway)
			return
		}
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		_, raw, err := socket.ReadMessage()
		if err != nil {
			return
		}
		var signal struct{ Type, Value string }
		if json.Unmarshal(raw, &signal) != nil {
			return
		}
		api, err := mediaAPI()
		if err != nil {
			return
		}
		peer, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			return
		}
		peersMu.Lock()
		peers = append(peers, peer)
		peersMu.Unlock()
		defer peer.Close()
		if withMedia {
			video, trackErr := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "video", "go2rtc")
			if trackErr != nil {
				return
			}
			audio, trackErr := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "go2rtc")
			if trackErr != nil {
				return
			}
			if _, trackErr = peer.AddTrack(video); trackErr != nil {
				return
			}
			if _, trackErr = peer.AddTrack(audio); trackErr != nil {
				return
			}
			videoTracks <- video
		}
		if peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: signal.Value}) != nil {
			return
		}
		answer, err := peer.CreateAnswer(nil)
		if err != nil {
			return
		}
		gather := webrtc.GatheringCompletePromise(peer)
		if peer.SetLocalDescription(answer) != nil {
			return
		}
		<-gather
		if socket.WriteJSON(map[string]string{"type": "webrtc/answer", "value": peer.LocalDescription().SDP}) != nil {
			return
		}
		for {
			if _, _, err = socket.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(func() {
		server.Close()
		peersMu.Lock()
		defer peersMu.Unlock()
		for _, peer := range peers {
			_ = peer.Close()
		}
	})
	return server, videoTracks
}

func TestTalkBridgeConnectLocal(t *testing.T) {
	server, _ := fakeGo2RTCServer(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newTalkBridge(ctx, server.URL, "front_talk", talkBridgeOffer{}, func(json.RawMessage) {})
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = bridge.connectLocal(ctx, track); err != nil {
		t.Fatal(err)
	}
	bridge.close()

	bad := newTalkBridge(ctx, server.URL+"/missing", "front", talkBridgeOffer{}, func(json.RawMessage) {})
	if err = bad.connectLocal(ctx, track); err == nil {
		t.Fatal("expected signaling status error")
	}
	bad.close()
}

func TestViewBridgeConnectLocalForwardsVideo(t *testing.T) {
	server, tracks := fakeGo2RTCServer(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newViewBridge(ctx, server.URL, "front", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	video, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "video", "edge")
	if err != nil {
		t.Fatal(err)
	}
	audio, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "edge")
	if err != nil {
		t.Fatal(err)
	}
	first, err := bridge.connectLocal(ctx, video, audio, "front")
	if err != nil {
		t.Fatal(err)
	}
	track := <-tracks
	for i := 0; i < 100 && bridge.localVideoSSRC.Load() == 0; i++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: uint16(i), Timestamp: uint32(i + 1), SSRC: 2}, Payload: []byte{0x61}})
		time.Sleep(5 * time.Millisecond)
	}
	if bridge.localVideoSSRC.Load() == 0 {
		t.Fatal("local video callback did not start")
	}
	_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 101, Timestamp: 101, SSRC: 2}, Payload: []byte{0x61}})
	time.Sleep(20 * time.Millisecond)
	bridge.paused.Store(true)
	close(bridge.remoteReady)
	for i := 5; i < 10; i++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: uint16(i), Timestamp: uint32(i + 1), SSRC: 2}, Payload: []byte{0x61}})
	}
	time.Sleep(30 * time.Millisecond)
	bridge.paused.Store(false)
	bridge.waitForVideoIDR.Store(true)
	for i := 10; i < 15; i++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: uint16(i), Timestamp: uint32(i + 1), SSRC: 2}, Payload: []byte{0x61}})
	}
	time.Sleep(30 * time.Millisecond)
	for sequence := uint16(1); ; sequence++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: sequence, Timestamp: uint32(sequence) * 3000, SSRC: 2}, Payload: []byte{0x65}})
		select {
		case <-first:
			goto received
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
received:
	if packets, _ := bridge.forwardedMedia(); packets == 0 {
		t.Fatal("video not forwarded")
	}
	bridge.close()

	bad := newViewBridge(ctx, server.URL+"/missing", "front", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err = bad.connectLocal(ctx, video, audio, "front"); err == nil {
		t.Fatal("expected signaling status error")
	}
	bad.close()
}

func TestViewBridgeConnectLocalStopsOnWriteError(t *testing.T) {
	server, tracks := fakeGo2RTCServer(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newViewBridge(ctx, server.URL, "front", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	bridge.writeRTP = func(*webrtc.TrackLocalStaticRTP, *rtp.Packet) error { return errors.New("forward") }
	close(bridge.remoteReady)
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "video", "edge")
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "edge")
	if _, err := bridge.connectLocal(ctx, video, audio, "front"); err != nil {
		t.Fatal(err)
	}
	track := <-tracks
	for i := 0; i < 20; i++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: uint16(i), Timestamp: uint32(i + 1), SSRC: 2}, Payload: []byte{0x65}})
		time.Sleep(5 * time.Millisecond)
	}
	bridge.close()
}

func TestRunHealthcheckAndMain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	t.Setenv("LISTEN_ADDR", strings.TrimPrefix(server.URL, "http://"))
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"edge", "healthcheck"}
	if err := runHealthcheck(strings.TrimPrefix(server.URL, "http://")); err != nil {
		t.Fatal(err)
	}
	main()
	if err := runHealthcheck("invalid"); err == nil {
		t.Fatal("invalid listen accepted")
	}
	if err := runHealthcheck("127.0.0.1:1"); err == nil {
		t.Fatal("unreachable health accepted")
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad", http.StatusBadGateway) }))
	defer bad.Close()
	if err := runHealthcheck(strings.TrimPrefix(bad.URL, "http://")); err == nil {
		t.Fatal("bad status accepted")
	}
}

func TestRunEdgeHTTPRoutesWithoutRelay(t *testing.T) {
	frigate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"cameras":{}}`) }))
	defer frigate.Close()
	t.Setenv("FRIGATE_URL", frigate.URL)
	t.Setenv("EDGE_RELAY_URL", "")
	oldArgs := os.Args
	os.Args = []string{"edge"}
	defer func() { os.Args = oldArgs }()
	oldListen := listenAndServe
	defer func() { listenAndServe = oldListen }()
	sentinel := errors.New("stop")
	listenAndServe = func(server *http.Server) error {
		for _, tc := range []struct {
			method, path string
			status       int
		}{
			{http.MethodGet, "/health", http.StatusOK}, {http.MethodGet, "/metrics", http.StatusOK},
			{http.MethodGet, "/stream/bad/path.ts", http.StatusNotFound}, {http.MethodGet, "/stream/missing.ts", http.StatusNotFound},
			{http.MethodGet, "/webrtc", http.StatusServiceUnavailable}, {http.MethodPost, "/pair", http.StatusServiceUnavailable},
			{http.MethodGet, "/pairing", http.StatusServiceUnavailable}, {http.MethodPost, "/pair/claim", http.StatusServiceUnavailable},
		} {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			server.Handler.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Errorf("%s %s=%d", tc.method, tc.path, rec.Code)
			}
		}
		return sentinel
	}
	if err := runEdge(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestRunEdgeDiscoveryError(t *testing.T) {
	t.Setenv("FRIGATE_URL", "http://127.0.0.1:1")
	t.Setenv("EDGE_RELAY_URL", "")
	oldArgs := os.Args
	os.Args = []string{"edge"}
	defer func() { os.Args = oldArgs }()
	if err := runEdge(); err == nil || !strings.Contains(err.Error(), "discovery") {
		t.Fatal(err)
	}
}

func TestTalkBridgeStartEndToEnd(t *testing.T) {
	go2rtc, _ := fakeGo2RTCServer(t, false)
	api, err := pcmaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "audio", "client")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTrack(track); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newTalkBridge(ctx, go2rtc.URL, "front_talk", talkBridgeOffer{SDP: peer.LocalDescription().SDP}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
	done := make(chan error, 1)
	go func() { done <- bridge.start() }()
	for sequence := uint16(1); ; sequence++ {
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, SequenceNumber: sequence, Timestamp: uint32(sequence) * 160, SSRC: 3}, Payload: []byte{1}})
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			bridge.close()
			return
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestViewBridgeStartEndToEnd(t *testing.T) {
	go2rtc, _ := fakeGo2RTCServer(t, true)
	api, err := mediaAPI()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bridge := newViewBridge(ctx, go2rtc.URL, "front", func() [][]byte { return [][]byte{{0, 0, 1, 0x65, 1}} }, viewBridgeOffer{SDP: peer.LocalDescription().SDP}, func(payload json.RawMessage) { applyBridgeSignal(t, peer, payload) })
	if err := bridge.start(); err != nil {
		t.Fatal(err)
	}
	if !bridge.bootstrapWritten.Load() {
		t.Fatal("bootstrap not used")
	}
	bridge.close()

	paused := newViewBridge(ctx, go2rtc.URL, "front", nil, viewBridgeOffer{SDP: peer.LocalDescription().SDP, WarmPaused: true}, func(json.RawMessage) {})
	if !paused.paused.Load() {
		t.Fatal("pause flag")
	}
	paused.close()
}

func TestInjectedErrorPaths(t *testing.T) {
	want := errors.New("injected")
	oldPairRead, oldQR := pairingRandomRead, pairingQREncode
	defer func() { pairingRandomRead = oldPairRead; pairingQREncode = oldQR }()
	pairingRandomRead = func([]byte) (int, error) { return 0, want }
	manager := newPairingManager(deriveIdentity("root"))
	if _, err := manager.issue("http://localhost"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	pairingRandomRead = oldPairRead
	pairingQREncode = func(string, qrcode.RecoveryLevel, int) ([]byte, error) { return nil, want }
	rec := httptest.NewRecorder()
	manager.servePage(rec, httptest.NewRequest(http.MethodGet, "http://localhost/pairing", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatal(rec.Code)
	}

	oldRandom, oldRead, oldMkdir, oldWrite := identityRandomRead, identityReadFile, identityMkdirAll, identityWriteFile
	defer func() {
		identityRandomRead = oldRandom
		identityReadFile = oldRead
		identityMkdirAll = oldMkdir
		identityWriteFile = oldWrite
	}()
	identityRandomRead = func([]byte) (int, error) { return 0, want }
	if _, err := loadOrCreateIdentity("x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	identityRandomRead = oldRandom
	identityMkdirAll = func(string, os.FileMode) error { return want }
	if _, err := loadOrCreateIdentity("x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	identityMkdirAll = oldMkdir
	identityWriteFile = func(string, []byte, os.FileMode) error { return want }
	if _, err := loadOrCreateIdentity(filepath.Join(t.TempDir(), "x")); !errors.Is(err, want) {
		t.Fatal(err)
	}

	oldFatalf := fatalLogf
	defer func() { fatalLogf = oldFatalf }()
	called := false
	fatalLogf = func(string, ...any) { called = true }
	t.Setenv("BAD_INT", "bad")
	_ = mustIntEnv("BAD_INT", 2, 1, 3)
	if !called {
		t.Fatal("fatalf not called")
	}
}

type failingResponseWriter struct{ header http.Header }

func (w *failingResponseWriter) Header() http.Header       { return w.header }
func (w *failingResponseWriter) WriteHeader(int)           {}
func (w *failingResponseWriter) Write([]byte) (int, error) { return 0, errors.New("write") }

func TestWriteJSONFailureAndMainFailure(t *testing.T) {
	writeJSON(&failingResponseWriter{header: make(http.Header)}, http.StatusOK, map[string]string{"x": "y"})
	oldArgs := os.Args
	os.Args = []string{"edge"}
	defer func() { os.Args = oldArgs }()
	t.Setenv("FRIGATE_URL", "http://127.0.0.1:1")
	oldFatal := fatalLog
	defer func() { fatalLog = oldFatal }()
	called := false
	fatalLog = func(...any) { called = true }
	main()
	if !called {
		t.Fatal("fatal not called")
	}
}

func TestPairingIssuePrunesCodes(t *testing.T) {
	now := time.Now()
	manager := newPairingManager(deriveIdentity("root"))
	manager.now = func() time.Time { return now }
	manager.codes["expired"] = now.Add(-time.Second)
	for i := 0; i < 64; i++ {
		manager.codes[fmt.Sprint(i)] = now.Add(time.Hour)
	}
	if _, err := manager.issue("http://localhost"); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.codes["expired"]; ok {
		t.Fatal("expired code retained")
	}
}

func TestRunEdgeRelayRoutes(t *testing.T) {
	frigate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/config" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "" && r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"cameras":{"front":{"live":{"streams":{"HD":"front"}}}}}`)
	}))
	defer frigate.Close()
	t.Setenv("FRIGATE_URL", frigate.URL)
	t.Setenv("FRIGATE_AUTH_URL", frigate.URL)
	t.Setenv("EDGE_RELAY_URL", "wss://relay.example/")
	t.Setenv("EDGE_IDENTITY_FILE", filepath.Join(t.TempDir(), "identity.json"))
	oldArgs := os.Args
	os.Args = []string{"edge"}
	defer func() { os.Args = oldArgs }()
	oldListen, oldRefresh, oldRelay := listenAndServe, startRefreshLoop, startRelayLoop
	oldPairRead := pairingRandomRead
	defer func() {
		listenAndServe = oldListen
		startRefreshLoop = oldRefresh
		startRelayLoop = oldRelay
		pairingRandomRead = oldPairRead
	}()
	startRefreshLoop = func(*streamManager) {}
	startRelayLoop = func(*relayController) {}
	pairingRandomRead = func(value []byte) (int, error) {
		for i := range value {
			value[i] = 1
		}
		return len(value), nil
	}
	sentinel := errors.New("stop")
	listenAndServe = func(server *http.Server) error {
		request := func(method, path, authorization string, body []byte) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, bytes.NewReader(body))
			req.Host = "127.0.0.1:8099"
			req.RemoteAddr = "127.0.0.1:2"
			req.Header.Set("Authorization", authorization)
			rec := httptest.NewRecorder()
			server.Handler.ServeHTTP(rec, req)
			return rec
		}
		if rec := request(http.MethodGet, "/health", "", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "device_id") {
			t.Fatal(rec.Code, rec.Body.String())
		}
		if rec := request(http.MethodGet, "/stream/front.ts", "", nil); rec.Code == http.StatusNotFound {
			t.Fatal(rec.Code, rec.Body.String())
		}
		if rec := request(http.MethodGet, "/webrtc", "", nil); rec.Code != http.StatusUnauthorized {
			t.Fatal(rec.Code)
		}
		if rec := request(http.MethodPost, "/pair", "", nil); rec.Code != http.StatusUnauthorized {
			t.Fatal(rec.Code)
		}
		if rec := request(http.MethodPost, "/pair", "Bearer secret", nil); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "clientToken") {
			t.Fatal(rec.Code, rec.Body.String())
		}
		if rec := request(http.MethodGet, "/pairing", "", nil); rec.Code != http.StatusOK {
			t.Fatal(rec.Code)
		}
		if rec := request(http.MethodPost, "/pair/claim", "Bearer secret", []byte("{")); rec.Code != http.StatusBadRequest {
			t.Fatal(rec.Code)
		}
		if rec := request(http.MethodPost, "/pair/claim", "", []byte(`{}`)); rec.Code != http.StatusUnauthorized {
			t.Fatal(rec.Code)
		}
		code := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 24))
		body, _ := json.Marshal(map[string]string{"code": code})
		if rec := request(http.MethodPost, "/pair/claim", "Bearer secret", body); rec.Code != http.StatusOK {
			t.Fatal(rec.Code, rec.Body.String())
		}
		if rec := request(http.MethodPost, "/pair/claim", "Bearer secret", body); rec.Code != http.StatusBadRequest {
			t.Fatal(rec.Code)
		}
		return sentinel
	}
	if err := runEdge(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}

func TestRunEdgeIdentityError(t *testing.T) {
	frigate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"cameras":{}}`) }))
	defer frigate.Close()
	t.Setenv("FRIGATE_URL", frigate.URL)
	t.Setenv("EDGE_RELAY_URL", "wss://relay.example")
	t.Setenv("EDGE_IDENTITY_FILE", t.TempDir())
	oldArgs := os.Args
	os.Args = []string{"edge"}
	defer func() { os.Args = oldArgs }()
	oldRefresh := startRefreshLoop
	startRefreshLoop = func(*streamManager) {}
	defer func() { startRefreshLoop = oldRefresh }()
	if err := runEdge(); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatal(err)
	}
}

func TestRemainingParserAndBootstrapBranches(t *testing.T) {
	if _, ok := psiSection([]byte{9, 0, 0, 0, 0}, true); ok {
		t.Fatal("pointer overflow")
	}
	pat := []byte{0, 0xb0, 17, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if parsePAT(pat) != 0 {
		t.Fatal("network PID")
	}
	pmt := []byte{2, 0xb0, 18, 0, 1, 0, 0, 0, 0xe1, 0, 0xf0, 0, 0x0f, 0xe1, 1, 0xf0, 0, 0, 0, 0}
	if pid, codec := parsePMT(pmt); pid != 0 || codec != "" {
		t.Fatal(pid, codec)
	}
	pmt[0] = 3
	if pid, _ := parsePMT(pmt); pid != 0 {
		t.Fatal(pid)
	}
	badPES := append(makeTSPacket(0, true, []byte{0, 0, 0xb0, 17, 0, 1, 0, 0, 0, 0, 1, 0xe1, 0, 0, 0, 0, 0, 0, 0, 0}), makeTSPacket(0x100, true, []byte{0, 2, 0xb0, 18, 0, 1, 0, 0, 0, 0xe1, 0, 0xf0, 0, 0x1b, 0xe1, 1, 0xf0, 0, 0, 0, 0})...)
	badPES = append(badPES, makeTSPacket(0x101, true, []byte{1, 2, 3})...)
	if extractVideoAccessUnits(badPES) != nil {
		t.Fatal("bad PES")
	}
	manager := &streamManager{streams: map[string]*streamCache{"front": {sourceNames: []string{"front_main", "front_sub"}, sourceIndex: 0, codec: "h264", cache: testTransportStream(), wakeup: make(chan struct{})}}}
	if len(manager.h264Bootstrap("front")) == 0 || len(manager.h264Bootstrap("front_main")) == 0 {
		t.Fatal("bootstrap lookup")
	}
	if manager.h264Bootstrap("front_sub") != nil {
		t.Fatal("borrowed bootstrap")
	}
}

func TestAuthorizationAndDiscoveryRemainingErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Cookie"), "frigate_token") {
			t.Error("basic auth got cookie")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if !validateFrigateAuthorization(context.Background(), server.URL, "Basic abc") {
		t.Fatal("basic rejected")
	}
	if validateFrigateAuthorization(context.Background(), ":", "Bearer x") {
		t.Fatal("bad URL")
	}
	if _, err := discoverCameraStreams(context.Background(), http.DefaultClient, ":", "HD"); err == nil {
		t.Fatal("bad discovery URL")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
	go httpServer.Serve(listener)
	defer httpServer.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	if err := runHealthcheck("0.0.0.0:" + port); err != nil {
		t.Fatal(err)
	}
}

func TestPairingPageIssueFailure(t *testing.T) {
	old := pairingRandomRead
	defer func() { pairingRandomRead = old }()
	pairingRandomRead = func([]byte) (int, error) { return 0, errors.New("random") }
	rec := httptest.NewRecorder()
	newPairingManager(deriveIdentity("root")).servePage(rec, httptest.NewRequest(http.MethodGet, "http://localhost/pairing", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatal(rec.Code)
	}
}

func TestManagerSyncAddsAndReplaces(t *testing.T) {
	var configBody atomic.Value
	configBody.Store(`{"cameras":{"front":{"live":{"streams":{"HD":"main"}}}}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, configBody.Load().(string)) }))
	defer server.Close()
	m := newStreamManager(config{frigateURL: server.URL, go2rtcURL: "http://127.0.0.1:1", preferredQuality: "HD", idleTimeout: time.Second, maxCache: 1024})
	if err := m.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := m.get("front")
	if first == nil {
		t.Fatal("not added")
	}
	configBody.Store(`{"cameras":{"front":{"live":{"streams":{"HD":"changed"}}}}}`)
	if err := m.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := m.get("front")
	if second == nil || second == first {
		t.Fatal("not replaced")
	}
	second.stop()
}

func TestWriteLocalSignalStartsBridges(t *testing.T) {
	credential := strings.Repeat("a", 64)
	talkPayload := json.RawMessage(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 8\r\na=sendonly\r\n","ice_servers":[{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	viewPayload := json.RawMessage(fmt.Sprintf(`{"type":"webrtc","value":{"type":"offer","sdp":"v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=recvonly\r\n","ice_servers":[{"urls":["turns:turn.cloudflare.com:443?transport=tcp"],"username":"%s","credential":"%s"}]}}`, credential, credential))
	oldTalk, oldView := startTalkBridge, startViewBridge
	defer func() { startTalkBridge = oldTalk; startViewBridge = oldView }()
	r := newRelayController("", "http://127.0.0.1:1", deriveIdentity("root"), &streamManager{})
	newSession := func(id, source string) *relayMediaSession {
		ctx, cancel := context.WithCancel(context.Background())
		s := &relayMediaSession{id: id, camera: "front", source: source, ctx: ctx, cancel: cancel}
		r.sessions[id] = s
		return s
	}
	startTalkBridge = func(*talkBridge) error { return errors.New("talk fail") }
	newSession("talk-error", "front_talk")
	r.writeLocalSignal("talk-error", talkPayload)
	for i := 0; i < 100; i++ {
		r.mu.Lock()
		exists := r.sessions["talk-error"] != nil
		r.mu.Unlock()
		if !exists {
			break
		}
		time.Sleep(time.Millisecond)
	}
	startTalkBridge = func(*talkBridge) error { return nil }
	talkSession := newSession("talk-ok", "front_talk")
	r.writeLocalSignal("talk-ok", talkPayload)
	if talkSession.talk == nil {
		t.Fatal("talk bridge missing")
	}
	talkSession.talk.send(json.RawMessage(`{"type":"test"}`))
	startViewBridge = func(*viewBridge) error { return errors.New("view fail") }
	newSession("view-error", "front")
	r.writeLocalSignal("view-error", viewPayload)
	for i := 0; i < 100; i++ {
		r.mu.Lock()
		exists := r.sessions["view-error"] != nil
		r.mu.Unlock()
		if !exists {
			break
		}
		time.Sleep(time.Millisecond)
	}
	startViewBridge = func(*viewBridge) error { return nil }
	viewSession := newSession("view-ok", "front")
	r.writeLocalSignal("view-ok", viewPayload)
	if viewSession.view == nil {
		t.Fatal("view bridge missing")
	}
	_ = viewSession.view.bootstrap()
	viewSession.view.send(json.RawMessage(`{"type":"test"}`))
	r.writeLocalSignal("view-ok", json.RawMessage(`{"type":"webrtc","value":{}}`))
	raw := newSession("raw", "front")
	r.writeLocalSignal("raw", json.RawMessage(`{"type":"webrtc/offer","value":"v=0"}`))
	if raw.conn != nil {
		t.Fatal("unexpected connection")
	}
	r.closeSessions()
}

func TestLocalSignalReadersMalformedAndCandidates(t *testing.T) {
	for _, kind := range []string{"talk", "view"} {
		t.Run(kind, func(t *testing.T) {
			client, server, closePair := websocketPair(t)
			defer closePair()
			api, err := mediaAPI()
			if err != nil {
				t.Fatal(err)
			}
			peer, err := api.NewPeerConnection(webrtc.Configuration{})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			answerSet := make(chan struct{}, 1)
			failed := make(chan error, 1)
			if kind == "talk" {
				bridge := newTalkBridge(context.Background(), "", "", talkBridgeOffer{}, func(json.RawMessage) {})
				go bridge.readLocalSignals(peer, server, answerSet, failed)
				defer bridge.close()
			} else {
				bridge := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {})
				go bridge.readLocalSignals(peer, server, answerSet, failed)
				defer bridge.close()
			}
			for _, raw := range []string{`{`, `{"type":"webrtc/candidate","value":3}`, `{"type":"webrtc/candidate","value":"a=candidate:1 1 udp 1 127.0.0.1 9 typ host"}`, `{"type":"other","value":"x"}`} {
				if err := client.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "talk" {
				if err := client.WriteMessage(websocket.TextMessage, []byte(`{"type":"webrtc/answer","value":"bad"}`)); err != nil {
					t.Fatal(err)
				}
				select {
				case <-failed:
				case <-time.After(time.Second):
					t.Fatal("talk failure missing")
				}
			} else {
				if err := client.WriteMessage(websocket.TextMessage, []byte(`{"type":"webrtc/answer","value":3}`)); err != nil {
					t.Fatal(err)
				}
				client.Close()
				select {
				case <-failed:
				case <-time.After(time.Second):
					t.Fatal("view close missing")
				}
			}
		})
	}
}

func TestSignalMetadataAndSanitizerRemainingBranches(t *testing.T) {
	if kind, candidate := signalMetadata([]byte(`{"type":"webrtc/candidate","value":"candidate without type"}`)); kind != "candidate" || candidate != "-" {
		t.Fatal(kind, candidate)
	}
	if kind, _ := signalMetadata([]byte(`{"type":"webrtc","value":"bad"}`)); kind != "" {
		t.Fatal(kind)
	}
	credential := strings.Repeat("a", 64)
	for _, offer := range []map[string]any{
		{"type": "offer", "sdp": strings.Repeat("x", 96*1024+1), "ice_servers": []edgeICEServer{{URLs: []string{"stun:stun.cloudflare.com:3478"}}}},
		{"type": "offer", "sdp": "v=0", "ice_servers": []edgeICEServer{{URLs: make([]string, 7)}}},
		{"type": "offer", "sdp": "v=0", "ice_servers": []edgeICEServer{{URLs: []string{"turns:turn.cloudflare.com:443?transport=tcp"}, Username: credential, Credential: "short"}}},
	} {
		value, _ := json.Marshal(offer)
		payload, _ := json.Marshal(map[string]any{"type": "webrtc", "value": json.RawMessage(value)})
		if _, ok := sanitizeSignalForGo2RTC(payload); ok {
			t.Fatal("invalid offer accepted")
		}
	}
}

func TestWebRTCRegistrationErrorsAndLaunchers(t *testing.T) {
	want := errors.New("register")
	oldCodec, oldInterceptors := registerWebRTCCodec, registerWebRTCInterceptors
	defer func() { registerWebRTCCodec, registerWebRTCInterceptors = oldCodec, oldInterceptors }()
	registerWebRTCCodec = func(*webrtc.MediaEngine, webrtc.RTPCodecParameters, webrtc.RTPCodecType) error { return want }
	if _, err := pcmaAPI(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := mediaAPI(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	registerWebRTCCodec = oldCodec
	registerWebRTCInterceptors = func(*webrtc.MediaEngine, *interceptor.Registry) error { return want }
	if _, err := newWebRTCAPI(&webrtc.MediaEngine{}); !errors.Is(err, want) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	manager := &streamManager{cfg: config{discoveryInterval: time.Hour}, streams: map[string]*streamCache{}}
	done := make(chan struct{})
	go func() { manager.refreshLoop(ctx); close(done) }()
	<-done
	oldDaemon := daemonContext
	defer func() { daemonContext = oldDaemon }()
	daemonContext = func() context.Context { return ctx }
	launchRefreshLoop(manager)
	relay := newRelayController("ws://127.0.0.1:1", "", edgeIdentity{DeviceID: "device01"}, manager)
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	cancelRelay()
	relay.run(relayCtx)
	launchRelayLoop(relay)
}

func TestPeerOperationEarlyErrors(t *testing.T) {
	want := errors.New("peer operation")
	oldAddTrack, oldKind, oldTrack := peerAddTrack, peerAddTransceiverFromKind, peerAddTransceiverFromTrack
	oldOffer, oldSetRemote := peerCreateOffer, peerSetRemoteDescription
	defer func() {
		peerAddTrack = oldAddTrack
		peerAddTransceiverFromKind = oldKind
		peerAddTransceiverFromTrack = oldTrack
		peerCreateOffer = oldOffer
		peerSetRemoteDescription = oldSetRemote
	}()
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	view := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{SDP: "bad"}, func(json.RawMessage) {})
	peerAddTrack = func(*webrtc.PeerConnection, webrtc.TrackLocal) (*webrtc.RTPSender, error) { return nil, want }
	if _, err := view.connectRemote(context.Background(), video, audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	count := 0
	peerAddTrack = func(peer *webrtc.PeerConnection, track webrtc.TrackLocal) (*webrtc.RTPSender, error) {
		count++
		if count == 2 {
			return nil, want
		}
		return oldAddTrack(peer, track)
	}
	if _, err := view.connectRemote(context.Background(), video, audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	peerAddTrack = oldAddTrack
	if _, err := view.connectRemote(context.Background(), video, audio); err == nil {
		t.Fatal("invalid SDP accepted")
	}
	peerAddTransceiverFromKind = func(*webrtc.PeerConnection, webrtc.RTPCodecType, ...webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
		return nil, want
	}
	if _, err := view.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	count = 0
	peerAddTransceiverFromKind = func(peer *webrtc.PeerConnection, kind webrtc.RTPCodecType, options ...webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
		count++
		if count == 2 {
			return nil, want
		}
		return oldKind(peer, kind, options...)
	}
	if _, err := view.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	peerAddTransceiverFromKind = oldKind
	peerCreateOffer = func(*webrtc.PeerConnection, *webrtc.OfferOptions) (webrtc.SessionDescription, error) {
		return webrtc.SessionDescription{}, want
	}
	signaling, _ := fakeGo2RTCServer(t, false)
	view.go2rtcURL = signaling.URL
	if _, err := view.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	view.close()
	talk := newTalkBridge(context.Background(), "", "", talkBridgeOffer{SDP: "bad"}, func(json.RawMessage) {})
	peerAddTransceiverFromTrack = func(*webrtc.PeerConnection, webrtc.TrackLocal, ...webrtc.RTPTransceiverInit) (*webrtc.RTPTransceiver, error) {
		return nil, want
	}
	if err := talk.connectLocal(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	peerAddTransceiverFromTrack = oldTrack
	peerCreateOffer = oldOffer
	if err := talk.connectRemote(context.Background(), audio); err == nil {
		t.Fatal("invalid SDP accepted")
	}
}

func TestViewStartWaitAndLocalErrors(t *testing.T) {
	oldRemote, oldLocal := connectViewRemote, connectViewLocal
	defer func() { connectViewRemote = oldRemote; connectViewLocal = oldLocal }()
	closed := make(chan struct{})
	close(closed)
	never := make(chan struct{})
	want := errors.New("local")
	connectViewRemote = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP) (<-chan struct{}, error) {
		return closed, nil
	}
	connectViewLocal = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP, string) (<-chan struct{}, error) {
		return nil, want
	}
	if err := newViewBridge(context.Background(), "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, want) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connectViewRemote = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP) (<-chan struct{}, error) {
		return never, nil
	}
	connectViewLocal = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP, string) (<-chan struct{}, error) {
		return never, nil
	}
	if err := newViewBridge(ctx, "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	connectViewRemote = func(*viewBridge, context.Context, *webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP) (<-chan struct{}, error) {
		return closed, nil
	}
	if err := newViewBridge(ctx, "", "", nil, viewBridgeOffer{}, func(json.RawMessage) {}).start(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTalkConnectLocalLateErrors(t *testing.T) {
	server, _ := fakeGo2RTCServer(t, false)
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	oldOffer, oldGather, oldJSON := peerCreateOffer, gatheringCompletePromise, writeWebSocketJSON
	defer func() { peerCreateOffer = oldOffer; gatheringCompletePromise = oldGather; writeWebSocketJSON = oldJSON }()
	want := errors.New("offer")
	peerCreateOffer = func(*webrtc.PeerConnection, *webrtc.OfferOptions) (webrtc.SessionDescription, error) {
		return webrtc.SessionDescription{}, want
	}
	b := newTalkBridge(context.Background(), server.URL, "x", talkBridgeOffer{}, func(json.RawMessage) {})
	if err := b.connectLocal(context.Background(), audio); !errors.Is(err, want) {
		t.Fatal(err)
	}
	b.close()
	peerCreateOffer = oldOffer
	never := make(chan struct{})
	gatheringCompletePromise = func(*webrtc.PeerConnection) <-chan struct{} { return never }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	b = newTalkBridge(ctx, server.URL, "x", talkBridgeOffer{}, func(json.RawMessage) {})
	if err := b.connectLocal(ctx, audio); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	b.close()
	gatheringCompletePromise = oldGather
	writeWebSocketJSON = func(*websocket.Conn, any) error { return nil }
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	b = newTalkBridge(ctx, server.URL, "x", talkBridgeOffer{}, func(json.RawMessage) {})
	if err := b.connectLocal(ctx, audio); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	b.close()
}

func TestViewConnectLocalLateErrors(t *testing.T) {
	video, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "v", "x")
	audio, _ := newRTPTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, "a", "x")
	b := newViewBridge(context.Background(), "%", "x", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err := b.connectLocal(context.Background(), video, audio, "x"); err == nil {
		t.Fatal("malformed signaling URL accepted")
	}
	b.close()

	server, _ := fakeGo2RTCServer(t, false)
	oldJSON, oldRegister := writeWebSocketJSON, registerICECandidate
	defer func() { writeWebSocketJSON = oldJSON; registerICECandidate = oldRegister }()
	writeWebSocketJSON = func(*websocket.Conn, any) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	b = newViewBridge(ctx, server.URL, "x", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err := b.connectLocal(ctx, video, audio, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	b.close()

	candidate := &webrtc.ICECandidate{Foundation: "1", Priority: 1, Address: "127.0.0.1", Protocol: webrtc.ICEProtocolUDP, Port: 9, Typ: webrtc.ICECandidateTypeHost, Component: 1}
	registerICECandidate = func(_ *webrtc.PeerConnection, handler func(*webrtc.ICECandidate)) { handler(candidate) }
	calls := 0
	want := errors.New("queued candidate")
	writeWebSocketJSON = func(*websocket.Conn, any) error {
		calls++
		if calls == 2 {
			return want
		}
		return nil
	}
	b = newViewBridge(context.Background(), server.URL, "x", nil, viewBridgeOffer{}, func(json.RawMessage) {})
	if _, err := b.connectLocal(context.Background(), video, audio, "x"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	b.close()
}
