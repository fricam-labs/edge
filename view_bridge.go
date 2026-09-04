package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
)

const viewBridgeTimeout = 15 * time.Second

type viewBridgeOffer = talkBridgeOffer

var newRTPTrack = webrtc.NewTrackLocalStaticRTP
var mediaAPIFactory = mediaAPI
var pcmaAPIFactory = pcmaAPI
var newPeerConnection = func(api *webrtc.API, configuration webrtc.Configuration) (*webrtc.PeerConnection, error) {
	return api.NewPeerConnection(configuration)
}
var registerWebRTCCodec = func(engine *webrtc.MediaEngine, parameters webrtc.RTPCodecParameters, kind webrtc.RTPCodecType) error {
	return engine.RegisterCodec(parameters, kind)
}
var registerWebRTCInterceptors = webrtc.RegisterDefaultInterceptors
var peerAddTrack = (*webrtc.PeerConnection).AddTrack
var peerAddTransceiverFromKind = (*webrtc.PeerConnection).AddTransceiverFromKind
var peerAddTransceiverFromTrack = (*webrtc.PeerConnection).AddTransceiverFromTrack
var peerCreateOffer = (*webrtc.PeerConnection).CreateOffer
var peerCreateAnswer = (*webrtc.PeerConnection).CreateAnswer
var peerSetLocalDescription = (*webrtc.PeerConnection).SetLocalDescription
var peerSetRemoteDescription = (*webrtc.PeerConnection).SetRemoteDescription
var readSenderRTCP = (*webrtc.RTPSender).ReadRTCP
var writeLocalRTP = (*webrtc.TrackLocalStaticRTP).WriteRTP
var writeWebSocketJSON = (*websocket.Conn).WriteJSON
var writeWebSocketMessage = (*websocket.Conn).WriteMessage
var gatheringCompletePromise = webrtc.GatheringCompletePromise
var connectViewRemote = (*viewBridge).connectRemote
var connectViewLocal = (*viewBridge).connectLocal
var registerICECandidate = (*webrtc.PeerConnection).OnICECandidate

func candidateHandler(sent *atomic.Bool, mu *sync.Mutex, pending *[]string, send func(string) error) func(*webrtc.ICECandidate) {
	return func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		value := candidate.ToJSON().Candidate
		mu.Lock()
		if !sent.Load() {
			*pending = append(*pending, value)
			mu.Unlock()
			return
		}
		mu.Unlock()
		_ = send(value)
	}
}

func flushCandidates(pending []string, send func(string) error) error {
	for _, candidate := range pending {
		if err := send(candidate); err != nil {
			return err
		}
	}
	return nil
}

func waitContext(ready <-chan struct{}, ctx context.Context) error {
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitPeer(ready <-chan struct{}, failed <-chan error, ctx context.Context) error {
	select {
	case <-ready:
		return nil
	case err := <-failed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseViewBridgeOffer(payload []byte) (viewBridgeOffer, bool) {
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(payload, &signal) != nil || signal.Type != "webrtc" {
		return viewBridgeOffer{}, false
	}
	var offer viewBridgeOffer
	if json.Unmarshal(signal.Value, &offer) != nil || offer.Type != "offer" ||
		offer.SDP == "" || len(offer.ICEServers) == 0 {
		return viewBridgeOffer{}, false
	}
	hasVideo := false
	for _, line := range strings.Split(strings.ReplaceAll(offer.SDP, "\r\n", "\n"), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "m=video ") {
			hasVideo = true
			break
		}
	}
	return offer, hasVideo
}

type viewBridge struct {
	ctx       context.Context
	cancel    context.CancelFunc
	go2rtcURL string
	source    string
	offer     viewBridgeOffer
	bootstrap func() [][]byte
	send      func(json.RawMessage)
	writeRTP  func(*webrtc.TrackLocalStaticRTP, *rtp.Packet) error

	mu                sync.Mutex
	mediaWriteMu      sync.Mutex
	remote            *webrtc.PeerConnection
	local             *webrtc.PeerConnection
	localSocket       *websocket.Conn
	video             *webrtc.TrackLocalStaticRTP
	audio             *webrtc.TrackLocalStaticRTP
	pendingCandidates []webrtc.ICECandidateInit
	closeOnce         sync.Once
	packetsForwarded  atomic.Uint64
	bytesForwarded    atomic.Uint64
	localVideoSSRC    atomic.Uint32
	startedAt         time.Time
	remoteReady       chan struct{}
	bootstrapWritten  atomic.Bool
	waitForVideoIDR   atomic.Bool
	paused            atomic.Bool
	videoContinuity   videoRTPContinuity
}

// videoRTPContinuity owns the RTP timeline sent to Android. Local go2rtc
// packets keep arriving while a warm session is paused, so forwarding their
// original sequence/timestamp after a cached GOP would create a large gap.
// Rebase every live epoch immediately after the last emitted bootstrap frame.
// All access is protected by viewBridge.mediaWriteMu.
type videoRTPContinuity struct {
	initialized         bool
	inputReady          bool
	nextSequence        uint16
	nextTimestamp       uint32
	lastInputTimestamp  uint32
	lastOutputTimestamp uint32
	timestampStep       uint32
}

func (c *videoRTPContinuity) rewriteLive(packet *rtp.Packet) {
	inputTimestamp := packet.Timestamp
	if !c.initialized {
		c.initialized = true
		c.nextSequence = packet.SequenceNumber
		c.nextTimestamp = packet.Timestamp
		c.timestampStep = 3000
	}
	outputTimestamp := c.nextTimestamp
	if c.inputReady {
		delta := inputTimestamp - c.lastInputTimestamp
		if delta == 0 {
			outputTimestamp = c.lastOutputTimestamp
		} else {
			outputTimestamp = c.lastOutputTimestamp + delta
			// Ignore implausible discontinuities when learning frame cadence.
			if delta < 90000 {
				c.timestampStep = delta
			}
		}
	}
	packet.SequenceNumber = c.nextSequence
	packet.Timestamp = outputTimestamp
	c.nextSequence++
	c.lastInputTimestamp = inputTimestamp
	c.lastOutputTimestamp = outputTimestamp
	c.inputReady = true
	c.nextTimestamp = outputTimestamp + c.timestampStep
}

func (c *videoRTPContinuity) beginBootstrap() (uint16, uint32) {
	if !c.initialized {
		c.initialized = true
		c.nextSequence = 1
		c.nextTimestamp = 1
		c.timestampStep = 3000
	}
	return c.nextSequence, c.nextTimestamp
}

func (c *videoRTPContinuity) finishBootstrap(nextSequence uint16, nextTimestamp uint32) {
	c.nextSequence = nextSequence
	c.nextTimestamp = nextTimestamp
	// The next local packet belongs to a new live epoch. Its input timestamp
	// must be anchored to nextTimestamp rather than compared with packets that
	// were discarded while paused.
	c.inputReady = false
}

func newViewBridge(
	parent context.Context,
	go2rtcURL string,
	source string,
	bootstrap func() [][]byte,
	offer viewBridgeOffer,
	send func(json.RawMessage),
) *viewBridge {
	ctx, cancel := context.WithCancel(parent)
	bridge := &viewBridge{
		ctx: ctx, cancel: cancel, go2rtcURL: go2rtcURL,
		source: source, bootstrap: bootstrap, offer: offer, send: send, writeRTP: writeLocalRTP, startedAt: time.Now(),
		remoteReady: make(chan struct{}),
	}
	// Starting paused is part of the offer so it is applied before either peer
	// can deliver media. A follow-up pause message would race the first GOP.
	bridge.paused.Store(offer.WarmPaused)
	return bridge
}

func (b *viewBridge) mark(phase string) {
	b.mu.Lock()
	source := b.source
	b.mu.Unlock()
	log.Printf("edge view timing source=%s phase=%s elapsed_ms=%d", source, phase, time.Since(b.startedAt).Milliseconds())
}

func mediaAPI() (*webrtc.API, error) {
	engine := &webrtc.MediaEngine{}
	codecs := []struct {
		parameters webrtc.RTPCodecParameters
		kind       webrtc.RTPCodecType
	}{
		{webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, ClockRate: 90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		}, PayloadType: 96}, webrtc.RTPCodecTypeVideo},
		{webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1,
		}, PayloadType: 8}, webrtc.RTPCodecTypeAudio},
		{webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		}, PayloadType: 111}, webrtc.RTPCodecTypeAudio},
	}
	for _, codec := range codecs {
		if err := registerWebRTCCodec(engine, codec.parameters, codec.kind); err != nil {
			return nil, err
		}
	}
	return newWebRTCAPI(engine)
}

func newWebRTCAPI(engine *webrtc.MediaEngine) (*webrtc.API, error) {
	registry := &interceptor.Registry{}
	if err := registerWebRTCInterceptors(engine, registry); err != nil {
		return nil, err
	}
	setting := webrtc.SettingEngine{}
	setting.SetInterfaceFilter(func(name string) bool {
		name = strings.ToLower(name)
		return name != "lo" && !strings.HasPrefix(name, "docker") &&
			!strings.HasPrefix(name, "br-") && !strings.HasPrefix(name, "veth")
	})
	return webrtc.NewAPI(
		webrtc.WithMediaEngine(engine),
		webrtc.WithInterceptorRegistry(registry),
		webrtc.WithSettingEngine(setting),
	), nil
}

func (b *viewBridge) start() error {
	ctx, cancel := context.WithTimeout(b.ctx, viewBridgeTimeout)
	defer cancel()
	video, err := newRTPTrack(
		webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, ClockRate: 90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		},
		"video", "fricam-edge",
	)
	if err != nil {
		return err
	}
	audio, err := newRTPTrack(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1},
		"audio", "fricam-edge",
	)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.video, b.audio = video, audio
	b.mu.Unlock()
	remoteConnected, err := connectViewRemote(b, ctx, video, audio)
	if err != nil {
		return fmt.Errorf("remote WebRTC: %w", err)
	}
	b.mu.Lock()
	source := b.source
	b.mu.Unlock()
	firstVideo, err := connectViewLocal(b, ctx, video, audio, source)
	if err != nil {
		return fmt.Errorf("local stream: %w", err)
	}
	if err := waitContext(remoteConnected, ctx); err != nil {
		return err
	}
	b.mark("remote_connected")
	if b.paused.Load() {
		close(b.remoteReady)
		b.mark("warm_paused")
		return nil
	}
	if b.writeBootstrap(video) {
		b.mark("cached_gop_sent")
		close(b.remoteReady)
		b.mark("bootstrap_video_rtp")
		return nil
	}
	close(b.remoteReady)
	return waitContext(firstVideo, ctx)
}

func edgeICEServers(servers []edgeICEServer) []webrtc.ICEServer {
	result := make([]webrtc.ICEServer, 0, len(servers))
	for _, server := range servers {
		result = append(result, webrtc.ICEServer{
			URLs: server.URLs, Username: server.Username,
			Credential: server.Credential, CredentialType: webrtc.ICECredentialTypePassword,
		})
	}
	return result
}

func edgeViewPeerConfiguration(servers []edgeICEServer) webrtc.Configuration {
	// Android remains relay-only for predictable cellular connectivity. Let the
	// always-on Edge endpoint advertise host/srflx candidates so the selected
	// pair needs only one TURN allocation instead of relaying both endpoints.
	return webrtc.Configuration{ICEServers: edgeICEServers(servers)}
}

func edgeTalkPeerConfiguration(servers []edgeICEServer) webrtc.Configuration {
	return edgeViewPeerConfiguration(servers)
}

func (b *viewBridge) connectRemote(
	ctx context.Context,
	video *webrtc.TrackLocalStaticRTP,
	audio *webrtc.TrackLocalStaticRTP,
) (<-chan struct{}, error) {
	api, err := mediaAPIFactory()
	if err != nil {
		return nil, err
	}
	peer, err := newPeerConnection(api, edgeViewPeerConfiguration(b.offer.ICEServers))
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.remote = peer
	pending := b.pendingCandidates
	b.pendingCandidates = nil
	b.mu.Unlock()

	videoSender, err := peerAddTrack(peer, video)
	if err != nil {
		return nil, err
	}
	audioSender, err := peerAddTrack(peer, audio)
	if err != nil {
		return nil, err
	}
	go b.forwardVideoRTCP(videoSender)
	go drainRTCP(audioSender)
	connected := make(chan struct{})
	var connectedOnce sync.Once
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})
	answerSent := atomic.Bool{}
	var candidateMu sync.Mutex
	pendingLocal := make([]string, 0, 8)
	registerICECandidate(peer, candidateHandler(&answerSent, &candidateMu, &pendingLocal, func(value string) error {
		b.sendSignal("webrtc/candidate", value)
		return nil
	}))
	if err := peerSetRemoteDescription(peer, webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: b.offer.SDP}); err != nil {
		return nil, err
	}
	for _, candidate := range pending {
		_ = peer.AddICECandidate(candidate)
	}
	answer, err := peerCreateAnswer(peer, nil)
	if err != nil {
		return nil, err
	}
	if err := peerSetLocalDescription(peer, answer); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"type": "webrtc", "value": map[string]string{"type": "answer", "sdp": peer.LocalDescription().SDP},
	})
	b.send(payload)
	b.mark("answer_sent")
	candidateMu.Lock()
	answerSent.Store(true)
	queued := append([]string(nil), pendingLocal...)
	pendingLocal = nil
	candidateMu.Unlock()
	_ = flushCandidates(queued, b.sendCandidate)
	return connected, nil
}

func (b *viewBridge) connectLocal(
	ctx context.Context,
	video *webrtc.TrackLocalStaticRTP,
	audio *webrtc.TrackLocalStaticRTP,
	source string,
) (<-chan struct{}, error) {
	api, err := mediaAPIFactory()
	if err != nil {
		return nil, err
	}
	peer, err := newPeerConnection(api, webrtc.Configuration{})
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.local = peer
	b.mu.Unlock()
	if _, err = peerAddTransceiverFromKind(peer, webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		return nil, err
	}
	if _, err = peerAddTransceiverFromKind(peer, webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		return nil, err
	}
	firstVideo := make(chan struct{})
	var firstVideoOnce sync.Once
	peer.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		destination := audio
		if track.Kind() == webrtc.RTPCodecTypeVideo {
			destination = video
			b.localVideoSSRC.Store(uint32(track.SSRC()))
			// Do not wait for the camera's GOP interval. Ask for a fresh IDR as
			// soon as the warm go2rtc producer is attached to this consumer.
			_ = peer.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{
				MediaSSRC: uint32(track.SSRC()),
			}})
		}
		bootstrapReady := false
		for {
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				return
			}
			if !b.forwardLocalPacket(track.Kind(), destination, packet, &bootstrapReady, &firstVideoOnce, firstVideo) {
				return
			}
		}
	})
	query := url.Values{"src": []string{source}}
	endpoint := strings.NewReplacer("http://", "ws://", "https://", "wss://").Replace(b.go2rtcURL) +
		"/api/ws?" + query.Encode()
	socket, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if response != nil {
			return nil, fmt.Errorf("signaling returned %s", response.Status)
		}
		return nil, err
	}
	socket.SetReadLimit(edgeMaxSignalBytes)
	b.mu.Lock()
	b.localSocket = socket
	b.mu.Unlock()
	var socketWriteMu sync.Mutex
	writeSignal := func(signal map[string]string) error {
		socketWriteMu.Lock()
		defer socketWriteMu.Unlock()
		return writeWebSocketJSON(socket, signal)
	}
	offerSent := atomic.Bool{}
	var candidateMu sync.Mutex
	pendingLocal := make([]string, 0, 8)
	sendCandidate := func(value string) error {
		return writeSignal(map[string]string{"type": "webrtc/candidate", "value": value})
	}
	registerICECandidate(peer, candidateHandler(&offerSent, &candidateMu, &pendingLocal, sendCandidate))
	offer, err := peerCreateOffer(peer, nil)
	if err != nil {
		return nil, err
	}
	if err := peerSetLocalDescription(peer, offer); err != nil {
		return nil, err
	}
	if err := writeSignal(map[string]string{
		"type": "webrtc/offer", "value": peer.LocalDescription().SDP,
	}); err != nil {
		return nil, err
	}
	candidateMu.Lock()
	offerSent.Store(true)
	queued := append([]string(nil), pendingLocal...)
	pendingLocal = nil
	candidateMu.Unlock()
	if err := flushCandidates(queued, sendCandidate); err != nil {
		return nil, err
	}
	answerSet := make(chan struct{}, 1)
	failed := make(chan error, 1)
	go b.readLocalSignals(peer, socket, answerSet, failed)
	if err := waitPeer(answerSet, failed, ctx); err != nil {
		return nil, err
	}
	b.mark("local_answer_set")
	return firstVideo, nil
}

func (b *viewBridge) forwardLocalPacket(kind webrtc.RTPCodecType, destination *webrtc.TrackLocalStaticRTP, packet *rtp.Packet, bootstrapReady *bool, firstVideoOnce *sync.Once, firstVideo chan struct{}) bool {
	// During a warm source switch, wait until edge/resume has written the new
	// source's cached GOP before establishing sequence/timestamp continuity.
	if b.paused.Load() {
		return true
	}
	if kind == webrtc.RTPCodecTypeVideo && !*bootstrapReady {
		select {
		case <-b.remoteReady:
			*bootstrapReady = true
		default:
			return true
		}
	}
	if kind == webrtc.RTPCodecTypeVideo && b.waitForVideoIDR.Load() {
		if !h264RTPStartsIDR(packet.Payload) {
			return true
		}
		b.waitForVideoIDR.Store(false)
	}
	b.mediaWriteMu.Lock()
	if kind == webrtc.RTPCodecTypeVideo {
		b.videoContinuity.rewriteLive(packet)
	}
	writeErr := b.writeRTP(destination, packet)
	b.mediaWriteMu.Unlock()
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return false
	}
	b.packetsForwarded.Add(1)
	b.bytesForwarded.Add(uint64(len(packet.Payload)))
	if kind == webrtc.RTPCodecTypeVideo {
		firstVideoOnce.Do(func() {
			b.mark("first_video_rtp")
			close(firstVideo)
		})
	}
	return true
}

// h264RTPStartsIDR recognizes the packet that starts an IDR access unit for
// single-NALU, STAP-A and FU-A payloads. A later FU-A fragment is not decodable.
func h264RTPStartsIDR(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	switch payload[0] & 0x1f {
	case 5:
		return true
	case 24: // STAP-A
		for offset := 1; offset+2 <= len(payload); {
			size := int(payload[offset])<<8 | int(payload[offset+1])
			offset += 2
			if size == 0 || offset+size > len(payload) {
				return false
			}
			if payload[offset]&0x1f == 5 {
				return true
			}
			offset += size
		}
	case 28: // FU-A
		return len(payload) >= 2 && payload[1]&0x80 != 0 && payload[1]&0x1f == 5
	}
	return false
}

func (b *viewBridge) writeBootstrap(destination *webrtc.TrackLocalStaticRTP) bool {
	b.mu.Lock()
	provider := b.bootstrap
	b.mu.Unlock()
	if provider == nil {
		return false
	}
	bootstrap := provider()
	if len(bootstrap) == 0 {
		return false
	}
	payloader := &codecs.H264Payloader{}
	b.mediaWriteMu.Lock()
	defer b.mediaWriteMu.Unlock()
	sequence, timestamp := b.videoContinuity.beginBootstrap()
	for _, accessUnit := range bootstrap {
		packets := packetizeBootstrapAccessUnit(payloader, accessUnit, sequence, timestamp)
		for _, packet := range packets {
			if err := writeLocalRTP(destination, packet); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				return false
			}
			sequence++
		}
		timestamp += 3000
	}
	b.videoContinuity.finishBootstrap(sequence, timestamp)
	b.bootstrapWritten.Store(true)
	// A cached GOP renders immediately, but dependent frames from another GOP
	// cannot safely follow it. Join every following live epoch at a fresh IDR.
	b.waitForVideoIDR.Store(true)
	return true
}

func packetizeBootstrapAccessUnit(
	payloader *codecs.H264Payloader,
	accessUnit []byte,
	sequence uint16,
	timestamp uint32,
) []*rtp.Packet {
	payloads := payloader.Payload(1200, accessUnit)
	packets := make([]*rtp.Packet, 0, len(payloads))
	for index, payload := range payloads {
		packets = append(packets, &rtp.Packet{Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: sequence + uint16(index),
			Timestamp: timestamp, SSRC: 1, Marker: index == len(payloads)-1,
		}, Payload: payload})
	}
	return packets
}

func (b *viewBridge) setPaused(paused bool) {
	if paused {
		b.paused.Store(true)
		return
	}
	b.mu.Lock()
	video := b.video
	b.mu.Unlock()
	if video != nil {
		_ = b.writeBootstrap(video)
	}
	b.paused.Store(false)
	b.mark("warm_resumed")
}

// Forward decoder keyframe requests from Android to the local go2rtc peer.
// Dropping these messages makes first-frame latency depend on the camera GOP,
// which can vary by several seconds even after ICE is already connected.
func (b *viewBridge) forwardVideoRTCP(sender *webrtc.RTPSender) {
	for {
		packets, _, err := readSenderRTCP(sender)
		if err != nil {
			return
		}
		needsKeyframe := false
		for _, packet := range packets {
			switch packet.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				needsKeyframe = true
			}
		}
		if !needsKeyframe {
			continue
		}
		b.mu.Lock()
		local := b.local
		b.mu.Unlock()
		ssrc := b.localVideoSSRC.Load()
		if local != nil && ssrc != 0 {
			_ = local.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}})
		}
	}
}

func (b *viewBridge) readLocalSignals(
	peer *webrtc.PeerConnection,
	socket *websocket.Conn,
	answerSet chan<- struct{},
	failed chan<- error,
) {
	for {
		_, payload, err := socket.ReadMessage()
		if err != nil {
			select {
			case failed <- err:
			default:
			}
			return
		}
		var signal struct {
			Type  string          `json:"type"`
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(payload, &signal) != nil {
			continue
		}
		switch signal.Type {
		case "webrtc/answer":
			var sdp string
			if json.Unmarshal(signal.Value, &sdp) == nil &&
				peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}) == nil {
				select {
				case answerSet <- struct{}{}:
				default:
				}
			}
		case "webrtc/candidate":
			var candidate string
			if json.Unmarshal(signal.Value, &candidate) == nil {
				_ = peer.AddICECandidate(webrtc.ICECandidateInit{Candidate: strings.TrimPrefix(candidate, "a=")})
			}
		}
	}
}

func (b *viewBridge) sendSignal(kind, value string) {
	payload, err := json.Marshal(map[string]string{"type": kind, "value": value})
	if err == nil {
		b.send(payload)
	}
}

func (b *viewBridge) sendCandidate(value string) error {
	b.sendSignal("webrtc/candidate", value)
	return nil
}

func (b *viewBridge) addClientSignal(payload []byte) {
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(payload, &signal) != nil || signal.Type != "webrtc/candidate" {
		return
	}
	var value string
	if json.Unmarshal(signal.Value, &value) != nil || value == "" {
		return
	}
	candidate := webrtc.ICECandidateInit{Candidate: strings.TrimPrefix(value, "a=")}
	b.mu.Lock()
	peer := b.remote
	if peer == nil {
		b.pendingCandidates = append(b.pendingCandidates, candidate)
	}
	b.mu.Unlock()
	if peer != nil {
		_ = peer.AddICECandidate(candidate)
	}
}

func (b *viewBridge) close() {
	b.closeOnce.Do(func() {
		b.cancel()
		b.mu.Lock()
		remote, local, socket := b.remote, b.local, b.localSocket
		b.mu.Unlock()
		if socket != nil {
			_ = socket.Close()
		}
		if remote != nil {
			_ = remote.Close()
		}
		if local != nil {
			_ = local.Close()
		}
	})
}

func (b *viewBridge) forwardedMedia() (uint64, uint64) {
	return b.packetsForwarded.Load(), b.bytesForwarded.Load()
}
