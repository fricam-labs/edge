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

	mu                 sync.Mutex
	remote             *webrtc.PeerConnection
	local              *webrtc.PeerConnection
	localSocket        *websocket.Conn
	pendingCandidates  []webrtc.ICECandidateInit
	closeOnce          sync.Once
	packetsForwarded   atomic.Uint64
	bytesForwarded     atomic.Uint64
	localVideoSSRC     atomic.Uint32
	startedAt          time.Time
	remoteReady        chan struct{}
	bootstrapSequence  atomic.Uint32
	bootstrapTimestamp atomic.Uint32
	bootstrapWritten   atomic.Bool
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
	return &viewBridge{
		ctx: ctx, cancel: cancel, go2rtcURL: go2rtcURL,
		source: source, bootstrap: bootstrap, offer: offer, send: send, startedAt: time.Now(),
		remoteReady: make(chan struct{}),
	}
}

func (b *viewBridge) mark(phase string) {
	log.Printf("edge view timing source=%s phase=%s elapsed_ms=%d", b.source, phase, time.Since(b.startedAt).Milliseconds())
}

func mediaAPI() (*webrtc.API, error) {
	engine := &webrtc.MediaEngine{}
	if err := engine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	return newWebRTCAPI(engine)
}

func newWebRTCAPI(engine *webrtc.MediaEngine) (*webrtc.API, error) {
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(engine, registry); err != nil {
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
	video, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, ClockRate: 90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		},
		"video", "fricam-edge",
	)
	if err != nil {
		return err
	}
	audio, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1},
		"audio", "fricam-edge",
	)
	if err != nil {
		return err
	}
	remoteConnected, err := b.connectRemote(ctx, video, audio)
	if err != nil {
		return fmt.Errorf("remote WebRTC: %w", err)
	}
	firstVideo, err := b.connectLocal(ctx, video, audio)
	if err != nil {
		return fmt.Errorf("local stream: %w", err)
	}
	select {
	case <-remoteConnected:
		b.mark("remote_connected")
		if b.writeBootstrap(video) {
			b.mark("cached_gop_sent")
			close(b.remoteReady)
			b.mark("bootstrap_video_rtp")
			return nil
		}
		close(b.remoteReady)
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-firstVideo:
		b.mark("first_video_rtp")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	configuration := edgeViewPeerConfiguration(servers)
	for _, server := range servers {
		for _, candidateURL := range server.URLs {
			if strings.HasPrefix(candidateURL, "turn:") || strings.HasPrefix(candidateURL, "turns:") {
				configuration.ICETransportPolicy = webrtc.ICETransportPolicyRelay
				return configuration
			}
		}
	}
	return configuration
}

func (b *viewBridge) connectRemote(
	ctx context.Context,
	video *webrtc.TrackLocalStaticRTP,
	audio *webrtc.TrackLocalStaticRTP,
) (<-chan struct{}, error) {
	api, err := mediaAPI()
	if err != nil {
		return nil, err
	}
	peer, err := api.NewPeerConnection(edgeViewPeerConfiguration(b.offer.ICEServers))
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.remote = peer
	pending := b.pendingCandidates
	b.pendingCandidates = nil
	b.mu.Unlock()

	videoSender, err := peer.AddTrack(video)
	if err != nil {
		return nil, err
	}
	audioSender, err := peer.AddTrack(audio)
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
	peer.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		value := candidate.ToJSON().Candidate
		candidateMu.Lock()
		if !answerSent.Load() {
			pendingLocal = append(pendingLocal, value)
			candidateMu.Unlock()
			return
		}
		candidateMu.Unlock()
		b.sendSignal("webrtc/candidate", value)
	})
	if err := peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: b.offer.SDP}); err != nil {
		return nil, err
	}
	for _, candidate := range pending {
		_ = peer.AddICECandidate(candidate)
	}
	answer, err := peer.CreateAnswer(nil)
	if err != nil {
		return nil, err
	}
	if err := peer.SetLocalDescription(answer); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]interface{}{
		"type": "webrtc", "value": map[string]string{"type": "answer", "sdp": peer.LocalDescription().SDP},
	})
	if err != nil {
		return nil, err
	}
	b.send(payload)
	b.mark("answer_sent")
	candidateMu.Lock()
	answerSent.Store(true)
	queued := append([]string(nil), pendingLocal...)
	pendingLocal = nil
	candidateMu.Unlock()
	for _, candidate := range queued {
		b.sendSignal("webrtc/candidate", candidate)
	}
	return connected, nil
}

func (b *viewBridge) connectLocal(
	ctx context.Context,
	video *webrtc.TrackLocalStaticRTP,
	audio *webrtc.TrackLocalStaticRTP,
) (<-chan struct{}, error) {
	api, err := mediaAPI()
	if err != nil {
		return nil, err
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.local = peer
	b.mu.Unlock()
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		return nil, err
	}
	if _, err = peer.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
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
		var inputBaseSequence uint16
		var inputBaseTimestamp uint32
		var outputBaseSequence uint16
		var outputBaseTimestamp uint32
		for {
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				return
			}
			if track.Kind() == webrtc.RTPCodecTypeVideo && !bootstrapReady {
				select {
				case <-b.remoteReady:
					if b.bootstrapWritten.Load() {
						inputBaseSequence = packet.SequenceNumber
						inputBaseTimestamp = packet.Timestamp
						outputBaseSequence = uint16(b.bootstrapSequence.Load())
						outputBaseTimestamp = b.bootstrapTimestamp.Load()
					}
					bootstrapReady = true
				default:
					// The cache is sampled only after remote ICE connects, so these
					// pre-ready packets are already represented in the fresh GOP.
					continue
				}
			}
			if track.Kind() == webrtc.RTPCodecTypeVideo && b.bootstrapWritten.Load() {
				packet.SequenceNumber = outputBaseSequence + (packet.SequenceNumber - inputBaseSequence)
				packet.Timestamp = outputBaseTimestamp + (packet.Timestamp - inputBaseTimestamp)
			}
			if writeErr := destination.WriteRTP(packet); writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
				return
			}
			b.packetsForwarded.Add(1)
			b.bytesForwarded.Add(uint64(len(packet.Payload)))
			if track.Kind() == webrtc.RTPCodecTypeVideo {
				firstVideoOnce.Do(func() {
					b.mark("live_video_rtp")
					close(firstVideo)
				})
			}
		}
	})
	query := url.Values{"src": []string{b.source}}
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
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		return nil, err
	}
	gathering := webrtc.GatheringCompletePromise(peer)
	if err := peer.SetLocalDescription(offer); err != nil {
		return nil, err
	}
	select {
	case <-gathering:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := socket.WriteJSON(map[string]string{
		"type": "webrtc/offer", "value": peer.LocalDescription().SDP,
	}); err != nil {
		return nil, err
	}
	answerSet := make(chan struct{}, 1)
	failed := make(chan error, 1)
	go b.readLocalSignals(peer, socket, answerSet, failed)
	select {
	case <-answerSet:
		b.mark("local_answer_set")
		return firstVideo, nil
	case err := <-failed:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *viewBridge) writeBootstrap(destination *webrtc.TrackLocalStaticRTP) bool {
	if b.bootstrap == nil {
		return false
	}
	bootstrap := b.bootstrap()
	if len(bootstrap) == 0 {
		return false
	}
	packetizer := rtp.NewPacketizer(
		1200, 96, 1, &codecs.H264Payloader{}, rtp.NewFixedSequencer(1), 90000,
	)
	var lastSequence uint16
	var lastTimestamp uint32
	for _, accessUnit := range bootstrap {
		for _, packet := range packetizer.Packetize(accessUnit, 3000) {
			if err := destination.WriteRTP(packet); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				return false
			}
			lastSequence = packet.SequenceNumber
			lastTimestamp = packet.Timestamp
		}
	}
	b.bootstrapSequence.Store(uint32(lastSequence + 1))
	b.bootstrapTimestamp.Store(lastTimestamp + 3000)
	b.bootstrapWritten.Store(true)
	return true
}

// Forward decoder keyframe requests from Android to the local go2rtc peer.
// Dropping these messages makes first-frame latency depend on the camera GOP,
// which can vary by several seconds even after ICE is already connected.
func (b *viewBridge) forwardVideoRTCP(sender *webrtc.RTPSender) {
	for {
		packets, _, err := sender.ReadRTCP()
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
