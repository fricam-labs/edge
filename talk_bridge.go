package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

const talkBridgeTimeout = 20 * time.Second

type talkBridgeOffer struct {
	Type       string          `json:"type"`
	SDP        string          `json:"sdp"`
	ICEServers []edgeICEServer `json:"ice_servers"`
	WarmPaused bool            `json:"warm_paused,omitempty"`
}

type talkBridge struct {
	ctx       context.Context
	cancel    context.CancelFunc
	go2rtcURL string
	source    string
	offer     talkBridgeOffer
	send      func(json.RawMessage)

	mu                sync.Mutex
	remote            *webrtc.PeerConnection
	local             *webrtc.PeerConnection
	localSocket       *websocket.Conn
	pendingCandidates []webrtc.ICECandidateInit
	closeOnce         sync.Once
	packetsForwarded  atomic.Uint64
	bytesForwarded    atomic.Uint64
}

func parseTalkBridgeOffer(payload []byte) (talkBridgeOffer, bool) {
	var signal struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(payload, &signal) != nil || signal.Type != "webrtc" {
		return talkBridgeOffer{}, false
	}
	var offer talkBridgeOffer
	if json.Unmarshal(signal.Value, &offer) != nil || offer.Type != "offer" ||
		offer.SDP == "" || len(offer.ICEServers) == 0 || !audioSendOnlySDP(offer.SDP) {
		return talkBridgeOffer{}, false
	}
	return offer, true
}

func audioSendOnlySDP(sdp string) bool {
	hasAudio := false
	hasSendOnly := false
	for _, rawLine := range strings.Split(strings.ReplaceAll(sdp, "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(rawLine)
		switch {
		case strings.HasPrefix(line, "m=audio "):
			hasAudio = true
		case strings.HasPrefix(line, "m=") && !strings.HasPrefix(line, "m=audio "):
			return false
		case line == "a=sendonly":
			hasSendOnly = true
		}
	}
	return hasAudio && hasSendOnly
}

func newTalkBridge(
	parent context.Context,
	go2rtcURL string,
	source string,
	offer talkBridgeOffer,
	send func(json.RawMessage),
) *talkBridge {
	ctx, cancel := context.WithCancel(parent)
	return &talkBridge{
		ctx: ctx, cancel: cancel, go2rtcURL: go2rtcURL,
		source: source, offer: offer, send: send,
	}
}

func (b *talkBridge) start() error {
	ctx, cancel := context.WithTimeout(b.ctx, talkBridgeTimeout)
	defer cancel()
	localTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1},
		"microphone", "fricam-edge",
	)
	if err != nil {
		return err
	}
	if err := b.connectLocal(ctx, localTrack); err != nil {
		return fmt.Errorf("local backchannel: %w", err)
	}
	if err := b.connectRemote(ctx, localTrack); err != nil {
		return fmt.Errorf("remote WebRTC: %w", err)
	}
	return nil
}

func pcmaAPI() (*webrtc.API, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1,
		},
		PayloadType: 8,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	return newWebRTCAPI(mediaEngine)
}

func (b *talkBridge) connectLocal(ctx context.Context, track *webrtc.TrackLocalStaticRTP) error {
	api, err := pcmaAPI()
	if err != nil {
		return err
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.local = peer
	b.mu.Unlock()

	transceiver, err := peer.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		return err
	}
	go drainRTCP(transceiver.Sender())

	connected := make(chan struct{}, 1)
	failed := make(chan error, 1)
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			select {
			case connected <- struct{}{}:
			default:
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			select {
			case failed <- fmt.Errorf("local peer state %s", state):
			default:
			}
		}
	})

	query := url.Values{"src": []string{b.source}}
	endpoint := strings.NewReplacer("http://", "ws://", "https://", "wss://").Replace(b.go2rtcURL) +
		"/api/ws?" + query.Encode()
	socket, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if response != nil {
			return fmt.Errorf("signaling returned %s", response.Status)
		}
		return err
	}
	socket.SetReadLimit(edgeMaxSignalBytes)
	b.mu.Lock()
	b.localSocket = socket
	b.mu.Unlock()

	offer, err := peer.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathering := webrtc.GatheringCompletePromise(peer)
	if err := peer.SetLocalDescription(offer); err != nil {
		return err
	}
	select {
	case <-gathering:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := socket.WriteJSON(map[string]string{
		"type": "webrtc/offer", "value": peer.LocalDescription().SDP,
	}); err != nil {
		return err
	}
	answerSet := make(chan struct{}, 1)
	go b.readLocalSignals(peer, socket, answerSet, failed)
	select {
	case <-answerSet:
	case err := <-failed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-connected:
		return nil
	case err := <-failed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *talkBridge) readLocalSignals(
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
			if json.Unmarshal(signal.Value, &sdp) == nil {
				if err := peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
					select {
					case failed <- err:
					default:
					}
					return
				}
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

func (b *talkBridge) connectRemote(ctx context.Context, localTrack *webrtc.TrackLocalStaticRTP) error {
	api, err := pcmaAPI()
	if err != nil {
		return err
	}
	servers := make([]webrtc.ICEServer, 0, len(b.offer.ICEServers))
	for _, server := range b.offer.ICEServers {
		servers = append(servers, webrtc.ICEServer{
			URLs: server.URLs, Username: server.Username,
			Credential: server.Credential, CredentialType: webrtc.ICECredentialTypePassword,
		})
	}
	configuration := edgeTalkPeerConfiguration(b.offer.ICEServers)
	configuration.ICEServers = servers
	peer, err := api.NewPeerConnection(configuration)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.remote = peer
	pending := b.pendingCandidates
	b.pendingCandidates = nil
	b.mu.Unlock()

	trackStarted := make(chan struct{}, 1)
	peer.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackStarted <- struct{}{}:
		default:
		}
		for {
			packet, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			if err := localTrack.WriteRTP(packet); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				return
			}
			b.packetsForwarded.Add(1)
			b.bytesForwarded.Add(uint64(len(packet.Payload)))
		}
	})
	failed := make(chan error, 1)
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			select {
			case failed <- fmt.Errorf("remote peer state %s", state):
			default:
			}
		}
	})
	if err := peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: b.offer.SDP}); err != nil {
		return err
	}
	for _, candidate := range pending {
		_ = peer.AddICECandidate(candidate)
	}
	answer, err := peer.CreateAnswer(nil)
	if err != nil {
		return err
	}
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
	if err := peer.SetLocalDescription(answer); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]interface{}{
		"type":  "webrtc",
		"value": map[string]string{"type": "answer", "sdp": peer.LocalDescription().SDP},
	})
	if err != nil {
		return err
	}
	b.send(payload)
	candidateMu.Lock()
	answerSent.Store(true)
	queued := append([]string(nil), pendingLocal...)
	pendingLocal = nil
	candidateMu.Unlock()
	for _, candidate := range queued {
		b.sendSignal("webrtc/candidate", candidate)
	}
	select {
	case <-trackStarted:
		return nil
	case err := <-failed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *talkBridge) sendSignal(kind, value string) {
	payload, err := json.Marshal(map[string]string{"type": kind, "value": value})
	if err == nil {
		b.send(payload)
	}
}

func (b *talkBridge) addClientSignal(payload []byte) {
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

func (b *talkBridge) close() {
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

func (b *talkBridge) forwardedMedia() (uint64, uint64) {
	return b.packetsForwarded.Load(), b.bytesForwarded.Load()
}

func drainRTCP(sender *webrtc.RTPSender) {
	buffer := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buffer); err != nil {
			return
		}
	}
}
