package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const clientTokenContext = "fricam-edge-client-v1"

type identity struct {
	DeviceID   string `json:"device_id"`
	RootSecret string `json:"root_secret"`
}

type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type sessionResponse struct {
	ICEServers   []iceServer `json:"iceServers"`
	SignalingURL string      `json:"signalingUrl"`
}

type accessResponse struct {
	AccessToken string `json:"accessToken"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "edge talk e2e failed:", err)
		os.Exit(1)
	}
}

func run() error {
	stream := env("EDGE_TEST_STREAM", "kapi_onu_talk")
	relayURL := strings.TrimRight(env("EDGE_RELAY_URL", "https://relay.fricam.app"), "/")
	rawIdentity, err := os.ReadFile(env("EDGE_IDENTITY_FILE", "/identity/identity.json"))
	if err != nil {
		return fmt.Errorf("read identity: %w", err)
	}
	var edge identity
	if err := json.Unmarshal(rawIdentity, &edge); err != nil || edge.DeviceID == "" || edge.RootSecret == "" {
		return errors.New("invalid Edge identity")
	}
	personalToken, err := bufio.NewReader(io.LimitReader(os.Stdin, 4096)).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read test entitlement: %w", err)
	}
	personalToken = strings.TrimSpace(personalToken)
	if personalToken == "" {
		return errors.New("test entitlement is empty")
	}
	clientToken := deriveClientToken(edge.RootSecret)
	access, err := exchangeAccess(relayURL, edge.DeviceID, clientToken, personalToken)
	personalToken = ""
	if err != nil {
		return err
	}
	session, rawICE, err := createSession(relayURL, edge.DeviceID, access)
	if err != nil {
		return err
	}
	return relayOnlyTalk(edge.DeviceID, stream, access, session, rawICE)
}

func deriveClientToken(root string) string {
	mac := hmac.New(sha256.New, []byte(root))
	_, _ = mac.Write([]byte(clientTokenContext))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func exchangeAccess(relayURL, deviceID, clientToken, personalToken string) (string, error) {
	body, _ := json.Marshal(map[string]string{"personalAccessToken": personalToken})
	request, _ := http.NewRequest(http.MethodPost, relayURL+"/edge/entitlement/"+deviceID, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+clientToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("entitlement returned %s", response.Status)
	}
	var payload accessResponse
	if json.NewDecoder(response.Body).Decode(&payload) != nil || payload.AccessToken == "" {
		return "", errors.New("invalid entitlement response")
	}
	return payload.AccessToken, nil
}

func createSession(relayURL, deviceID, access string) (sessionResponse, json.RawMessage, error) {
	request, _ := http.NewRequest(http.MethodGet, relayURL+"/edge/session/"+deviceID, nil)
	request.Header.Set("Authorization", "Bearer "+access)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return sessionResponse{}, nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return sessionResponse{}, nil, fmt.Errorf("session returned %s", response.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return sessionResponse{}, nil, err
	}
	var payload sessionResponse
	if json.Unmarshal(raw, &payload) != nil || payload.SignalingURL == "" || len(payload.ICEServers) == 0 {
		return sessionResponse{}, nil, errors.New("invalid session response")
	}
	var envelope struct {
		ICEServers json.RawMessage `json:"iceServers"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return payload, envelope.ICEServers, nil
}

func relayOnlyTalk(_ string, stream, access string, session sessionResponse, rawICE json.RawMessage) error {
	servers := make([]webrtc.ICEServer, 0, len(session.ICEServers))
	for _, server := range session.ICEServers {
		servers = append(servers, webrtc.ICEServer{
			URLs: server.URLs, Username: server.Username,
			Credential: server.Credential, CredentialType: webrtc.ICECredentialTypePassword,
		})
	}
	peer, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: servers, ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	})
	if err != nil {
		return err
	}
	defer peer.Close()
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1},
		"microphone", "fricam-edge-e2e",
	)
	if err != nil {
		return err
	}
	transceiver, err := peer.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		return err
	}
	sender := transceiver.Sender()
	go func() {
		buffer := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buffer); err != nil {
				return
			}
		}
	}()
	connected := make(chan struct{}, 1)
	failed := make(chan error, 1)
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			select {
			case connected <- struct{}{}:
			default:
			}
		case webrtc.PeerConnectionStateFailed:
			select {
			case failed <- errors.New("peer connection failed"):
			default:
			}
		}
	})
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathering := webrtc.GatheringCompletePromise(peer)
	if err := peer.SetLocalDescription(offer); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	select {
	case <-gathering:
	case <-ctx.Done():
		return errors.New("ICE gathering timed out")
	}
	endpoint := session.SignalingURL + "?camera=" + url.QueryEscape(stream)
	header := http.Header{"Authorization": []string{"Bearer " + access}}
	socket, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err != nil {
		if response != nil {
			return fmt.Errorf("signaling returned %s", response.Status)
		}
		return err
	}
	defer socket.Close()
	message := map[string]any{
		"type": "webrtc",
		"value": map[string]any{
			"type": "offer", "sdp": peer.LocalDescription().SDP,
			"ice_servers": json.RawMessage(rawICE),
		},
	}
	if err := socket.WriteJSON(message); err != nil {
		return err
	}
	go readSignals(socket, peer, failed)
	select {
	case <-connected:
	case err := <-failed:
		return err
	case <-ctx.Done():
		return errors.New("relay-only talk connection timed out")
	}
	var bytesSubmitted uint64
	for i := 0; i < 125; i++ {
		// 160 PCMA samples (8 kHz, 20 ms). 0xd5 is A-law silence.
		data := bytes.Repeat([]byte{0xd5}, 160)
		if err := track.WriteSample(media.Sample{Data: data, Duration: 20 * time.Millisecond}); err != nil {
			return err
		}
		bytesSubmitted += uint64(len(data))
		time.Sleep(20 * time.Millisecond)
	}
	route, _ := relayStats(peer.GetStats())
	if route != "relay" {
		return fmt.Errorf("selected ICE route is %q, want relay", route)
	}
	fmt.Printf("edge talk e2e passed: stream=%s route=turn bytes_submitted=%d\n", stream, bytesSubmitted)
	return nil
}

func readSignals(socket *websocket.Conn, peer *webrtc.PeerConnection, failed chan<- error) {
	for {
		_, raw, err := socket.ReadMessage()
		if err != nil {
			select {
			case failed <- err:
			default:
			}
			return
		}
		var message struct {
			Type  string          `json:"type"`
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(raw, &message) != nil {
			continue
		}
		switch message.Type {
		case "webrtc":
			var answer webrtc.SessionDescription
			if json.Unmarshal(message.Value, &answer) == nil && answer.Type == webrtc.SDPTypeAnswer {
				if err := peer.SetRemoteDescription(answer); err != nil {
					select {
					case failed <- err:
					default:
					}
				}
			}
		case "webrtc/answer":
			var sdp string
			if json.Unmarshal(message.Value, &sdp) == nil {
				if err := peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
					select {
					case failed <- err:
					default:
					}
				}
			}
		case "webrtc/candidate":
			var candidate string
			if json.Unmarshal(message.Value, &candidate) == nil {
				_ = peer.AddICECandidate(webrtc.ICECandidateInit{Candidate: strings.TrimPrefix(candidate, "a=")})
			}
		case "error":
			select {
			case failed <- errors.New("Edge rejected talk stream"):
			default:
			}
		}
	}
}

func relayStats(report webrtc.StatsReport) (string, uint64) {
	candidates := map[string]webrtc.ICECandidateType{}
	var selected *webrtc.ICECandidatePairStats
	var bytesSent uint64
	for _, stat := range report {
		switch value := stat.(type) {
		case webrtc.ICECandidateStats:
			candidates[value.ID] = value.CandidateType
		case webrtc.ICECandidatePairStats:
			if value.Nominated && value.State == webrtc.StatsICECandidatePairStateSucceeded {
				copy := value
				selected = &copy
			}
		case webrtc.OutboundRTPStreamStats:
			bytesSent += value.BytesSent
		}
	}
	if selected == nil {
		return "", bytesSent
	}
	if candidates[selected.LocalCandidateID] == webrtc.ICECandidateTypeRelay ||
		candidates[selected.RemoteCandidateID] == webrtc.ICECandidateTypeRelay {
		return "relay", bytesSent
	}
	return "direct", bytesSent
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
