package main

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
)

func (r *relayController) serveProgressive(w http.ResponseWriter, request *http.Request) {
	if !r.authorizeLocalClient(request) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	requested := request.URL.Query().Get("src")
	source, ok := r.manager.signalingSource(requested)
	if !ok {
		http.NotFound(w, request)
		return
	}
	detail := ""
	if warm, hd, ready := r.manager.progressiveSources(requested); ready {
		source, detail = warm, hd
	}
	socket, err := localWebRTCUpgrader.Upgrade(w, request, http.Header{"X-Fricam-Edge": []string{"1"}})
	if err != nil {
		return
	}
	defer socket.Close()
	socket.SetReadLimit(edgeMaxSignalBytes)
	var writes sync.Mutex
	send := func(payload json.RawMessage) {
		writes.Lock()
		defer writes.Unlock()
		_ = socket.WriteMessage(websocket.TextMessage, payload)
	}
	var bridge *viewBridge
	defer func() {
		if bridge != nil {
			bridge.close()
		}
	}()
	for {
		_, payload, readErr := socket.ReadMessage()
		if readErr != nil {
			return
		}
		if bridge != nil {
			bridge.addClientSignal(payload)
			continue
		}
		var signal struct {
			Type  string          `json:"type"`
			Value viewBridgeOffer `json:"value"`
		}
		if json.Unmarshal(payload, &signal) != nil || signal.Type != "webrtc" || signal.Value.Type != "offer" || len(signal.Value.SDP) == 0 || len(signal.Value.SDP) > 96*1024 || !signal.Value.Progressive {
			return
		}
		// LAN authentication is independent of TURN. Never accept client-selected
		// ICE services here: the host-only peer needs no outbound ICE credentials.
		signal.Value.ICEServers = nil
		bridge = newViewBridge(request.Context(), r.go2rtcURL, source, func() [][]byte { return r.manager.h264Bootstrap(source) }, signal.Value, send)
		bridge.detailSource, bridge.manager = detail, r.manager
		if detail == "" {
			bridge.demandBase = true
			bridge.detailSource = source
		}
		current := bridge
		go func() {
			if err := current.start(); err != nil {
				current.sendSignal("error", "stream unavailable")
				_ = socket.Close()
			}
		}()
	}
}
