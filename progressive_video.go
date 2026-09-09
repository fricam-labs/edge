package main

import (
	"context"
	"encoding/json"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"time"
)

// Both tracks are negotiated once. The client keeps displaying the startup
// decoder until the HD decoder delivers a frame; codecs never change in place.
func (b *viewBridge) startHD() {
	b.mu.Lock()
	if b.ctx.Err() != nil || b.paused.Load() || b.hd != nil || (b.hdVideo == nil && !b.demandBase) || b.manager == nil || b.detailSource == "" || (b.detailSource == b.source && !b.demandBase) {
		b.mu.Unlock()
		return
	}
	release, ok := b.manager.acquireHD(b.detailSource)
	if !ok {
		b.mu.Unlock()
		b.sendSignal("edge/quality", "busy")
		return
	}
	child := newViewBridge(b.ctx, b.go2rtcURL, b.detailSource, nil, viewBridgeOffer{}, func(_ json.RawMessage) {})
	// No remote peer on the child: it writes directly to the already-negotiated
	// HD track. The startup peer keeps sending throughout HD connection setup.
	close(child.remoteReady)
	b.hdGeneration++
	generation := b.hdGeneration
	b.hd, b.hdRelease = child, release
	video, audio := b.hdVideo, b.audio
	if b.demandBase {
		video = b.video
	}
	b.mediaWriteMu.Lock()
	b.hdContinuity.inputReady = false
	b.mediaWriteMu.Unlock()
	child.writeRTP = func(track *webrtc.TrackLocalStaticRTP, packet *rtp.Packet) error {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.hd != child || b.paused.Load() {
			return nil
		}
		b.mediaWriteMu.Lock()
		defer b.mediaWriteMu.Unlock()
		if track == video {
			b.hdContinuity.rewriteLive(packet)
		}
		return b.writeRTP(track, packet)
	}
	b.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(child.ctx, viewBridgeTimeout)
		defer cancel()
		// Keep startup audio, avoiding two audio sources interleaved on one track.
		silentAudio, _ := newRTPTrack(audio.Codec(), "discard-audio", "discard")
		if b.demandBase {
			silentAudio = audio
		}
		ready, err := connectViewLocal(child, ctx, video, silentAudio, child.source)
		if err == nil {
			err = waitContext(ready, ctx)
		}
		if err != nil {
			b.stopHDGeneration(generation)
			b.sendSignal("edge/quality", "unavailable")
			return
		}
		b.sendSignal("edge/quality", "hd-sending")
		// No first-decoded-frame acknowledgement means the client cannot use HD.
		timer := time.NewTimer(8 * time.Second)
		defer timer.Stop()
		select {
		case <-child.ctx.Done():
			return
		case <-timer.C:
		}
		if !b.demandBase && !b.hdActive.Load() {
			b.stopHDGeneration(generation)
			b.sendSignal("edge/quality", "decode-timeout")
			return
		}
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		last := child.videoPacketsForwarded.Load()
		for {
			select {
			case <-child.ctx.Done():
				return
			case <-ticker.C:
				now := child.videoPacketsForwarded.Load()
				if now == last {
					b.stopHDGeneration(generation)
					b.sendSignal("edge/quality", "unavailable")
					return
				}
				last = now
			}
		}
	}()
}

func (b *viewBridge) stopHDGeneration(generation uint64) {
	b.mu.Lock()
	if generation != b.hdGeneration {
		b.mu.Unlock()
		return
	}
	child, release := b.hd, b.hdRelease
	b.hd, b.hdRelease = nil, nil
	wasHD := b.hdActive.Swap(false)
	video := b.video
	b.mu.Unlock()
	if child != nil {
		child.close()
	}
	if release != nil {
		release()
	}
	if wasHD && video != nil && b.ctx.Err() == nil && !b.paused.Load() {
		b.writeBootstrap(video)
	}
}

func (b *viewBridge) stopHD() {
	b.mu.Lock()
	generation := b.hdGeneration
	b.mu.Unlock()
	b.stopHDGeneration(generation)
}

func (b *viewBridge) forwardHDRTCP(sender *webrtc.RTPSender) {
	for {
		packets, _, err := readSenderRTCP(sender)
		if err != nil {
			return
		}
		b.mu.Lock()
		child := b.hd
		b.mu.Unlock()
		if child == nil {
			continue
		}
		child.mu.Lock()
		local := child.local
		child.mu.Unlock()
		if local != nil {
			for _, packet := range packets {
				switch packet.(type) {
				case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
					if ssrc := child.localVideoSSRC.Load(); ssrc != 0 {
						_ = local.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}})
					}
				}
			}
		}
	}
}
