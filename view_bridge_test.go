package main

import (
	"testing"

	"github.com/pion/rtp/codecs"
)

func TestH264BootstrapPayloadEndsWithMarker(t *testing.T) {
	payloader := &codecs.H264Payloader{}
	// A payload larger than the MTU exercises FU-A fragmentation. Only the
	// final fragment may terminate the H.264 access unit for the decoder.
	packets := packetizeBootstrapAccessUnit(payloader, make([]byte, 3000), 41, 9000)
	if len(packets) < 2 {
		t.Fatalf("expected fragmented payload, got %d packet", len(packets))
	}
	for index, packet := range packets {
		if packet.Marker != (index == len(packets)-1) {
			t.Fatalf("unexpected marker at packet %d", index)
		}
		if packet.SequenceNumber != 41+uint16(index) || packet.Timestamp != 9000 {
			t.Fatalf("unexpected RTP continuity at packet %d", index)
		}
	}
}
