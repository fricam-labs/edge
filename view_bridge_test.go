package main

import (
	"testing"

	"github.com/pion/rtp"
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

func TestH264RTPStartsIDROnDecoderSafeBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"single IDR", []byte{0x65, 0x01}, true},
		{"single P frame", []byte{0x41, 0x01}, false},
		{"FU-A IDR start", []byte{0x7c, 0x85, 0x01}, true},
		{"FU-A IDR continuation", []byte{0x7c, 0x05, 0x01}, false},
		{"STAP-A containing IDR", []byte{0x78, 0, 2, 0x65, 0x01}, true},
		{"truncated STAP-A", []byte{0x78, 0, 9, 0x65}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := h264RTPStartsIDR(test.payload); got != test.want {
				t.Fatalf("h264RTPStartsIDR() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestVideoRTPContinuityRebasesLiveAfterEachBootstrap(t *testing.T) {
	var continuity videoRTPContinuity
	sequence, timestamp := continuity.beginBootstrap()
	continuity.finishBootstrap(sequence+4, timestamp+6000)

	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 500, Timestamp: 90000}}
	continuity.rewriteLive(first)
	if first.SequenceNumber != 5 || first.Timestamp != 6001 {
		t.Fatalf("first live packet = seq %d timestamp %d", first.SequenceNumber, first.Timestamp)
	}
	second := &rtp.Packet{Header: rtp.Header{SequenceNumber: 501, Timestamp: 93000}}
	continuity.rewriteLive(second)
	if second.SequenceNumber != 6 || second.Timestamp != 9001 {
		t.Fatalf("second live packet = seq %d timestamp %d", second.SequenceNumber, second.Timestamp)
	}

	// Simulate a long pause. The new input timestamp is far ahead, but the
	// resumed output must begin directly after the newly appended cached GOP.
	sequence, timestamp = continuity.beginBootstrap()
	continuity.finishBootstrap(sequence+3, timestamp+6000)
	resumed := &rtp.Packet{Header: rtp.Header{SequenceNumber: 900, Timestamp: 900000}}
	continuity.rewriteLive(resumed)
	if resumed.SequenceNumber != 10 || resumed.Timestamp != 18001 {
		t.Fatalf("resumed packet = seq %d timestamp %d", resumed.SequenceNumber, resumed.Timestamp)
	}
}

func TestVideoRTPContinuityPreservesFragmentsAndFrameCadence(t *testing.T) {
	var continuity videoRTPContinuity
	packets := []*rtp.Packet{
		{Header: rtp.Header{SequenceNumber: 10, Timestamp: 1000}},
		{Header: rtp.Header{SequenceNumber: 11, Timestamp: 1000}},
		{Header: rtp.Header{SequenceNumber: 12, Timestamp: 4600}},
	}
	for _, packet := range packets {
		continuity.rewriteLive(packet)
	}
	if packets[0].Timestamp != packets[1].Timestamp {
		t.Fatal("fragments from one frame received different timestamps")
	}
	if packets[2].Timestamp-packets[1].Timestamp != 3600 {
		t.Fatalf("frame cadence changed to %d", packets[2].Timestamp-packets[1].Timestamp)
	}
}
