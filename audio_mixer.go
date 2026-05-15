package main

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
)

const (
	roomAudioSampleRate = 48000
	roomAudioChannels   = 2
	roomAudioMaxFrameMs = 60

	roomAudioMixInterval  = 20 * time.Millisecond
	roomAudioMixFrameSize = roomAudioSampleRate / 50 * roomAudioChannels
	audioSourceLimit      = roomAudioMixFrameSize * 50

	// Per-source AGC: normalize each track to this RMS target before mixing.
	// Prevents quiet microphones from being drowned out by louder ones.
	// Gain is only updated during voiced frames (above agcSpeechFloor) and
	// held steady during silence, so quiet gaps never trigger runaway boost.
	agcTargetRMS   = 2000.0 // target RMS in int16 units (~6% of full scale)
	agcSpeechFloor = 300.0  // frame RMS below this is treated as silence; gain frozen
	agcMaxGain     = 4.0    // cap gain so we never amplify more than 4x
)

type mixedAudioSink interface {
	WriteMixedPCM([]int16) error
}

type audioMixer struct {
	mu             sync.Mutex
	sinks          map[string]mixedAudioSink
	manualGains    map[string]float64 // AI-controlled override per track key; 1.0 = no override
	hasManualGains atomic.Bool        // true when manualGains is non-empty; avoids lock on every tick
	input          chan audioInput
	statsReq       chan chan map[string]float64 // request AGC heldGain snapshot from run()
	stop           chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
}

type audioInput struct {
	trackKey string
	pcm      []int16
	remove   bool
}

type audioSource struct {
	buffer   []int16
	heldGain float64 // AGC gain frozen at last voiced frame; 0 means uninitialized (use 1.0)
}

func newAudioMixer() *audioMixer {
	mixer := &audioMixer{
		sinks:       map[string]mixedAudioSink{},
		manualGains: map[string]float64{},
		input:       make(chan audioInput, 128),
		statsReq:    make(chan chan map[string]float64, 4),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	go mixer.run()
	return mixer
}

func (mixer *audioMixer) submit(trackKey string, pcm []int16) {
	if mixer == nil || trackKey == "" || len(pcm) == 0 {
		return
	}

	select {
	case <-mixer.stop:
		return
	default:
	}

	select {
	case mixer.input <- audioInput{trackKey: trackKey, pcm: pcm}:
	default:
		log.Warnf("Dropping decoded audio frame for track=%s", trackKey)
	}
}

func (mixer *audioMixer) removeTrack(trackKey string) {
	if mixer == nil || trackKey == "" {
		return
	}

	select {
	case <-mixer.stop:
		return
	default:
	}

	select {
	case mixer.input <- audioInput{trackKey: trackKey, remove: true}:
	default:
		log.Warnf("Dropping decoded audio remove for track=%s", trackKey)
	}
}

func (mixer *audioMixer) setSink(key string, sink mixedAudioSink) {
	if mixer == nil || key == "" {
		return
	}

	mixer.mu.Lock()
	defer mixer.mu.Unlock()

	if sink == nil {
		delete(mixer.sinks, key)
		return
	}
	mixer.sinks[key] = sink
}

func (mixer *audioMixer) removeSink(key string) {
	if mixer == nil || key == "" {
		return
	}

	mixer.mu.Lock()
	delete(mixer.sinks, key)
	mixer.mu.Unlock()
}

// setManualGain sets the AI-controlled gain multiplier for a track.
// The final applied gain is heldGain (AGC) * manualGain.
// Pass gain=0 (or any negative value) to remove a previously set override.
// Pass gain=1.0 to apply unity gain explicitly (AGC-only, no AI boost/cut).
func (mixer *audioMixer) setManualGain(trackKey string, gain float64) {
	if mixer == nil || trackKey == "" {
		return
	}
	mixer.mu.Lock()
	defer mixer.mu.Unlock()
	if gain <= 0 {
		delete(mixer.manualGains, trackKey)
	} else {
		mixer.manualGains[trackKey] = gain
	}
	mixer.hasManualGains.Store(len(mixer.manualGains) > 0)
}

// gainInfo is returned by gainSnapshot for each active source.
type gainInfo struct {
	AGCGain    float64 // automatic gain from last voiced frame (1.0 if not yet voiced)
	ManualGain float64 // AI-controlled override (1.0 if not set)
	Applied    float64 // AGCGain * ManualGain
}

// gainSnapshot returns gain info for every active source in the mixer.
// Blocks up to one mix interval (20ms) until run() responds with current heldGain values.
func (mixer *audioMixer) gainSnapshot() map[string]gainInfo {
	if mixer == nil {
		return nil
	}

	replyCh := make(chan map[string]float64, 1)
	select {
	case mixer.statsReq <- replyCh:
	case <-mixer.stop:
		return nil
	}

	var agcGains map[string]float64
	select {
	case agcGains = <-replyCh:
	case <-mixer.stop:
		return nil
	}

	mixer.mu.Lock()
	manuals := make(map[string]float64, len(mixer.manualGains))
	for k, v := range mixer.manualGains {
		manuals[k] = v
	}
	mixer.mu.Unlock()

	// Union of all known track keys (AGC may have tracks with no manual override and vice versa).
	keys := make(map[string]struct{}, len(agcGains)+len(manuals))
	for k := range agcGains {
		keys[k] = struct{}{}
	}
	for k := range manuals {
		keys[k] = struct{}{}
	}

	result := make(map[string]gainInfo, len(keys))
	for key := range keys {
		agc := agcGains[key]
		if agc == 0 {
			agc = 1.0
		}
		manual := manuals[key]
		if manual <= 0 {
			manual = 1.0
		}
		result[key] = gainInfo{AGCGain: agc, ManualGain: manual, Applied: agc * manual}
	}
	return result
}

func (mixer *audioMixer) snapshotManualGains() map[string]float64 {
	if mixer == nil || !mixer.hasManualGains.Load() {
		return nil
	}
	mixer.mu.Lock()
	defer mixer.mu.Unlock()
	out := make(map[string]float64, len(mixer.manualGains))
	for k, v := range mixer.manualGains {
		out[k] = v
	}
	return out
}

func (mixer *audioMixer) close() {
	if mixer == nil {
		return
	}

	mixer.closeOnce.Do(func() {
		close(mixer.stop)
		<-mixer.done
	})
}

func (mixer *audioMixer) run() {
	defer close(mixer.done)

	ticker := time.NewTicker(roomAudioMixInterval)
	defer ticker.Stop()

	sources := map[string]*audioSource{}
	for {
		select {
		case <-mixer.stop:
			return
		case input := <-mixer.input:
			if input.remove {
				delete(sources, input.trackKey)
				continue
			}

			source := sources[input.trackKey]
			if source == nil {
				source = &audioSource{}
				sources[input.trackKey] = source
			}

			source.buffer = append(source.buffer, input.pcm...)
			if overflow := len(source.buffer) - audioSourceLimit; overflow > 0 {
				source.buffer = source.buffer[overflow:]
			}
		case replyCh := <-mixer.statsReq:
			agcGains := make(map[string]float64, len(sources))
			for key, src := range sources {
				g := src.heldGain
				if g == 0 {
					g = 1.0
				}
				agcGains[key] = g
			}
			replyCh <- agcGains
		case <-ticker.C:
			mixedPCM := mixAudioFrame(sources, mixer.snapshotManualGains())
			if len(mixedPCM) == 0 {
				continue
			}

			for key, sink := range mixer.snapshotSinks() {
				if err := sink.WriteMixedPCM(mixedPCM); err != nil {
					log.Errorf("Failed to write mixed audio sink=%s: %v", key, err)
				}
			}
		}
	}
}

func (mixer *audioMixer) snapshotSinks() map[string]mixedAudioSink {
	mixer.mu.Lock()
	defer mixer.mu.Unlock()

	sinks := make(map[string]mixedAudioSink, len(mixer.sinks))
	for key, sink := range mixer.sinks {
		sinks[key] = sink
	}

	return sinks
}

// agcSpeechFloorSq is agcSpeechFloor² — used to skip sqrt on silent frames.
const agcSpeechFloorSq = agcSpeechFloor * agcSpeechFloor

func frameMeanSumSq(frame []int16) float64 {
	if len(frame) == 0 {
		return 0
	}
	var sum float64
	for _, s := range frame {
		v := float64(s)
		sum += v * v
	}
	return sum / float64(len(frame))
}

func mixAudioFrame(sources map[string]*audioSource, manualGains map[string]float64) []int16 {
	type readySource struct {
		src      *audioSource
		trackKey string
	}
	ready := make([]readySource, 0, len(sources))
	for key, source := range sources {
		if len(source.buffer) >= roomAudioMixFrameSize {
			ready = append(ready, readySource{source, key})
		}
	}
	if len(ready) == 0 {
		return nil
	}

	// Compute per-source gain. Only update heldGain during voiced frames;
	// during silence, freeze it so quiet gaps don't trigger runaway boost.
	// Final gain = AGC held gain * AI manual override.
	// Use mean sum-of-squares for the silence threshold (avoids sqrt on silent frames).
	gains := make([]float64, len(ready))
	for i, rs := range ready {
		frame := rs.src.buffer[:roomAudioMixFrameSize]
		meanSq := frameMeanSumSq(frame)
		if meanSq >= agcSpeechFloorSq {
			rms := math.Sqrt(meanSq)
			rs.src.heldGain = math.Min(agcTargetRMS/rms, agcMaxGain)
		}
		agc := rs.src.heldGain
		if agc == 0 {
			agc = 1.0
		}
		manual := manualGains[rs.trackKey]
		if manual <= 0 {
			manual = 1.0
		}
		gains[i] = agc * manual
	}

	mixedPCM := make([]int16, roomAudioMixFrameSize)
	for sampleIndex := range mixedPCM {
		var sampleSum int32
		for i, rs := range ready {
			sampleSum += int32(float64(rs.src.buffer[sampleIndex]) * gains[i])
		}
		mixedPCM[sampleIndex] = clampPCM16(sampleSum / int32(len(ready)))
	}

	for _, rs := range ready {
		rs.src.buffer = rs.src.buffer[roomAudioMixFrameSize:]
	}

	return mixedPCM
}

func clampPCM16(sample int32) int16 {
	switch {
	case sample > 32767:
		return 32767
	case sample < -32768:
		return -32768
	default:
		return int16(sample)
	}
}

func roomAudioTrackKey(remoteTrack *webrtc.TrackRemote) string {
	return fmt.Sprintf("%s:%s:%d", remoteTrack.StreamID(), remoteTrack.ID(), remoteTrack.SSRC())
}

func newRoomAudioDecoder(remoteTrack *webrtc.TrackRemote) (*opusDecoder, int, error) {
	if remoteTrack.Kind() != webrtc.RTPCodecTypeAudio {
		return nil, 0, nil
	}

	codec := remoteTrack.Codec()
	if !strings.EqualFold(codec.MimeType, webrtc.MimeTypeOpus) {
		return nil, 0, fmt.Errorf("unsupported audio codec %q", codec.MimeType)
	}

	clockRate := int(codec.ClockRate)
	if clockRate == 0 {
		clockRate = roomAudioSampleRate
	}
	if clockRate != roomAudioSampleRate {
		return nil, 0, fmt.Errorf("unsupported opus clock rate %d", codec.ClockRate)
	}

	channels := normalizedRoomAudioChannels(codec.Channels)
	decoder, err := newOpusDecoder(clockRate, channels)
	if err != nil {
		return nil, 0, err
	}

	return decoder, channels, nil
}

func normalizedRoomAudioChannels(channels uint16) int {
	switch channels {
	case 1:
		return 1
	case 2:
		return 2
	default:
		return roomAudioChannels
	}
}

func roomAudioDecodeBufferSize(channels int) int {
	if channels <= 0 {
		return 0
	}

	return roomAudioSampleRate * channels * roomAudioMaxFrameMs / 1000
}

func decodeOpusToRoomPCM(decoder *opusDecoder, buffer []int16, channels int, payload []byte) ([]int16, error) {
	if decoder == nil || channels == 0 || len(payload) == 0 {
		return nil, nil
	}

	samplesPerChannel, err := decoder.Decode(payload, buffer)
	if err != nil {
		return nil, err
	}

	return normalizeRoomAudioPCM(buffer[:samplesPerChannel*channels], channels), nil
}

func normalizeRoomAudioPCM(pcm []int16, channels int) []int16 {
	switch channels {
	case 1:
		stereoPCM := make([]int16, len(pcm)*roomAudioChannels)
		for sampleIndex, sample := range pcm {
			baseIndex := sampleIndex * roomAudioChannels
			stereoPCM[baseIndex] = sample
			stereoPCM[baseIndex+1] = sample
		}
		return stereoPCM
	case roomAudioChannels:
		return append([]int16(nil), pcm...)
	default:
		return nil
	}
}
