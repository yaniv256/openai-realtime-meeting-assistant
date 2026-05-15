package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	realtimeCallsURL          = "https://api.openai.com/v1/realtime/calls"
	defaultRealtimeModel      = "gpt-realtime-2"
	defaultReasoningEffort    = "low"
	realtimeEventChannelLabel = "oai-events"
	realtimeInputTrackID      = "kanban-realtime:mixed-audio"
	realtimeInputStreamID     = "kanban-realtime-input"
	realtimeMixedAudioSinkKey = "kanban-realtime"
)

type kanbanStatus string

const (
	kanbanStatusBacklog    kanbanStatus = "Backlog"
	kanbanStatusInProgress kanbanStatus = "In Progress"
	kanbanStatusBlocked    kanbanStatus = "Blocked"
	kanbanStatusDone       kanbanStatus = "Done"
)

var kanbanStatuses = []kanbanStatus{
	kanbanStatusBacklog,
	kanbanStatusInProgress,
	kanbanStatusBlocked,
	kanbanStatusDone,
}

type kanbanCard struct {
	ID     string       `json:"id"`
	Status kanbanStatus `json:"status"`
	Title  string       `json:"title"`
	Notes  string       `json:"notes"`
	Tags   []string     `json:"tags"`
}

type kanbanBoardState struct {
	Cards     []kanbanCard `json:"cards"`
	UpdatedAt string       `json:"updatedAt,omitempty"`
}

type kanbanRealtimeEvent struct {
	Type       string `json:"type,omitempty"`
	Transcript string `json:"transcript,omitempty"`
	Delta      string `json:"delta,omitempty"`
	Name       string `json:"name,omitempty"`
	Arguments  string `json:"arguments,omitempty"`
	CallID     string `json:"call_id,omitempty"`
	Error      *struct {
		Code    string `json:"code,omitempty"`
		Message string `json:"message,omitempty"`
	} `json:"error,omitempty"`
	Item     *kanbanRealtimeOutputItem `json:"item,omitempty"`
	Response *struct {
		Output []kanbanRealtimeOutputItem `json:"output,omitempty"`
	} `json:"response,omitempty"`
}

type kanbanRealtimeOutputItem struct {
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	CallID    string `json:"call_id,omitempty"`
}

type kanbanBoardApp struct {
	mu               sync.Mutex
	cards            []kanbanCard
	nextCreatedIndex int
	updatedAt        time.Time
	handledCalls     map[string]struct{}

	model      string
	voiceMode  bool // true = output_modalities includes audio
	pc         *webrtc.PeerConnection
	events     *webrtc.DataChannel
	inputTrack *webrtc.TrackLocalStaticSample
	inputEnc   *opusEncoder
	connected  bool
	closeOnce  sync.Once
}

var initialKanbanBoardCards = []kanbanCard{
	{
		ID:     "card-002",
		Status: kanbanStatusBacklog,
		Title:  "Add RTP Retransmission Buffer",
		Notes:  "Keep recent RTP packets available for NACK-driven retransmission without unbounded memory growth.",
		Tags:   []string{"webrtc", "rtp", "nack"},
	},
	{
		ID:     "card-003",
		Status: kanbanStatusBacklog,
		Title:  "Implement ICE Restart Handling",
		Notes:  "Support renegotiation paths that refresh ICE credentials and reconnect peers after network changes.",
		Tags:   []string{"webrtc", "ice", "signaling"},
	},
	{
		ID:     "card-004",
		Status: kanbanStatusBacklog,
		Title:  "Harden DTLS/SRTP Cleanup",
		Notes:  "Ensure failed and closed peer connections release transports, tracks, and SRTP state promptly.",
		Tags:   []string{"webrtc", "dtls", "srtp"},
	},
	{
		ID:     "card-005",
		Status: kanbanStatusBacklog,
		Title:  "Add Simulcast Forwarding Controls",
		Notes:  "Choose forwarded RTP layers per subscriber so the server can adapt streams to bandwidth and viewport size.",
		Tags:   []string{"webrtc", "simulcast", "bandwidth"},
	},
	{
		ID:     "card-001",
		Status: kanbanStatusBacklog,
		Title:  "Finish RTP HEVC Packetizer",
		Notes:  "Complete HEVC payload fragmentation, aggregation, and marker-bit handling for outbound RTP streams.",
		Tags:   []string{"webrtc", "rtp", "hevc"},
	},
}

func newKanbanBoardApp() *kanbanBoardApp {
	return &kanbanBoardApp{
		cards:            cloneKanbanCards(initialKanbanBoardCards),
		nextCreatedIndex: 1,
		updatedAt:        time.Now().UTC(),
		handledCalls:     map[string]struct{}{},
		voiceMode:        true, // default on; user mutes rather than unmutes
	}
}

func (app *kanbanBoardApp) JoinConferenceRoom() error {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY is not configured")
	}

	peerConnection, err := newPeerConnection()
	if err != nil {
		return fmt.Errorf("create Realtime peer connection: %w", err)
	}

	inputTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: roomAudioSampleRate,
			Channels:  roomAudioChannels,
		},
		realtimeInputTrackID,
		realtimeInputStreamID,
	)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime mixed audio input track: %w", err)
	}

	inputEnc, err := newOpusEncoder(roomAudioSampleRate, roomAudioChannels)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime mixed audio encoder: %w", err)
	}

	inputSender, err := peerConnection.AddTrack(inputTrack)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("attach Realtime mixed audio input track: %w", err)
	}
	go drainRTCP(inputSender)

	events, err := peerConnection.CreateDataChannel(realtimeEventChannelLabel, nil)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime event data channel: %w", err)
	}

	model := realtimeModel()
	app.mu.Lock()
	app.model = model
	app.pc = peerConnection
	app.events = events
	app.inputTrack = inputTrack
	app.inputEnc = inputEnc
	app.mu.Unlock()

	peerConnection.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		log.Infof("OpenAI Realtime audio track: Kind=%s, MimeType=%s", t.Kind(), t.Codec().MimeType)
		if t.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		// Fan out AI audio directly to all browser peers as a static RTP track.
		// Do NOT route through the room mixer — the mixer feeds OpenAI's input,
		// so putting AI output there creates a feedback loop and never reaches browsers.
		trackLocal := addTrack(t)
		defer removeTrack(trackLocal)
		for {
			packet, _, err := t.ReadRTP()
			if err != nil {
				return
			}
			if err = trackLocal.WriteRTP(packet); err != nil {
				return
			}
		}
	})

	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Infof("OpenAI Realtime peer state changed: %s", state.String())
		broadcastKanbanEvent("status", "OpenAI Realtime: "+state.String())
		switch state {
		case webrtc.PeerConnectionStateConnected:
			// ICE fully established — AI is actually reachable now
			app.mu.Lock()
			app.connected = true
			app.mu.Unlock()
			broadcastKanbanEvent("ai_status", map[string]any{"connected": true})
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateDisconnected:
			app.mu.Lock()
			app.connected = false
			app.mu.Unlock()
			broadcastKanbanEvent("ai_status", map[string]any{"connected": false})
		}
	})
	events.OnOpen(func() {
		log.Infof("OpenAI Realtime event channel opened")
		_ = app.SendEvent(app.sessionUpdateEvent())
	})
	events.OnMessage(func(message webrtc.DataChannelMessage) {
		app.handleRealtimeEvent(message.Data)
	})

	go func() {
		if err := app.connectRealtimePeer(apiKey, model); err != nil {
			log.Errorf("Failed to connect OpenAI Realtime peer: %v", err)
			broadcastKanbanEvent("status", "OpenAI Realtime disabled: "+err.Error())
			_ = peerConnection.Close()
			return
		}
		if roomMixer != nil {
			roomMixer.setSink(realtimeMixedAudioSinkKey, app)
		}
	}()

	return nil
}

func (app *kanbanBoardApp) Close() error {
	var closeErr error
	app.closeOnce.Do(func() {
		if roomMixer != nil {
			roomMixer.removeSink(realtimeMixedAudioSinkKey)
		}

		app.mu.Lock()
		peerConnection := app.pc
		app.mu.Unlock()
		if peerConnection != nil {
			closeErr = peerConnection.Close()
		}
	})

	return closeErr
}

func (app *kanbanBoardApp) connectRealtimePeer(apiKey string, model string) error {
	app.mu.Lock()
	if app.connected {
		app.mu.Unlock()
		return nil
	}
	peerConnection := app.pc
	app.mu.Unlock()

	if peerConnection == nil {
		return fmt.Errorf("Realtime peer connection is unavailable")
	}

	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create Realtime offer: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set Realtime local description: %w", err)
	}
	<-gatherComplete

	localDescription := peerConnection.LocalDescription()
	if localDescription == nil || strings.TrimSpace(localDescription.SDP) == "" {
		return fmt.Errorf("Realtime peer connection did not produce a local description")
	}

	answerSDP, err := app.createRealtimeCall(apiKey, model, localDescription.SDP)
	if err != nil {
		return err
	}

	if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	}); err != nil {
		return fmt.Errorf("set Realtime remote description: %w", err)
	}

	return nil
}

func (app *kanbanBoardApp) WriteMixedPCM(roomPCM []int16) error {
	if len(roomPCM) == 0 {
		return nil
	}
	if len(roomPCM)%roomAudioMixFrameSize != 0 {
		return fmt.Errorf("mixed PCM length %d must be a multiple of %d samples", len(roomPCM), roomAudioMixFrameSize)
	}

	app.mu.Lock()
	inputTrack := app.inputTrack
	inputEnc := app.inputEnc
	app.mu.Unlock()

	if inputTrack == nil || inputEnc == nil {
		return fmt.Errorf("Realtime mixed audio input is unavailable")
	}

	for offset := 0; offset < len(roomPCM); offset += roomAudioMixFrameSize {
		frame := roomPCM[offset : offset+roomAudioMixFrameSize]

		opusFrame, err := inputEnc.Encode(frame)
		if err != nil {
			return fmt.Errorf("encode mixed room audio: %w", err)
		}

		if err := inputTrack.WriteSample(media.Sample{
			Data:     opusFrame,
			Duration: roomAudioMixInterval,
		}); err != nil {
			return fmt.Errorf("write mixed room audio sample: %w", err)
		}
	}

	return nil
}

func drainRTCP(sender *webrtc.RTPSender) {
	buffer := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buffer); err != nil {
			return
		}
	}
}

func (app *kanbanBoardApp) createRealtimeCall(apiKey string, model string, offerSDP string) (string, error) {
	contentType, body, err := buildRealtimeCallRequest(offerSDP, app.sessionConfig(model))
	if err != nil {
		return "", err
	}

	request, err := http.NewRequest(http.MethodPost, realtimeCallsURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create Realtime request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", contentType)

	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return "", fmt.Errorf("create Realtime session: %w", err)
	}
	defer response.Body.Close()

	answerSDP, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read Realtime answer: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body := strings.TrimSpace(string(answerSDP))
		if len(body) > 300 {
			body = body[:300]
		}
		log.Errorf("OpenAI /v1/realtime/calls failed: status=%s body=%s", response.Status, body)
		return "", fmt.Errorf("Realtime session failed: status=%s body=%s", response.Status, strings.TrimSpace(string(answerSDP)))
	}

	return string(answerSDP), nil
}

func buildRealtimeCallRequest(offerSDP string, session map[string]any) (string, []byte, error) {
	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return "", nil, fmt.Errorf("marshal Realtime session: %w", err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("sdp", offerSDP); err != nil {
		return "", nil, fmt.Errorf("write SDP offer: %w", err)
	}
	if err := writer.WriteField("session", string(sessionJSON)); err != nil {
		return "", nil, fmt.Errorf("write session config: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", nil, fmt.Errorf("finalize multipart request: %w", err)
	}

	return writer.FormDataContentType(), body.Bytes(), nil
}

func (app *kanbanBoardApp) SendEvent(payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal Realtime event: %w", err)
	}

	app.mu.Lock()
	events := app.events
	app.mu.Unlock()
	if events == nil || events.ReadyState() != webrtc.DataChannelStateOpen {
		return fmt.Errorf("Realtime event channel is unavailable")
	}

	return events.SendText(string(raw))
}

func (app *kanbanBoardApp) sessionConfig(model string) map[string]any {
	app.mu.Lock()
	voiceMode := app.voiceMode
	app.mu.Unlock()

	outputModalities := []string{"text"}
	if voiceMode {
		outputModalities = []string{"audio"}
	}
	session := map[string]any{
		"type":              "realtime",
		"model":             model,
		"output_modalities": outputModalities,
		"audio": map[string]any{
			"input": map[string]any{
				"noise_reduction": map[string]any{
					"type": "near_field",
				},
				"transcription": map[string]any{
					"model":    "gpt-4o-mini-transcribe",
					"language": "en",
				},
				"turn_detection": map[string]any{
					"type":                "server_vad",
					"threshold":           0.5,
					"prefix_padding_ms":   300,
					"silence_duration_ms": 200,
					"create_response":     true,
					"interrupt_response":  false,
				},
			},
		},
		"instructions": app.sessionInstructions(voiceMode),
		"tools":        app.kanbanTools(),
		"tool_choice":  "auto",
	}

	if usesAdvancedCommandProfile(model) {
		session["reasoning"] = map[string]any{
			"effort": defaultReasoningEffort,
		}
	}

	return session
}

func (app *kanbanBoardApp) sessionUpdateEvent() map[string]any {
	return map[string]any{
		"type":    "session.update",
		"session": app.sessionConfig(app.model),
	}
}

func realtimeModel() string {
	if model := strings.TrimSpace(os.Getenv("OPENAI_REALTIME_MODEL")); model != "" {
		return model
	}

	return defaultRealtimeModel
}

func usesAdvancedCommandProfile(model string) bool {
	normalizedModel := strings.ToLower(strings.TrimSpace(model))
	return normalizedModel == "gpt-realtime-2"
}

func (app *kanbanBoardApp) sessionInstructions(voiceMode bool) string {
	return strings.Join([]string{
		"You are the meeting assistant for a real-time video meeting app with a shared Kanban board.",
		"About this app: participants join at realtimevoice.dev/meeting — anyone with the URL can join, no sign-up required. Audio from all participants is mixed so everyone hears each other. Everyone sees each other's cameras. The Kanban board is shared and updates live for all participants. This is a real meeting tool, not a demo.",
		"About privacy and hosting: the app and its source code are open-source at github.com/yaniv256/openai-realtime-meeting-assistant. Anyone can self-host their own private instance by following the instructions in the README. The Kanban board is built into the app — it cannot currently connect to external Kanban tools like Jira or Linear. A future version that lets users sign up, connect to their own Kanban, and have a private room is in development.",
		"About reading the codebase: you have a fetch_url tool. Use it to read files from the repo when asked how the app works. Key URLs: README at https://raw.githubusercontent.com/yaniv256/openai-realtime-meeting-assistant/main/README.md — main Go server at https://raw.githubusercontent.com/yaniv256/openai-realtime-meeting-assistant/main/main.go — Kanban/AI logic at https://raw.githubusercontent.com/yaniv256/openai-realtime-meeting-assistant/main/kanban.go — frontend at https://raw.githubusercontent.com/yaniv256/openai-realtime-meeting-assistant/main/index.html",
		"About voice replies: you have a speaker button in the topbar. By default it is off and you see text toasts. If you click it to turn it on, I will also speak my replies aloud through the meeting audio.",
		"About security: the app has no built-in authentication — anyone with the URL can join. To host it securely, add authentication in front of it (e.g. nginx basic auth, VPN, or an OAuth proxy). The app itself does not manage user accounts.",
		"About authorship: the original app was written by Sean DuBois, an engineer at OpenAI and creator of the Pion Go WebRTC library that powers OpenAI's Realtime API. This version running at realtimevoice.dev was adapted and extended by Yaniv Ben-Ami.",
		"Your primary job is to operate the Kanban board by voice on behalf of the participants.",
		"Listen to the user and decide whether they want to create a ticket, move a ticket between columns, add tags to a ticket, update a ticket, delete a ticket, or do nothing.",
		"Use the board card ids exactly as provided when operating on existing tickets.",
		"Users may say ticket, card, task, issue, or sticky note; treat those as Kanban cards.",
		"Available columns are Backlog, In Progress, Blocked, and Done.",
		"This is used during standups and meetings. Treat concrete first-person status updates as implicit board operations; do not wait for the user to say create a ticket.",
		"If a user says they shipped, fixed, completed, closed, or finished work, move an existing related ticket to Done if one exists; otherwise create a concise Done ticket.",
		"If a user says they started, began, picked up, or are working on something, move an existing related ticket to In Progress if one exists; otherwise create a concise In Progress ticket.",
		"If a user says they are blocked, waiting on something, dependent on another team, or that work might slip, move or create the related ticket in Blocked and add blocked, dependency, or risk tags as appropriate.",
		"Track meeting context across turns. If a follow-up sentence adds dependency, blocker, or schedule-risk context for the most recently discussed related card, update or move that existing ticket instead of creating a duplicate.",
		"If a transcript includes a speaker label such as Sean:, do not include the label in the title; use it only as context for notes or tags when useful.",
		"If a user asks to start, work on, pick up, or begin a ticket, move it to In Progress.",
		"If a user asks to block, mark blocked, or note a dependency for a ticket, move it to Blocked and preserve the blocker details in notes.",
		"If a user asks to ship, finish, complete, close, or mark done, move it to Done.",
		"If a user asks to park, punt, defer, or move something back, move it to Backlog.",
		"If a user asks to add a tag, call add_tags; do not replace existing tags.",
		"If one transcript contains multiple status updates, call one tool for each board operation.",
		"Audio gain: you have get_mic_gains and set_mic_gain tools. If someone seems hard to hear or their speech is being misunderstood, call get_mic_gains to see current levels, then call set_mic_gain to boost their track (e.g. gain=2.0). Proactively adjust when you notice comprehension issues.",
		"Speaking and tool calls are independent. Your spoken words are always heard and transcribed to the room. Call a board tool (create_ticket, move_ticket, etc.) when participants report work status. Call do_nothing only when you cannot decide any other tool. For pure conversation — direct questions, greetings, app questions — just speak without calling any tool.",
		"If the user directly addresses you (e.g. 'can you hear me', 'are you there', 'assistant', any direct question), speak your answer naturally. Do not call do_nothing just to acknowledge.",
		"If the user asks for a board operation or gives an implicit status update, call the relevant tool.",
		"If the user is only wrapping up, handing off, giving filler, or saying something like That's it from me, do nothing (no tool call needed).",
		"If the user is not asking for a board operation and not giving a status update, do not call any tool — just stay quiet or speak if directly addressed.",
		fmt.Sprintf("Current Kanban board JSON: %s", app.boardContextJSON()),
		fmt.Sprintf("Voice mode: %s. Your spoken words are always transcribed and streamed as live text to all participants regardless of voice mode. Voice mode controls whether your audio is audible — ON means the room hears your voice, OFF means text only.", func() string {
			if voiceMode {
				return "ON"
			}
			return "OFF"
		}()),
	}, " ")
}

func (app *kanbanBoardApp) boardContextJSON() string {
	raw, err := json.Marshal(app.snapshotState().Cards)
	if err != nil {
		return "[]"
	}

	return string(raw)
}

func (app *kanbanBoardApp) kanbanTools() []map[string]any {
	statusProperty := map[string]any{
		"type":        "string",
		"description": "Kanban column for the ticket.",
		"enum":        []string{"Backlog", "In Progress", "Blocked", "Done"},
	}
	tagsProperty := map[string]any{
		"type":        "array",
		"description": "Short labels that capture people, area, state, or risk. Use blocked/dependency/risk tags for blockers when appropriate.",
		"items":       map[string]any{"type": "string"},
	}

	return []map[string]any{
		{
			"type":        "function",
			"name":        "create_ticket",
			"description": "Create a new Kanban ticket/card for explicit requests or implicit meeting status updates such as shipped, started, or blocked work.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title":  map[string]any{"type": "string", "description": "Concise title for the work, without speaker prefixes such as Sean:."},
					"notes":  map[string]any{"type": "string", "description": "Useful context from the utterance, including blocker, dependency, or schedule-risk details."},
					"tags":   tagsProperty,
					"status": statusProperty,
				},
				"required":             []string{"title", "notes", "tags"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "move_ticket",
			"description": "Move an existing Kanban ticket/card to another column, including Blocked when work is waiting on a dependency.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"status":  statusProperty,
				},
				"required":             []string{"card_id", "status"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "add_tags",
			"description": "Add one or more tags to an existing Kanban ticket/card without removing existing tags.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"tags":    tagsProperty,
				},
				"required":             []string{"card_id", "tags"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "update_ticket",
			"description": "Update the title or notes of an existing Kanban ticket/card. Use this to merge follow-up standup details, dependency details, or slip-risk context into the existing notes.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"title":   map[string]any{"type": "string", "description": "Replacement title, when the existing title should be made clearer."},
					"notes":   map[string]any{"type": "string", "description": "Full replacement notes. Preserve useful existing notes while adding the new context."},
				},
				"required":             []string{"card_id"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "delete_ticket",
			"description": "Delete an existing Kanban ticket/card.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
				},
				"required":             []string{"card_id"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "fetch_url",
			"description": "Fetch the text content of a public URL. Use this to read files from the app's GitHub repo (github.com/yaniv256/openai-realtime-meeting-assistant) when asked how the app works.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"url": map[string]any{"type": "string", "description": "Public URL to fetch (raw GitHub URLs, README, etc.)"},
				},
				"required":             []string{"url"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "get_mic_gains",
			"description": "Returns the current gain multiplier for each active audio track in the room mixer. Use this when someone seems hard to hear or is not being understood — check their gain and boost it if needed. Track keys identify participants.",
			"parameters": map[string]any{
				"type":                 "object",
				"properties":          map[string]any{},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "set_mic_gain",
			"description": "Override the gain multiplier for a specific audio track. Use when a participant is too quiet or too loud. gain=1.0 means AGC-only (no AI override). gain=2.0 doubles their volume. gain=0 removes the override entirely. Maximum useful value is 4.0.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"track_key": map[string]any{"type": "string", "description": "Track key as returned by get_mic_gains"},
					"gain":      map[string]any{"type": "number", "description": "Gain multiplier. 0 removes override. 1.0 = AGC only. 2.0 = double volume. Max 4.0."},
				},
				"required":             []string{"track_key", "gain"},
				"strict": true,
			},
		},
		{
			"type":        "function",
			"name":        "do_nothing",
			"description": "Use this when the user is not asking to operate on the Kanban board, is only wrapping up, or says a handoff phrase like That's it from me.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"reason": map[string]any{"type": "string"},
				},
				"required":             []string{"reason"},
				"strict": true,
			},
		},
	}
}

func (app *kanbanBoardApp) handleRealtimeEvent(raw []byte) {
	var event kanbanRealtimeEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		log.Errorf("Failed to parse OpenAI Realtime event: %v", err)
		return
	}

	switch event.Type {
	case "error":
		if event.Error != nil {
			log.Errorf("OpenAI Realtime error code=%s message=%s", event.Error.Code, event.Error.Message)
			broadcastKanbanEvent("status", event.Error.Message)
		}
	case "conversation.item.input_audio_transcription.completed":
		if t := strings.TrimSpace(event.Transcript); t != "" {
			log.Infof("User said: %q", t)
		}
	case "response.audio_transcript.delta", "response.output_audio_transcript.delta",
		"response.text.delta", "response.output_text.delta":
		if event.Delta != "" {
			broadcastKanbanEvent("ai_transcript_delta", event.Delta)
		}
	case "response.audio_transcript.done", "response.output_audio_transcript.done",
		"response.text.done", "response.output_text.done":
		broadcastKanbanEvent("ai_transcript_done", nil)
	case "response.done":
		// response.done is the sole dispatch point for tool calls — it fires once
		// per turn and carries all function calls in output[]. Using it exclusively
		// prevents the 3x firing that happens when response.output_item.done and
		// response.function_call_arguments.done also dispatch the same call.
		if event.Response == nil {
			return
		}
		for _, outputItem := range event.Response.Output {
			if outputItem.Type == "function_call" {
				app.handleToolCall(outputItem)
			}
		}
	}

}

func (app *kanbanBoardApp) handleToolCall(outputItem kanbanRealtimeOutputItem) {
	if strings.TrimSpace(outputItem.CallID) == "" {
		log.Errorf("Ignoring Kanban tool call %q without call_id", outputItem.Name)
		return
	}

	app.mu.Lock()
	if _, ok := app.handledCalls[outputItem.CallID]; ok {
		app.mu.Unlock()
		return
	}
	app.handledCalls[outputItem.CallID] = struct{}{}
	app.mu.Unlock()

	log.Infof("Tool call: %s args=%s", outputItem.Name, outputItem.Arguments)

	result, changed, err := app.applyToolCall(outputItem)
	if err != nil {
		result = map[string]any{
			"ok":    false,
			"error": err.Error(),
		}
	}

	if err := app.SendEvent(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type":    "function_call_output",
			"call_id": outputItem.CallID,
			"output":  mustMarshalJSON(result),
		},
	}); err != nil {
		log.Errorf("Failed to send Kanban function output: %v", err)
	}

	if !changed {
		return
	}

	broadcastKanbanEvent("board", app.snapshotState())
	if err := app.SendEvent(app.sessionUpdateEvent()); err != nil {
		log.Errorf("Failed to refresh Kanban Realtime session: %v", err)
	}
}

func (app *kanbanBoardApp) applyToolCall(outputItem kanbanRealtimeOutputItem) (map[string]any, bool, error) {
	args := map[string]any{}
	if rawArgs := strings.TrimSpace(outputItem.Arguments); rawArgs != "" {
		if err := json.Unmarshal([]byte(rawArgs), &args); err != nil {
			return nil, false, fmt.Errorf("parse %s arguments: %w", outputItem.Name, err)
		}
	}

	switch outputItem.Name {
	case "create_ticket":
		return app.createTicket(args)
	case "move_ticket":
		return app.moveTicket(args)
	case "add_tags":
		return app.addTags(args)
	case "update_ticket":
		return app.updateTicket(args)
	case "delete_ticket":
		return app.deleteTicket(args)
	case "fetch_url":
		rawURL := asString(args["url"])
		if rawURL == "" {
			return map[string]any{"ok": false, "error": "url is required"}, false, nil
		}
		resp, err := http.Get(rawURL) //nolint:gosec
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}, false, nil
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024)) // 32KB cap
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}, false, nil
		}
		if resp.StatusCode >= 400 {
			return map[string]any{"ok": false, "error": fmt.Sprintf("HTTP %d", resp.StatusCode)}, false, nil
		}
		return map[string]any{"ok": true, "content": string(body)}, false, nil
	case "get_mic_gains":
		gains := roomMixer.gainSnapshot()
		result := make([]map[string]any, 0, len(gains))
		for key, info := range gains {
			result = append(result, map[string]any{
				"track_key":   key,
				"agc_gain":    info.AGCGain,
				"manual_gain": info.ManualGain,
				"applied":     info.Applied,
			})
		}
		return map[string]any{"ok": true, "tracks": result}, false, nil
	case "set_mic_gain":
		trackKey := asString(args["track_key"])
		gain, _ := args["gain"].(float64)
		if trackKey == "" {
			return map[string]any{"ok": false, "error": "track_key is required"}, false, nil
		}
		roomMixer.setManualGain(trackKey, gain)
		log.Infof("AI set mic gain: track=%s gain=%.2f", trackKey, gain)
		return map[string]any{"ok": true, "track_key": trackKey, "gain": gain}, false, nil
	case "do_nothing":
		reason := asString(args["reason"])
		if reason == "" {
			reason = "No board update requested."
		}
		return map[string]any{
			"ok":     true,
			"reason": reason,
		}, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported function %q", outputItem.Name)
	}
}

func (app *kanbanBoardApp) createTicket(args map[string]any) (map[string]any, bool, error) {
	title := asString(args["title"])
	if title == "" {
		return nil, false, fmt.Errorf("title is required")
	}

	notes := asString(args["notes"])
	tags := uniqueStrings(asStringSlice(args["tags"]))
	status := kanbanStatusBacklog
	if rawStatus, ok := args["status"]; ok {
		parsedStatus, err := parseKanbanStatus(rawStatus)
		if err != nil {
			return nil, false, err
		}
		status = parsedStatus
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	card := kanbanCard{
		ID:     app.createCardIDLocked(),
		Status: status,
		Title:  title,
		Notes:  notes,
		Tags:   tags,
	}
	app.cards = append(app.cards, card)
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"created": true,
		"card":    cloneKanbanCard(card),
	}, true, nil
}

func (app *kanbanBoardApp) moveTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	status, err := parseKanbanStatus(args["status"])
	if err != nil {
		return nil, false, err
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	card.Status = status
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"moved":   true,
		"card_id": cardID,
		"status":  status,
	}, true, nil
}

func (app *kanbanBoardApp) addTags(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	tags := uniqueStrings(asStringSlice(args["tags"]))

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	card.Tags = uniqueStrings(append(card.Tags, tags...))
	app.touchLocked()

	return map[string]any{
		"ok":         true,
		"tags_added": true,
		"card_id":    cardID,
		"tags":       append([]string(nil), tags...),
	}, true, nil
}

func (app *kanbanBoardApp) updateTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	title := asString(args["title"])
	notes := asString(args["notes"])

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	if title != "" {
		card.Title = title
	}
	if notes != "" {
		card.Notes = notes
	}
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"updated": true,
		"card_id": cardID,
	}, true, nil
}

func (app *kanbanBoardApp) deleteTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	index := -1
	for candidateIndex, card := range app.cards {
		if card.ID == cardID {
			index = candidateIndex
			break
		}
	}
	if index == -1 {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	deleted := cloneKanbanCard(app.cards[index])
	app.cards = append(app.cards[:index], app.cards[index+1:]...)
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"deleted": true,
		"card_id": cardID,
		"deleted_card": map[string]any{
			"title":  deleted.Title,
			"notes":  deleted.Notes,
			"tags":   deleted.Tags,
			"status": deleted.Status,
		},
	}, true, nil
}

func (app *kanbanBoardApp) snapshotState() kanbanBoardState {
	app.mu.Lock()
	defer app.mu.Unlock()

	state := kanbanBoardState{
		Cards: cloneKanbanCards(app.cards),
	}
	if !app.updatedAt.IsZero() {
		state.UpdatedAt = app.updatedAt.UTC().Format(time.RFC3339Nano)
	}

	return state
}

func (app *kanbanBoardApp) createCardIDLocked() string {
	for {
		cardID := fmt.Sprintf("kanban-card-%03d", app.nextCreatedIndex)
		app.nextCreatedIndex++
		if _, exists := app.findCardLocked(cardID); exists {
			continue
		}
		return cardID
	}
}

func (app *kanbanBoardApp) findCardLocked(cardID string) (*kanbanCard, bool) {
	for index := range app.cards {
		if app.cards[index].ID == cardID {
			return &app.cards[index], true
		}
	}

	return nil, false
}

func (app *kanbanBoardApp) touchLocked() {
	app.updatedAt = time.Now().UTC()
}

func cloneKanbanCards(cards []kanbanCard) []kanbanCard {
	clonedCards := make([]kanbanCard, 0, len(cards))
	for _, card := range cards {
		clonedCards = append(clonedCards, cloneKanbanCard(card))
	}

	return clonedCards
}

func cloneKanbanCard(card kanbanCard) kanbanCard {
	return kanbanCard{
		ID:     card.ID,
		Status: card.Status,
		Title:  card.Title,
		Notes:  card.Notes,
		Tags:   append([]string(nil), card.Tags...),
	}
}

func asString(value any) string {
	candidate, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(candidate)
}

func asStringSlice(value any) []string {
	rawValues, ok := value.([]any)
	if !ok {
		return nil
	}

	values := make([]string, 0, len(rawValues))
	for _, rawValue := range rawValues {
		if value := asString(rawValue); value != "" {
			values = append(values, value)
		}
	}

	return values
}

func parseKanbanStatus(value any) (kanbanStatus, error) {
	status := kanbanStatus(asString(value))
	for _, candidate := range kanbanStatuses {
		if candidate == status {
			return status, nil
		}
	}

	return "", fmt.Errorf("unknown Kanban status: %v", value)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalizedValue := strings.TrimSpace(value)
		if normalizedValue == "" {
			continue
		}
		if _, ok := seen[normalizedValue]; ok {
			continue
		}
		seen[normalizedValue] = struct{}{}
		result = append(result, normalizedValue)
	}

	return result
}

func mustMarshalJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return `{"ok":false,"error":"Could not encode function output."}`
	}

	return string(raw)
}

func sendKanbanEvent(websocket *threadSafeWriter, event string, data any) error {
	raw, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		return err
	}

	return websocket.WriteJSON(&websocketMessage{
		Event: "kanban",
		Data:  string(raw),
	})
}

func broadcastKanbanEvent(event string, data any) {
	raw, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		log.Errorf("Failed to encode Kanban event: %v", err)
		return
	}

	listLock.RLock()
	websockets := make([]*threadSafeWriter, 0, len(peerConnections))
	for _, state := range peerConnections {
		if state.websocket != nil {
			websockets = append(websockets, state.websocket)
		}
	}
	listLock.RUnlock()

	for _, websocket := range websockets {
		if err := websocket.WriteJSON(&websocketMessage{
			Event: "kanban",
			Data:  string(raw),
		}); err != nil {
			log.Errorf("Failed to send Kanban event: %v", err)
		}
	}
}

// IsConnected reports whether the OpenAI Realtime session is active.
func (app *kanbanBoardApp) IsConnected() bool {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.connected
}

func (app *kanbanBoardApp) IsVoiceMode() bool {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.voiceMode
}

// Disconnect closes the OpenAI Realtime peer connection and resets session state
// so JoinConferenceRoom can be called again.
func (app *kanbanBoardApp) Disconnect() error {
	app.mu.Lock()
	pc := app.pc
	app.pc = nil
	app.events = nil
	app.inputTrack = nil
	app.inputEnc = nil
	app.connected = false
	app.closeOnce = sync.Once{} // reset so Close() works on a future instance
	app.mu.Unlock()

	if roomMixer != nil {
		roomMixer.removeSink(realtimeMixedAudioSinkKey)
	}
	if pc != nil {
		return pc.Close()
	}
	return nil
}
