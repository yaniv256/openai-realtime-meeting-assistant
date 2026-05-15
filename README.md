# Realtime Meeting Assistant

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Go](https://img.shields.io/badge/Built_with-Go-blue)
![WebRTC](https://img.shields.io/badge/Uses-WebRTC-blueviolet)
![OpenAI API](https://img.shields.io/badge/Powered_by-OpenAI_API-orange)

A real-time meeting assistant that listens to your room, updates a shared Kanban board by voice, and speaks back through the meeting audio. Multiple participants can join the same WebRTC room; the AI hears the mix, manages the board, and responds aloud when addressed.

Live at **[realtimevoice.dev/meeting](https://realtimevoice.dev/meeting)**.

Built on [Pion WebRTC](https://github.com/pion/webrtc) and the [OpenAI Realtime API](https://platform.openai.ai/docs/guides/realtime-webrtc). Originally based on [openai/openai-realtime-meeting-assistant](https://github.com/openai/openai-realtime-meeting-assistant); extended with voice output, live transcript, per-participant AGC, and mobile PWA support.

> [!IMPORTANT]
> This app has no built-in authentication. Anyone with the URL can join the room. To restrict access, put an auth proxy (nginx basic auth, OAuth, VPN) in front of it.

---

## Features

- **Voice Kanban** — say "I started the ICE restart ticket" and the card moves to In Progress
- **AI voice output** — the assistant speaks back through the room's audio mix when addressed
- **Live transcript toast** — the AI's words stream to the screen in real time as it speaks
- **Per-participant AGC** — automatic gain control normalizes mic levels across participants; AI can query and override gains with `get_mic_gains` / `set_mic_gain`
- **Multi-participant WebRTC** — camera + audio for all participants, mixed to the AI
- **Mobile PWA** — installable via "Add to Home Screen" with full-screen standalone mode
- **Delete with undo context** — deleted cards are returned in the tool result so the AI can recreate them if asked

---

## Running locally

### Prerequisites

- Go 1.24+
- Opus library available via `pkg-config`

```bash
# macOS
brew install opus pkg-config

# Ubuntu/Debian
sudo apt-get install libopus-dev pkg-config
```

### Steps

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yaniv256/openai-realtime-meeting-assistant.git
   cd openai-realtime-meeting-assistant
   ```

2. **Set your OpenAI API key:**

   ```bash
   export OPENAI_API_KEY=your_key_here
   ```

3. **Run:**

   ```bash
   go run .
   ```

   Open [http://localhost:3000/meeting](http://localhost:3000/meeting). Use `-addr :8080` for a different port.

4. **Join the room** — click **Join room**, allow camera and microphone access. Open the same URL in another tab or device to join as a second participant.

> **Use headphones** to avoid echo. Background audio is picked up by the room mix and may trigger board updates.

---

## Voice interaction

The assistant is quiet by default and only responds when directly addressed. Speak naturally — it picks up standup updates without explicit commands.

**Board operations:**

| What you say | What happens |
|---|---|
| "I started the ICE restart ticket" | Moves matching card to In Progress |
| "The DTLS work is blocked on transport shutdown" | Moves to Blocked, adds notes |
| "We shipped the RTP packetizer" | Moves to Done |
| "Create a ticket for simulcast subscription controls" | Creates a new card |
| "Add the bandwidth tag to the simulcast card" | Adds tag without replacing others |
| "Delete the packet retransmission buffer ticket" | Deletes card; full content returned to AI context for undo |

**Addressing the AI:**

Say "assistant" or ask it a direct question. It responds in voice (if the speaker button is on) and via the live transcript toast.

**Voice/mute toggle:** The robot icon in the top bar controls whether the AI speaks aloud. When muted, it still processes board updates and shows transcript text.

**Mic gain:** If someone sounds too quiet, the AI can detect this and call `set_mic_gain` to boost their track. You can also ask it: "Can you boost John's mic?"

---

## Customization

All AI behaviour is in `kanban.go`:

| What to change | Where |
|---|---|
| Initial board cards | `initialKanbanBoardCards` |
| AI instructions | `sessionInstructions()` |
| Tools exposed to the model | `kanbanTools()` |
| Realtime model | `OPENAI_REALTIME_MODEL` env var (default: `gpt-realtime-2`) |
| AGC constants | `agcTargetRMS`, `agcSpeechFloor`, `agcMaxGain` in `audio_mixer.go` |
| Browser UI | `index.html` |
| HTTP bind address | `-addr` flag |

---

## Architecture

```
Browser (WebRTC)          Go server                  OpenAI Realtime API
─────────────────         ─────────────────────      ─────────────────────
Camera + mic    ──────►   Room mixer (Pion)   ──────► gpt-realtime-2
                          Per-source AGC              │
                          ◄──────────────────────────  AI voice track
AI voice        ◄──────   addTrack → all peers        │
Transcript      ◄──────   WS broadcast ◄──────────────  output_audio_transcript.delta
Board updates   ◄──────   WS broadcast ◄──────────────  function_call (tool result)
```

The server mixes all participant audio with automatic gain control and sends the mix to the OpenAI Realtime peer via WebRTC. The AI's audio output arrives as a separate WebRTC track, which the server fans out to all browser peers directly (raw RTP, no re-encoding). Tool calls update the Kanban state and broadcast it to all connected browsers via WebSocket.

---

## Deployment

The repo includes a GitHub Actions workflow (`.github/workflows/deploy.yml`) that SSHes into an EC2 instance and runs a deploy script on every push to `main`. See the workflow file for the expected secrets (`EC2_HOST`, `EC2_SSH_KEY`).

Example deploy script:

```bash
cd /path/to/openai-realtime-meeting-assistant
git pull --ff-only origin main
go build -o meeting-assistant .
# restart with your process manager
```

---

## License

MIT — see [LICENSE](LICENSE).
