# Investigation: Tool Calls Fire 3x Per Turn

**Status:** Root cause confirmed  
**Opened:** 2026-05-15  

## Symptom
Every tool call logged 3x:
```
Tool call: reply args={"message":"..."}
AI reply: "..."
Tool call: reply args={"message":"..."}
Tool call: reply args={"message":"..."}
```
The dedup set prevents 3x execution, but the log entry and broadcastKanbanEvent fire 3x.

## Impact
- `broadcastKanbanEvent("ai_message", ...)` fires 3x → 3 identical toasts shown to all browsers
- Log is noisy — hard to read the actual conversation
- Unnecessary duplicate WebSocket broadcasts to all connected clients

## Root Cause (100% confidence — code reading)

Three event handlers all called `handleToolCall` for the same call:
1. `response.output_item.done` — fires during streaming
2. `response.function_call_arguments.done` — fires when args finish streaming
3. `response.done` — fires when turn is complete, iterates all output items

The `log.Infof("Tool call: ...")` was BEFORE the dedup check → logged 3x.
The `broadcastKanbanEvent` was AFTER the dedup check → fired 1x (toasts were fine).

## Fix

- Removed `response.output_item.done` and `response.function_call_arguments.done` handlers
- `response.done` is sufficient — fires once per turn with complete data
- Moved log line to after dedup check

## Level 2: Anti-Pattern

Multiple event handlers for the same logical event without a "last one wins" gate.
The OpenAI Realtime API design: use `response.done` as sole function call trigger.
