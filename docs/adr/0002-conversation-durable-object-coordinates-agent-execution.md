# Conversation Durable Object Coordinates Agent Execution

Denora coordinates chat-triggered agent execution inside a Conversation Durable Object rather than in the main Worker or a per-run object. This follows Flue's attached-agent model more closely: one durable conversation coordinator serializes prompts, owns the live conversation stream, bridges to the Pi agent runtime, and writes product state to Postgres while per-run records remain the execution/audit boundary.
