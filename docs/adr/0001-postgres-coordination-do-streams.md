# Postgres Coordinates Runs, Durable Objects Serve Streams

Denora stores product coordination state such as Conversations, messages, Agent Runs, approvals, and audit records in Postgres through Drizzle, while Durable Object SQLite remains the low-latency backing store for Durable Streams-compatible event delivery. This deliberately differs from Flue's mostly SQLite-backed runtime stores because Denora needs global user/account queries, ownership checks, audit surfaces, and mobile pagination, while the stream protocol still benefits from colocated Durable Object reads, long-polling, and SSE.
