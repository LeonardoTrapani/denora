# Denora Context

Denora is a secure personal agent product.

## Product Thesis

Denora is not primarily an email client, workflow automation builder, or model wrapper. It is a personal agent layer that owns identity, permissions, approvals, understandable control, and eventually memory across a user's digital life.

The user talks to their own named agent. Denora is the product name; each user's agent has its own identity.

## Core Differentiation

- Agent identity is explicit from the beginning.
- The agent can have its own email identity from day one.
- The model is replaceable; Denora owns the relationship, controls, permissions, audit surface, and action layer.
- Setup should be conversational, not workflow-builder configuration.
- Chat is the primary interface, with structured cards for permissions, approvals, tasks, routines, and action history.
- Security should mean understandable control: users can see what the agent can access, what it wants to do, what it has done, and how to stop or change it.

## Interface Direction

- Mobile-first, with web app support.
- Mostly chat.
- First screen should feel like creating an agent identity, not filling out SaaS setup.
- First screen includes an input box and agent identity/domain selection, then goes straight to the agent.

## Agent Behavior

- The agent should be useful with no integrations as a model-agnostic assistant.
- Integrations are requested by the user; the agent should create/setup them conversationally.
- Proactivity is opt-in. The agent proposes routines such as morning briefings or follow-up reports, and the user chooses when they want them.
- When the agent receives the first email or new class of inbound event, it should ask whether the user likes receiving that kind of report and under what conditions.

## Domain Language

- Agent: the named personal assistant identity owned by a user.
- Thread: one conversation or event-driven interaction with an agent.
- Agent Run: one model/tool execution started from a message, inbound event, or routine.

## Email Identity

- Every agent can have an email identity using a Denora-controlled domain, currently `denora.me`.
- This identity is a capability, not the center of the product.
- Inbound email should become an event/conversation with the agent.
- External actions need verification, rate limits, abuse controls, and auditability even if user-facing pricing is simple.

## Pricing Direction

- Free plan with usage limits.
- Main paid plan around $25/month.
- Higher plan around $200/month can exist later but should not drive the initial product proof.

## Competitor Positioning

- ChatGPT/Claude: strong intelligence, weak durable operational identity and permission layer.
- Lindy/Zapier/Relay: powerful automation, but too workflow-configured and brittle.
- Poke: closer to a personal assistant, but Denora should make identity, boundaries, permissions, and understandable control much more legible.
- Email clients such as Superhuman, Shortwave, HEY: Denora should not become an email client; email is one interoperability surface for delegation.
