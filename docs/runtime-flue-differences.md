# Denora vs Flue Runtime Differences

These runtime differences are intentional before implementation:

- Denora is a secure personal agent product, not a general-purpose framework.
- Product state may remain in Postgres for ownership, auditability, controls, and product queries, while Durable Object runtime state remains the execution truth for live coordination.
- Scheduling transport stays with Alchemy/Cloudflare schedules plus Durable Object alarms; do not copy Flue's scheduling transport wholesale.
- Stream authorization remains Worker-authorized and Durable Object-trusted after the Worker has checked access.
- A frontend SDK or public frontend API is not in scope for this pass.
- Durable Object migrations and global runtime versioning are not in scope for this pass.
- Implementation should copy Flue runtime behavior where applicable while adapting it to Denora's Effect style and module patterns.

Resolved parity notes:

- Stream chunk persistence now follows Flue's buffered writer semantics: assistant stream updates are flushed on a 3-second throttle, explicit `flush()`, or `close()`, rather than eagerly flushing every event.
