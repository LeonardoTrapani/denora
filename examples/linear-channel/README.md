# Linear channel example

This example shows verified Linear webhooks, a project-owned official
`LinearClient`, and an application-owned tool that posts to issue-comment
threads or agent sessions.

Required environment variables:

```sh
LINEAR_WEBHOOK_SECRET=...
LINEAR_API_KEY=lin_api_...
```

Use `LINEAR_ACCESS_TOKEN` instead of `LINEAR_API_KEY` for an installed OAuth
application. `LINEAR_ORGANIZATION_ID` and `LINEAR_WEBHOOK_ID` optionally pin
the signed endpoint to one fixed integration.

The webhook route is:

```txt
POST /channels/linear/webhook
```
