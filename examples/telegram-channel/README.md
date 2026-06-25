# Telegram channel example

This example shows verified Telegram Bot API webhook ingress, a project-owned
grammY `Api` client, and an application-owned message tool.

Required environment variables:

```sh
TELEGRAM_BOT_TOKEN=123456:replace-with-bot-token
TELEGRAM_WEBHOOK_SECRET_TOKEN=replace_with_random_secret
```

The webhook route is:

```txt
POST /channels/telegram/webhook
```

Configure it with `client.setWebhook(...)`, including the same
`TELEGRAM_WEBHOOK_SECRET_TOKEN`. Telegram polling is intentionally outside this
HTTP channel example.
