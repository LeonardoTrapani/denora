# Intercom channel example

This project receives verified Intercom webhook notifications at
`POST /channels/intercom/webhook`. Intercom validates the same URL with
`HEAD /channels/intercom/webhook`.

It demonstrates:

- exporting a first-party `channel`;
- exporting the project-owned official Intercom `client`;
- dispatching user-created and user-replied conversations through canonical
  workspace-scoped identity;
- exposing a narrow tool that retrieves the already-bound conversation;
- testing the official client in Node and workerd through fail-closed Fetch.

Required environment variables:

```sh
INTERCOM_ACCESS_TOKEN=...
INTERCOM_CLIENT_SECRET=...
INTERCOM_WORKSPACE_ID=...
INTERCOM_REGION=us
```

`INTERCOM_REGION` may be `us`, `eu`, or `au` and defaults to `us`. The official
SDK currently targets API version `2.14`, so the example pins that version.
Webhook topic items remain provider-native and version-tolerant.

Configure the complete HTTPS endpoint and desired topics in Intercom's
Developer Hub. Installation, OAuth, permissions, subscriptions, token lookup,
deduplication, and outbound inbox policy remain application-owned.
