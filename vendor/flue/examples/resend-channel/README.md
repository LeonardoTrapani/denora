# Resend channel example

This project receives verified Resend webhook events at
`POST /channels/resend/webhook`.

It demonstrates:

- exporting a first-party `channel`;
- exporting the project-owned official Resend `client`;
- dispatching `email.received` metadata to a message-scoped agent;
- exposing a narrow tool that retrieves the complete already-bound email;
- testing the real client in Node and workerd through fake Fetch.

Required environment variables:

```sh
RESEND_API_KEY=re_...
RESEND_WEBHOOK_SECRET=whsec_...
```

Configure a Resend webhook for `email.received` with the complete deployed
route. Receiving domains, MX records, webhook registration, credentials,
deduplication, attachment storage, and reply policy remain application-owned.
