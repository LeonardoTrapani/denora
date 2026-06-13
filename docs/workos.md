# WorkOS Setup

Denora uses WorkOS AuthKit with sealed sessions. Keep these values in sync between the WorkOS dashboard and the Denora environment.

## Environment

- `WORKOS_API_KEY`: WorkOS API key for the selected environment.
- `WORKOS_CLIENT_ID`: WorkOS client ID for the AuthKit application.
- `WORKOS_COOKIE_PASSWORD`: exactly 32 characters. WorkOS requires this length for sealed session cookies, and the server fails config loading if it is wrong.
- `CSRF_SECRET`: non-empty secret used to sign logout CSRF tokens.
- `DENORA_WEB_ORIGINS`: comma-separated allowed web origins. Defaults to `http://localhost:3000`.
- `DENORA_COOKIE_DOMAIN`: optional cookie domain. Leave empty locally unless testing across subdomains.

## Dashboard

- Redirect URI: add the server callback URL, for example `http://localhost:3000/auth/callback` or the deployed API origin plus `/auth/callback`.
- Sign-in endpoint: set this to the server login URL, `/auth/login` on the deployed API origin.
- Sign-out redirects: allow every destination Denora may pass as `returnTo`, usually each configured web origin.
- Session settings: review session lifetime, access token duration, and inactivity timeout for the environment before deploying.

## Logout CSRF

`POST /auth/logout` requires a CSRF token from `GET /auth/csrf-token`. The token is signed with `CSRF_SECRET`, expires quickly, and is bound to the current sealed session cookie when one is present.
