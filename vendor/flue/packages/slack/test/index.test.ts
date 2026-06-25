import { Hono } from 'hono';
import { describe, expect, it, vi } from 'vitest';
import {
	createSlackChannel,
	InvalidSlackConversationKeyError,
	type SlackChannel,
	type SlackEventsApiPayload,
} from '../src/index.ts';

const encoder = new TextEncoder();

describe('createSlackChannel()', () => {
	it('publishes only configured provider surfaces when callbacks are provided', () => {
		const events = createSlackChannel({
			signingSecret: 'secret',
			events() {},
		});
		const interactions = createSlackChannel({
			signingSecret: 'secret',
			interactions() {},
		});
		const commands = createSlackChannel({
			signingSecret: 'secret',
			commands() {},
		});

		expect(events.routes.map(({ method, path }) => ({ method, path }))).toEqual([
			{ method: 'POST', path: '/events' },
		]);
		expect(interactions.routes.map(({ method, path }) => ({ method, path }))).toEqual([
			{ method: 'POST', path: '/interactions' },
		]);
		expect(commands.routes.map(({ method, path }) => ({ method, path }))).toEqual([
			{ method: 'POST', path: '/commands' },
		]);
	});

	it('rejects configuration when no provider handler is configured', () => {
		expect(() =>
			createSlackChannel({
				signingSecret: 'secret',
			}),
		).toThrow('requires an events, interactions, or commands handler');
	});

	it('passes the provider-native Events API envelope and Hono context when a request is valid', async () => {
		let received: SlackEventsApiPayload | undefined;
		let retryNumber: string | undefined;
		let retryReason: string | undefined;
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events({ c, payload }) {
				received = payload;
				retryNumber = c.req.header('x-slack-retry-num');
				retryReason = c.req.header('x-slack-retry-reason');
			},
		});
		const payload = eventCallback({
			type: 'app_mention',
			channel: 'C123',
			ts: '1717971234.0012',
			event_ts: '1717971234.0012',
			text: '<@U1> hello',
			user: 'U2',
		});

		const response = await channelApp(slack).request(
			await signedJsonRequest('/events', payload, {
				'x-slack-retry-num': '1',
				'x-slack-retry-reason': 'http_timeout',
			}),
		);

		expect(response.status).toBe(200);
		expect(received).toEqual(payload);
		expect(retryNumber).toBe('1');
		expect(retryReason).toBe('http_timeout');
	});

	it('preserves official event narrowing when an assistant event is received', async () => {
		let threadTs: string | undefined;
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events({ payload }) {
				if (
					payload.type === 'event_callback' &&
					payload.event.type === 'assistant_thread_started'
				) {
					threadTs = payload.event.assistant_thread.thread_ts;
				}
			},
		});

		const response = await channelApp(slack).request(
			await signedJsonRequest(
				'/events',
				eventCallback({
					type: 'assistant_thread_started',
					assistant_thread: {
						user_id: 'U123',
						context: { channel_id: 'C123', team_id: 'T123' },
						channel_id: 'C123',
						thread_ts: '1717971234.0012',
					},
					event_ts: '1717971234.0012',
				}),
			),
		);

		expect(response.status).toBe(200);
		expect(threadTs).toBe('1717971234.0012');
	});

	it('forwards bot messages, message subtypes, and unmodeled events when they are authenticated', async () => {
		const events = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events,
		});
		const app = channelApp(slack);
		const providerEvents = [
			{
				type: 'message',
				subtype: 'bot_message',
				channel: 'C123',
				channel_type: 'channel',
				ts: '1717971234.0012',
				event_ts: '1717971234.0012',
				text: 'automation update',
				bot_id: 'B123',
			},
			{
				type: 'message',
				subtype: 'file_share',
				channel: 'C123',
				channel_type: 'channel',
				ts: '1717971234.0013',
				event_ts: '1717971234.0013',
				text: 'report',
				user: 'U123',
				files: [],
			},
			{
				type: 'reaction_added',
				user: 'U123',
				reaction: 'white_check_mark',
				item_user: 'U456',
				item: { type: 'message', channel: 'C123', ts: '1717971234.0012' },
				event_ts: '1717971234.0014',
			},
			{
				type: 'future_event_type',
				resource_id: 'R123',
			},
		];

		for (const [index, event] of providerEvents.entries()) {
			const response = await app.request(
				await signedJsonRequest('/events', eventCallback(event, `Ev${index + 1}`)),
			);
			expect(response.status).toBe(200);
		}

		expect(events).toHaveBeenCalledTimes(4);
		expect(events.mock.calls.map(([input]) => input.payload.event)).toEqual(providerEvents);
	});

	it('passes app rate-limit notifications when the request is authenticated', async () => {
		const events = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events,
		});
		const payload = {
			type: 'app_rate_limited',
			token: 'verification-token',
			team_id: 'T123',
			minute_rate_limited: 1717971240,
			api_app_id: 'A123',
		};

		const response = await channelApp(slack).request(await signedJsonRequest('/events', payload));

		expect(response.status).toBe(200);
		expect(events.mock.calls[0]?.[0].payload).toEqual(payload);
	});

	it('handles URL verification internally when the request is authenticated', async () => {
		const events = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events,
		});

		const response = await channelApp(slack).request(
			await signedJsonRequest('/events', {
				type: 'url_verification',
				challenge: 'challenge-value',
			}),
		);

		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({ challenge: 'challenge-value' });
		expect(events).not.toHaveBeenCalled();
	});

	it('passes provider-native slash-command fields when a request is authenticated', async () => {
		const commands = vi.fn(({ c, payload }) =>
			c.json({ received: payload.command, text: payload.text }),
		);
		const slack = createSlackChannel({
			signingSecret: 'secret',
			commands,
		});
		const fields = {
			api_app_id: 'A123',
			team_id: 'T123',
			team_domain: 'acme',
			channel_id: 'C123',
			channel_name: 'automation',
			user_id: 'U123',
			user_name: 'river',
			command: '/triage',
			text: 'incident 42',
			trigger_id: 'trigger-capability',
			response_url: 'https://hooks.slack.test/commands/response',
		};

		const response = await channelApp(slack).request(
			await signedCommandRequest('/commands', fields),
		);

		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({ received: '/triage', text: 'incident 42' });
		expect(commands.mock.calls[0]?.[0]).toMatchObject({
			c: expect.any(Object),
			payload: fields,
		});
	});

	it('forwards slash commands from different workspaces and enterprise installations', async () => {
		const commands = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			commands,
		});
		const fields = {
			api_app_id: 'A123',
			team_id: 'T123',
			channel_id: 'C123',
			user_id: 'U123',
			command: '/triage',
			text: '',
			trigger_id: 'trigger-capability',
			response_url: 'https://hooks.slack.test/commands/response',
		};
		const app = channelApp(slack);

		const otherWorkspace = await app.request(
			await signedCommandRequest('/commands', { ...fields, team_id: 'T999' }),
		);
		const enterpriseInstall = await app.request(
			await signedCommandRequest('/commands', {
				...fields,
				enterprise_id: 'E123',
				is_enterprise_install: 'true',
			}),
		);

		expect(otherWorkspace.status).toBe(200);
		expect(enterpriseInstall.status).toBe(200);
		expect(commands.mock.calls.map(([input]) => input.payload.team_id)).toEqual(['T999', 'T123']);
	});

	it('passes provider-native interactions without collapsing multiple actions', async () => {
		const interactions = vi.fn((_input: { payload: unknown }) => ({ accepted: true }));
		const slack = createSlackChannel({
			signingSecret: 'secret',
			interactions,
		});
		const payload = {
			type: 'block_actions',
			api_app_id: 'A123',
			team: { id: 'T123', domain: 'acme' },
			user: { id: 'U123', username: 'river' },
			trigger_id: 'action-trigger-capability',
			response_url: 'https://hooks.slack.test/actions/response',
			container: { type: 'message', channel_id: 'C123', message_ts: '1717971234.0012' },
			channel: { id: 'C123', name: 'automation' },
			message: { ts: '1717971234.0012', thread_ts: '1717971234.0001' },
			actions: [
				{ type: 'button', action_id: 'approve', block_id: 'decision', value: 'yes' },
				{ type: 'button', action_id: 'reject', block_id: 'decision', value: 'no' },
			],
		};

		const response = await channelApp(slack).request(
			await signedFormRequest('/interactions', payload),
		);

		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({ accepted: true });
		expect(interactions.mock.calls[0]?.[0].payload).toEqual(payload);
	});

	it('forwards a future interaction type when its request is authenticated', async () => {
		const interactions = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			interactions,
		});
		const payload = {
			type: 'future_interaction',
			api_app_id: 'A123',
			team: { id: 'T123' },
			user: { id: 'U123' },
			future_field: { value: true },
		};

		const response = await channelApp(slack).request(
			await signedFormRequest('/interactions', payload),
		);

		expect(response.status).toBe(200);
		expect(interactions.mock.calls[0]?.[0].payload).toEqual(payload);
	});

	it('returns provider-native view validation JSON when a view submission is received', async () => {
		const slack = createSlackChannel({
			signingSecret: 'secret',
			interactions: ({ payload }) => {
				if (payload.type !== 'view_submission') return;
				return {
					response_action: 'errors',
					errors: { email: 'Enter a valid email address.' },
				};
			},
		});

		const response = await channelApp(slack).request(
			await signedFormRequest('/interactions', {
				type: 'view_submission',
				api_app_id: 'A123',
				team: { id: 'T123' },
				user: { id: 'U123' },
				view: {
					id: 'V123',
					callback_id: 'settings',
					state: { values: {} },
				},
			}),
		);

		expect(await response.json()).toEqual({
			response_action: 'errors',
			errors: { email: 'Enter a valid email address.' },
		});
	});

	it('uses an empty 200 when no result is returned and passes Hono responses through', async () => {
		const defaultChannel = createSlackChannel({
			signingSecret: 'secret',
			events() {},
		});
		const responseChannel = createSlackChannel({
			signingSecret: 'secret',
			events: ({ c }) => c.text('accepted', 202),
		});
		const payload = eventCallback({
			type: 'reaction_added',
			user: 'U123',
			reaction: 'eyes',
			item_user: 'U456',
			item: { type: 'message', channel: 'C123', ts: '1717971234.0012' },
			event_ts: '1717971234.0013',
		});

		const defaultResponse = await channelApp(defaultChannel).request(
			await signedJsonRequest('/events', payload),
		);
		const response = await channelApp(responseChannel).request(
			await signedJsonRequest('/events', payload),
		);

		expect(defaultResponse.status).toBe(200);
		expect(await defaultResponse.text()).toBe('');
		expect(response.status).toBe(202);
		expect(await response.text()).toBe('accepted');
	});

	it('rejects malformed transport and oversized bodies before callbacks run', async () => {
		const events = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			bodyLimit: 32,
			events,
		});
		const app = channelApp(slack);

		const wrongType = await app.request(
			new Request('https://example.test/events', {
				method: 'POST',
				headers: { 'content-type': 'text/plain' },
				body: 'payload',
			}),
		);
		const malformed = await app.request(await signedRequest('/events', '{', 'application/json'));
		const oversized = await app.request(
			await signedRequest('/events', JSON.stringify({ value: 'x'.repeat(64) }), 'application/json'),
		);

		expect(wrongType.status).toBe(415);
		expect(malformed.status).toBe(400);
		expect(oversized.status).toBe(413);
		expect(events).not.toHaveBeenCalled();
	});

	it('rejects stale signatures and tampered request bytes', async () => {
		const events = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events,
		});
		const payload = eventCallback({ type: 'future_event_type' });
		const app = channelApp(slack);

		const stale = await app.request(
			await signedJsonRequest('/events', payload, {}, Math.floor(Date.now() / 1000) - 301),
		);
		const signed = await signedJsonRequest('/events', payload);
		const tampered = await app.request(
			new Request(signed.url, {
				method: signed.method,
				headers: signed.headers,
				body: `${await signed.text()} `,
			}),
		);

		expect(stale.status).toBe(401);
		expect(tampered.status).toBe(401);
		expect(events).not.toHaveBeenCalled();
	});

	it('forwards enterprise Events API and interaction payloads when they are authenticated', async () => {
		const events = vi.fn();
		const interactions = vi.fn();
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events,
			interactions,
		});
		const app = channelApp(slack);

		const eventResponse = await app.request(
			await signedJsonRequest('/events', {
				...eventCallback({ type: 'future_event_type' }),
				authorizations: [
					{
						enterprise_id: 'E123',
						team_id: null,
						user_id: 'U123',
						is_bot: true,
						is_enterprise_install: true,
					},
				],
			}),
		);
		const interactionResponse = await app.request(
			await signedFormRequest('/interactions', {
				type: 'shortcut',
				team: { id: 'T123' },
				enterprise: { id: 'E123' },
				user: { id: 'U123' },
				is_enterprise_install: true,
				callback_id: 'open_triage',
				trigger_id: 'shortcut-trigger',
			}),
		);

		expect(eventResponse.status).toBe(200);
		expect(interactionResponse.status).toBe(200);
		expect(events).toHaveBeenCalledOnce();
		expect(interactions).toHaveBeenCalledOnce();
	});

	it('lets the Hono error handler handle callback failures', async () => {
		const failure = new Error('failed');
		const throwing = createSlackChannel({
			signingSecret: 'secret',
			events() {
				throw failure;
			},
		});
		const app = channelApp(throwing);
		let received: Error | undefined;
		app.onError((error, c) => {
			received = error;
			return c.text('handled', 503);
		});
		const payload = eventCallback({ type: 'future_event_type' });

		const response = await app.request(await signedJsonRequest('/events', payload));

		expect(response.status).toBe(503);
		expect(await response.text()).toBe('handled');
		expect(received).toBe(failure);
	});

	it('round-trips canonical thread references when identifiers contain reserved characters', () => {
		const slack = createSlackChannel({
			signingSecret: 'secret',
			events() {},
		});
		const ref = { teamId: 'T:123', channelId: 'C/123', threadTs: '1717.00?#' };
		const key = slack.conversationKey(ref);

		expect(slack.parseConversationKey(key)).toEqual(ref);
		expect(() => slack.parseConversationKey(`github:v1:${key}`)).toThrow(
			InvalidSlackConversationKeyError,
		);
	});
});

function channelApp(channel: SlackChannel): Hono {
	const app = new Hono();
	for (const route of channel.routes) app.on(route.method, route.path, route.handler);
	return app;
}

function eventCallback(event: Record<string, unknown>, eventId = 'Ev123') {
	return {
		token: 'verification-token',
		team_id: 'T123',
		api_app_id: 'A123',
		event,
		type: 'event_callback',
		event_id: eventId,
		event_time: 1717971234,
	};
}

async function signedJsonRequest(
	path: string,
	payload: unknown,
	headers: Record<string, string> = {},
	timestamp = Math.floor(Date.now() / 1000),
): Promise<Request> {
	return signedRequest(path, JSON.stringify(payload), 'application/json', headers, timestamp);
}

async function signedFormRequest(path: string, payload: unknown): Promise<Request> {
	return signedRequest(
		path,
		new URLSearchParams({ payload: JSON.stringify(payload) }).toString(),
		'application/x-www-form-urlencoded',
	);
}

async function signedCommandRequest(
	path: string,
	fields: Record<string, string>,
): Promise<Request> {
	return signedRequest(
		path,
		new URLSearchParams(fields).toString(),
		'application/x-www-form-urlencoded',
	);
}

async function signedRequest(
	path: string,
	body: string,
	contentType: string,
	headers: Record<string, string> = {},
	timestamp = Math.floor(Date.now() / 1000),
): Promise<Request> {
	const signed = `v0:${timestamp}:${body}`;
	const key = await crypto.subtle.importKey(
		'raw',
		encoder.encode('secret'),
		{ name: 'HMAC', hash: 'SHA-256' },
		false,
		['sign'],
	);
	const signature = new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode(signed)));
	const hex = Array.from(signature, (byte) => byte.toString(16).padStart(2, '0')).join('');
	return new Request(`https://example.test${path}`, {
		method: 'POST',
		headers: {
			'content-type': contentType,
			'x-slack-request-timestamp': String(timestamp),
			'x-slack-signature': `v0=${hex}`,
			...headers,
		},
		body,
	});
}
