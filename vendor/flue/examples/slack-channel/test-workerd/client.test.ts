import { WebClient } from '@slack/web-api';
import { describe, expect, it, vi } from 'vitest';

describe('Slack WebClient', () => {
	it('calls chat.postMessage when a thread reply is sent in workerd', async () => {
		const fetch = vi.fn(async () =>
			Response.json({
				ok: true,
				channel: 'C123',
				ts: '1710000000.000001',
				message: { text: 'hello' },
			}),
		);
		vi.stubGlobal('fetch', fetch);

		try {
			const client = new WebClient('xoxb-test');
			const result = await client.chat.postMessage({
				channel: 'C123',
				thread_ts: '1710000000.000000',
				text: 'hello',
			});

			expect(result.ok).toBe(true);
			expect(result.channel).toBe('C123');
			expect(fetch).toHaveBeenCalledOnce();
			const [url, init] = fetch.mock.calls[0] ?? [];
			expect(String(url)).toBe('https://slack.com/api/chat.postMessage');
			expect(init?.method).toBe('POST');
			expect(new Headers(init?.headers).get('authorization')).toBe('Bearer xoxb-test');
			expect(Object.fromEntries(new URLSearchParams(String(init?.body)))).toMatchObject({
				channel: 'C123',
				thread_ts: '1710000000.000000',
				text: 'hello',
			});
		} finally {
			vi.unstubAllGlobals();
		}
	});

	it('calls assistant.threads.setStatus when an assistant status is shown in workerd', async () => {
		const fetch = vi.fn(async () => Response.json({ ok: true }));
		vi.stubGlobal('fetch', fetch);

		try {
			const client = new WebClient('xoxb-test');
			const result = await client.assistant.threads.setStatus({
				channel_id: 'C123',
				thread_ts: '1710000000.000000',
				status: 'is thinking...',
			});

			expect(result.ok).toBe(true);
			expect(fetch).toHaveBeenCalledOnce();
			const [url, init] = fetch.mock.calls[0] ?? [];
			expect(String(url)).toBe('https://slack.com/api/assistant.threads.setStatus');
			expect(new Headers(init?.headers).get('authorization')).toBe('Bearer xoxb-test');
			expect(Object.fromEntries(new URLSearchParams(String(init?.body)))).toMatchObject({
				channel_id: 'C123',
				thread_ts: '1710000000.000000',
				status: 'is thinking...',
			});
		} finally {
			vi.unstubAllGlobals();
		}
	});

	it('calls start, append, and stop stream methods when text is streamed in workerd', async () => {
		const methods: string[] = [];
		const fetch = vi.fn(async (input: RequestInfo | URL) => {
			const method = String(input).replace('https://slack.com/api/', '');
			methods.push(method);
			return Response.json({
				ok: true,
				channel: 'C123',
				...(method === 'chat.startStream' ? { ts: '1710000000.000001' } : {}),
			});
		});
		vi.stubGlobal('fetch', fetch);

		try {
			const client = new WebClient('xoxb-test');
			const stream = client.chatStream({
				channel: 'C123',
				thread_ts: '1710000000.000000',
				recipient_team_id: 'T123',
				recipient_user_id: 'U123',
				buffer_size: 1,
			});

			await stream.append({ markdown_text: 'Hello' });
			await stream.append({ markdown_text: ' world' });
			await stream.stop();

			expect(methods).toEqual(['chat.startStream', 'chat.appendStream', 'chat.stopStream']);
			for (const call of fetch.mock.calls) {
				expect(new Headers(call[1]?.headers).get('authorization')).toBe('Bearer xoxb-test');
			}
		} finally {
			vi.unstubAllGlobals();
		}
	});
});
