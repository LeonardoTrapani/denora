import { afterEach, describe, expect, it, vi } from 'vitest';
import { createResendClient } from '../src/resend-client.ts';

describe('Resend Client', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('retrieves the bound inbound email through Fetch when executed in workerd', async () => {
		const expectedUrl = 'https://resend-worker.test/emails/receiving/email-worker-29';
		const fetcher = vi.fn<typeof globalThis.fetch>(async (url, init) => {
			if (String(url) !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${url}`);
			}
			expect(init?.method).toBe('GET');
			const headers = new Headers(init?.headers);
			expect(headers.get('authorization')).toBe('Bearer re_worker_test_key');
			expect(headers.get('user-agent')).toMatch(/^resend-node:/);
			return Response.json({
				object: 'email',
				id: 'email-worker-29',
				to: ['support@example.test'],
				from: 'customer@example.test',
				created_at: '2026-06-13T23:25:00.000Z',
				subject: 'Worker support request',
				html: '<p>Worker body</p>',
				text: 'Worker body',
				bcc: [],
				cc: [],
				reply_to: [],
				last_event: 'received',
				scheduled_at: null,
				headers: { 'message-id': '<worker-message@example.test>' },
				attachments: [],
			});
		});
		vi.stubGlobal('fetch', fetcher);
		const client = createResendClient('re_worker_test_key', {
			baseUrl: 'https://resend-worker.test',
		});

		const result = await client.emails.receiving.get('email-worker-29');

		expect(result.error).toBeNull();
		expect(result.data).toMatchObject({
			id: 'email-worker-29',
			subject: 'Worker support request',
			text: 'Worker body',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		expect(globalThis.process).toBeDefined();
		expect(globalThis.Buffer).toBeDefined();
	});
});
