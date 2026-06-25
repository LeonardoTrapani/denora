import { afterEach, describe, expect, it, vi } from 'vitest';
import { createResendClient } from '../src/resend-client.ts';

describe('Resend Client', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('retrieves the bound inbound email through Fetch when executed in Node', async () => {
		const expectedUrl = 'https://resend-node.test/emails/receiving/email-node-17';
		const fetcher = vi.fn<typeof globalThis.fetch>(async (url, init) => {
			if (String(url) !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${url}`);
			}
			expect(init?.method).toBe('GET');
			const headers = new Headers(init?.headers);
			expect(headers.get('authorization')).toBe('Bearer re_node_test_key');
			expect(headers.get('user-agent')).toMatch(/^resend-node:/);
			return Response.json(receivedEmail('email-node-17'));
		});
		vi.stubGlobal('fetch', fetcher);
		const client = createResendClient('re_node_test_key', {
			baseUrl: 'https://resend-node.test',
		});

		const result = await client.emails.receiving.get('email-node-17');

		expect(result.error).toBeNull();
		expect(result.data).toMatchObject({
			id: 'email-node-17',
			subject: 'Support request',
			text: 'Please help with my account.',
		});
		expect(fetcher).toHaveBeenCalledOnce();
	});
});

function receivedEmail(id: string): Record<string, unknown> {
	return {
		object: 'email',
		id,
		to: ['support@example.test'],
		from: 'customer@example.test',
		created_at: '2026-06-13T23:20:00.000Z',
		subject: 'Support request',
		html: '<p>Please help with my account.</p>',
		text: 'Please help with my account.',
		bcc: [],
		cc: [],
		reply_to: [],
		last_event: 'received',
		scheduled_at: null,
		headers: {
			from: 'Customer <customer@example.test>',
			'message-id': '<node-message@example.test>',
		},
		attachments: [],
	};
}
