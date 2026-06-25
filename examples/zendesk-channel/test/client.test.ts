import { describe, expect, it, vi } from 'vitest';
import { createZendeskClient } from '../src/zendesk-client.ts';

describe('Zendesk Client', () => {
	it('retrieves the bound ticket through injected Fetch when executed in Node', async () => {
		const ticketId = '9007199254741997';
		const expectedUrl = `https://node-support.zendesk.com/api/v2/tickets/${ticketId}.json`;
		const fetcher = vi.fn<typeof globalThis.fetch>(async (input, init) => {
			const request = new Request(input, init);
			if (request.url !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${request.url}`);
			}
			expect(request.method).toBe('GET');
			expect(request.headers.get('accept')).toBe('application/json');
			expect(request.headers.get('authorization')).toBe(
				`Basic ${Buffer.from('agent-node@example.test/token:zendesk-node-test-token').toString(
					'base64',
				)}`,
			);
			return new Response(
				'{"ticket":{"id":9007199254741997,"subject":"Node customer cannot sign in","status":"open","requester_id":9007199254741999,"assignee_id":52,"organization_id":null,"priority":"high"}}',
				{ headers: { 'content-type': 'application/json' } },
			);
		});
		const client = createZendeskClient({
			subdomain: 'node-support',
			email: 'agent-node@example.test',
			apiToken: 'zendesk-node-test-token',
			fetcher,
		});

		const ticket = await client.getTicket(ticketId);

		expect(ticket).toMatchObject({
			id: ticketId,
			subject: 'Node customer cannot sign in',
			status: 'open',
			requester_id: '9007199254741999',
		});
		expect(fetcher).toHaveBeenCalledOnce();
	});

	it('rejects the client before issuing a request when the subdomain is not a DNS label', async () => {
		const fetcher = vi.fn<typeof globalThis.fetch>();

		expect(() =>
			createZendeskClient({
				subdomain: 'https://attacker.example',
				email: 'agent@example.test',
				apiToken: 'zendesk-test-token',
				fetcher,
			}),
		).toThrow('Zendesk subdomain must be a bare DNS label.');
		expect(fetcher).not.toHaveBeenCalled();
	});
});
