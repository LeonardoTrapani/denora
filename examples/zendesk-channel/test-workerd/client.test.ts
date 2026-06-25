import { describe, expect, it, vi } from 'vitest';
import { createZendeskClient } from '../src/zendesk-client.ts';

describe('Zendesk Client', () => {
	it('retrieves the bound ticket when running in workerd with nodejs_compat', async () => {
		const ticketId = '9007199254743001';
		const expectedUrl = `https://worker-support.zendesk.com/api/v2/tickets/${ticketId}.json`;
		const fetcher = vi.fn<typeof globalThis.fetch>(async (input, init) => {
			const request = new Request(input, init);
			if (request.url !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${request.url}`);
			}
			expect(request.method).toBe('GET');
			expect(request.headers.get('accept')).toBe('application/json');
			expect(request.headers.get('authorization')).toBe(
				`Basic ${Buffer.from('agent-worker@example.test/token:zendesk-worker-test-token').toString(
					'base64',
				)}`,
			);
			return new Response(
				'{"ticket":{"id":9007199254743001,"subject":"Worker customer needs a billing update","status":"pending","requester_id":73,"assignee_id":null,"organization_id":9007199254743003,"priority":"normal"}}',
				{ headers: { 'content-type': 'application/json' } },
			);
		});
		const client = createZendeskClient({
			subdomain: 'worker-support',
			email: 'agent-worker@example.test',
			apiToken: 'zendesk-worker-test-token',
			fetcher,
		});

		const ticket = await client.getTicket(ticketId);

		expect(ticket).toMatchObject({
			id: ticketId,
			subject: 'Worker customer needs a billing update',
			status: 'pending',
			organization_id: '9007199254743003',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		expect(globalThis.process).toBeDefined();
		expect(globalThis.Buffer).toBeDefined();
	});
});
