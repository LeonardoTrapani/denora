import { describe, expect, it, vi } from 'vitest';
import { createIntercomClient } from '../src/intercom-client.ts';

describe('Intercom Client', () => {
	it('retrieves the bound conversation in workerd with nodejs_compat', async () => {
		const expectedUrl =
			'https://api.eu.intercom.io/conversations/conversation-worker-29?display_as=plaintext';
		const fetcher = vi.fn<typeof globalThis.fetch>(async (input, init) => {
			const request = new Request(input, init);
			if (request.url !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${request.url}`);
			}
			expect(request.method).toBe('GET');
			expect(request.headers.get('authorization')).toBe('Bearer intercom-worker-test-token');
			expect(request.headers.get('intercom-version')).toBe('2.14');
			expect(request.headers.get('x-fern-runtime')).toBe('workerd');
			return Response.json({
				type: 'conversation',
				id: 'conversation-worker-29',
				title: 'Worker support request',
				created_at: 1781394100,
				updated_at: 1781394110,
				waiting_since: null,
				snoozed_until: null,
				open: true,
				state: 'open',
				read: false,
				priority: false,
				admin_assignee_id: null,
				team_assignee_id: null,
				tags: { type: 'tag.list', tags: [] },
				contacts: { type: 'contact.list', contacts: [] },
				conversation_parts: {
					type: 'conversation_part.list',
					conversation_parts: [],
					total_count: 0,
				},
			});
		});
		const client = createIntercomClient('intercom-worker-test-token', {
			region: 'eu',
			fetch: fetcher,
			maxRetries: 0,
		});

		const conversation = await client.conversations.find({
			conversation_id: 'conversation-worker-29',
			display_as: 'plaintext',
		});

		expect(conversation).toMatchObject({
			id: 'conversation-worker-29',
			title: 'Worker support request',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		expect(globalThis.process).toBeDefined();
		expect(globalThis.Buffer).toBeDefined();
	});
});
