import { describe, expect, it, vi } from 'vitest';
import { createNotionClient } from '../src/notion-client.ts';

describe('Notion Client', () => {
	it('retrieves the bound page through injected Fetch when executed in workerd', async () => {
		const expectedUrl = 'https://api.notion.com/v1/pages/page-worker-29';
		const fetcher = vi.fn<typeof globalThis.fetch>(async (url, init) => {
			if (String(url) !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${url}`);
			}
			expect(init?.method).toBe('GET');
			const headers = new Headers(init?.headers);
			expect(headers.get('authorization')).toBe('Bearer notion-worker-test-token');
			expect(headers.get('notion-version')).toBe('2025-09-03');
			return Response.json({
				object: 'page',
				id: 'page-worker-29',
				created_time: '2026-06-13T19:00:00.000Z',
				last_edited_time: '2026-06-13T19:05:00.000Z',
				created_by: { object: 'user', id: 'user-worker-1' },
				last_edited_by: { object: 'user', id: 'user-worker-1' },
				cover: null,
				icon: null,
				parent: { type: 'workspace', workspace: true },
				archived: false,
				in_trash: false,
				is_locked: false,
				properties: {},
				url: 'https://www.notion.so/page-worker-29',
				public_url: null,
				request_id: 'request-worker-1',
			});
		});
		const client = createNotionClient('notion-worker-test-token', fetcher);

		const page = await client.pages.retrieve({ page_id: 'page-worker-29' });

		expect(page).toMatchObject({ object: 'page', id: 'page-worker-29' });
		expect(fetcher).toHaveBeenCalledOnce();
	});
});
