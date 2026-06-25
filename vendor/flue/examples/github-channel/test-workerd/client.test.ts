import { Octokit } from '@octokit/rest';
import { describe, expect, it, vi } from 'vitest';

describe('Octokit', () => {
	it('creates an issue comment when executed in workerd', async () => {
		const fetch = vi.fn(async () =>
			Response.json({
				id: 4401,
				html_url: 'https://github.example/acme/widgets/issues/42#issuecomment-4401',
			}),
		);
		vi.stubGlobal('fetch', fetch);

		try {
			const client = new Octokit({ auth: 'github-test-token' });
			const result = await client.rest.issues.createComment({
				owner: 'acme',
				repo: 'widgets',
				issue_number: 42,
				body: 'Checked from a Worker.',
			});

			expect(result.data.id).toBe(4401);
			expect(fetch).toHaveBeenCalledOnce();
			const [url, init] = fetch.mock.calls[0] ?? [];
			expect(String(url)).toBe('https://api.github.com/repos/acme/widgets/issues/42/comments');
			expect(init?.method).toBe('POST');
			expect(new Headers(init?.headers).get('authorization')).toBe('token github-test-token');
			expect(JSON.parse(String(init?.body))).toEqual({ body: 'Checked from a Worker.' });
		} finally {
			vi.unstubAllGlobals();
		}
	});
});
