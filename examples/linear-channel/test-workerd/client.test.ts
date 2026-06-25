import { LinearClient } from '@linear/sdk';
import { describe, expect, it, vi } from 'vitest';

describe('LinearClient', () => {
	it('creates comments and agent activities through Fetch in workerd', async () => {
		const fetch = vi
			.fn<typeof globalThis.fetch>()
			.mockResolvedValueOnce(
				Response.json({
					data: {
						commentCreate: {
							success: true,
							lastSyncId: 1,
							comment: {
								id: 'comment-result',
								body: 'Investigating now.',
								createdAt: '2026-06-13T18:30:00.000Z',
								updatedAt: '2026-06-13T18:30:00.000Z',
								url: 'https://linear.app/field/issue/RUN-902#comment-result',
							},
						},
					},
				}),
			)
			.mockResolvedValueOnce(
				Response.json({
					data: {
						agentActivityCreate: {
							success: true,
							lastSyncId: 2,
							agentActivity: {
								id: 'activity-result',
								agentSessionId: 'session-cobalt',
								content: { type: 'response', body: 'The cache header is fixed.' },
								createdAt: '2026-06-13T18:31:00.000Z',
								updatedAt: '2026-06-13T18:31:00.000Z',
								userId: 'app-user-river',
							},
						},
					},
				}),
			);
		vi.stubGlobal('fetch', fetch);

		try {
			const client = new LinearClient({ apiKey: 'lin_api_test' });
			const comment = await client.createComment({
				issueId: 'issue-orchid',
				parentId: 'comment-root-lime',
				body: 'Investigating now.',
			});
			const activity = await client.createAgentActivity({
				agentSessionId: 'session-cobalt',
				content: { type: 'response', body: 'The cache header is fixed.' },
			});

			expect(comment.success).toBe(true);
			expect(comment.commentId).toBe('comment-result');
			expect(activity.success).toBe(true);
			expect(fetch).toHaveBeenCalledTimes(2);
			const [commentUrl, commentInit] = fetch.mock.calls[0] ?? [];
			expect(String(commentUrl)).toBe('https://api.linear.app/graphql');
			expect(commentInit?.method).toBe('POST');
			expect(new Headers(commentInit?.headers).get('authorization')).toBe('lin_api_test');
			expect(JSON.parse(String(commentInit?.body))).toMatchObject({
				variables: {
					input: {
						issueId: 'issue-orchid',
						parentId: 'comment-root-lime',
						body: 'Investigating now.',
					},
				},
			});
			const [, activityInit] = fetch.mock.calls[1] ?? [];
			expect(JSON.parse(String(activityInit?.body))).toMatchObject({
				variables: {
					input: {
						agentSessionId: 'session-cobalt',
						content: { type: 'response', body: 'The cache header is fixed.' },
					},
				},
			});
		} finally {
			vi.unstubAllGlobals();
		}
	});
});
