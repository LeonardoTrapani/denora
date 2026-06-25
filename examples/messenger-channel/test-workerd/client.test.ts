import { describe, expect, it, vi } from 'vitest';
import { MessengerClient } from '../src/messenger-client.ts';

describe('MessengerClient', () => {
	it('sends to a user reference and performs sender actions in workerd', async () => {
		const fetch = vi
			.fn<typeof globalThis.fetch>()
			.mockResolvedValueOnce(
				Response.json({
					recipient_id: 'user_ref_worker_91',
					message_id: 'm_worker_92',
				}),
			)
			.mockResolvedValueOnce(
				Response.json({
					recipient_id: 'user_ref_worker_91',
				}),
			);
		const client = new MessengerClient({
			pageId: 'page_worker_90',
			pageAccessToken: 'page-token-worker',
			graphVersion: 'v25.0',
			apiBaseUrl: 'https://graph.example.test',
			fetch,
		});
		const to = {
			type: 'user-ref' as const,
			id: 'user_ref_worker_91',
		};

		const result = await client.messages.send({
			to,
			messagingType: 'UPDATE',
			message: {
				attachment: {
					type: 'template',
					payload: {
						template_type: 'button',
						text: 'Choose a worker action',
					},
				},
			},
		});
		const action = await client.senderActions.send(to, {
			type: 'react',
			messageId: 'm_worker_parent_93',
			reaction: '🎉',
		});

		expect(result.messageId).toBe('m_worker_92');
		expect(action.recipientId).toBe('user_ref_worker_91');
		expect(fetch).toHaveBeenCalledTimes(2);
		expect(JSON.parse(String(fetch.mock.calls[0]?.[1]?.body))).toEqual({
			recipient: { user_ref: 'user_ref_worker_91' },
			messaging_type: 'UPDATE',
			message: {
				attachment: {
					type: 'template',
					payload: {
						template_type: 'button',
						text: 'Choose a worker action',
					},
				},
			},
		});
		expect(JSON.parse(String(fetch.mock.calls[1]?.[1]?.body))).toEqual({
			recipient: { user_ref: 'user_ref_worker_91' },
			sender_action: 'react',
			payload: {
				message_id: 'm_worker_parent_93',
				reaction: '🎉',
			},
		});
	});
});
