import { decodeJwt, exportPKCS8, generateKeyPair, jwtVerify } from 'jose';
import { describe, expect, it, vi } from 'vitest';
import { createGoogleChatClient } from '../src/lib/google-chat-client.ts';

describe('createGoogleChatClient()', () => {
	it('signs a service-account assertion and posts a threaded message in workerd', async () => {
		const keyPair = await generateKeyPair('RS256', { extractable: true });
		const privateKey = await exportPKCS8(keyPair.privateKey);
		const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			if (url === 'https://oauth.example.test/token') {
				const body = new URLSearchParams(String(init?.body));
				const assertion = body.get('assertion');
				expect(body.get('grant_type')).toBe('urn:ietf:params:oauth:grant-type:jwt-bearer');
				expect(assertion).toBeTruthy();
				expect(decodeJwt(assertion ?? '')).toMatchObject({
					iss: 'assistant@synthetic-project.iam.gserviceaccount.com',
					aud: 'https://oauth.example.test/token',
					scope: 'https://www.googleapis.com/auth/chat.bot',
				});
				await expect(
					jwtVerify(assertion ?? '', keyPair.publicKey, {
						algorithms: ['RS256'],
						issuer: 'assistant@synthetic-project.iam.gserviceaccount.com',
						audience: 'https://oauth.example.test/token',
					}),
				).resolves.toBeDefined();
				return Response.json({ access_token: 'local-google-token', expires_in: 3600 });
			}
			expect(url).toBe(
				'https://chat.example.test/v1/spaces/room-17/messages?messageReplyOption=REPLY_MESSAGE_OR_FAIL',
			);
			expect(init?.headers).toEqual({
				authorization: 'Bearer local-google-token',
				'content-type': 'application/json',
			});
			expect(JSON.parse(String(init?.body))).toEqual({
				text: 'A local workerd message',
				thread: { name: 'spaces/room-17/threads/thread-31' },
			});
			return Response.json({
				name: 'spaces/room-17/messages/message-44',
				thread: { name: 'spaces/room-17/threads/thread-31' },
			});
		});
		const client = createGoogleChatClient({
			clientEmail: 'assistant@synthetic-project.iam.gserviceaccount.com',
			privateKey,
			tokenUri: 'https://oauth.example.test/token',
			apiBaseUrl: 'https://chat.example.test',
			fetch: fetcher,
		});

		const result = await client.postMessage(
			{
				space: 'spaces/room-17',
				thread: 'spaces/room-17/threads/thread-31',
				spaceType: 'SPACE',
			},
			'A local workerd message',
		);

		expect(result).toEqual({
			name: 'spaces/room-17/messages/message-44',
			thread: { name: 'spaces/room-17/threads/thread-31' },
		});
		expect(fetcher).toHaveBeenCalledTimes(2);
	});
});
