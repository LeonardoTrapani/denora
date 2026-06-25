import { decodeJwt, exportPKCS8, generateKeyPair, jwtVerify } from 'jose';
import { describe, expect, it, vi } from 'vitest';
import { createGoogleChatClient } from '../src/lib/google-chat-client.ts';

describe('createGoogleChatClient()', () => {
	it('signs an assertion and posts a threaded message when executed in Node', async () => {
		const keyPair = await generateKeyPair('RS256', { extractable: true });
		const privateKey = await exportPKCS8(keyPair.privateKey);
		const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			if (url === 'https://oauth.example.test/token') {
				const body = new URLSearchParams(String(init?.body));
				const assertion = body.get('assertion') ?? '';
				expect(body.get('grant_type')).toBe('urn:ietf:params:oauth:grant-type:jwt-bearer');
				expect(decodeJwt(assertion)).toMatchObject({
					iss: 'assistant@synthetic-project.iam.gserviceaccount.com',
					aud: 'https://oauth.example.test/token',
					scope: 'https://www.googleapis.com/auth/chat.bot',
				});
				await expect(
					jwtVerify(assertion, keyPair.publicKey, {
						algorithms: ['RS256'],
						issuer: 'assistant@synthetic-project.iam.gserviceaccount.com',
						audience: 'https://oauth.example.test/token',
					}),
				).resolves.toBeDefined();
				return Response.json({ access_token: 'local-google-token', expires_in: 3600 });
			}
			if (
				url !==
				'https://chat.example.test/v1/spaces/room-17/messages?messageReplyOption=REPLY_MESSAGE_OR_FAIL'
			) {
				throw new Error(`Unexpected network request: ${url}`);
			}
			expect(new Headers(init?.headers).get('authorization')).toBe('Bearer local-google-token');
			expect(JSON.parse(String(init?.body))).toEqual({
				text: 'A local Node message',
				thread: { name: 'spaces/room-17/threads/thread-31' },
			});
			return Response.json({ name: 'spaces/room-17/messages/message-44' });
		});
		const client = createGoogleChatClient({
			clientEmail: 'assistant@synthetic-project.iam.gserviceaccount.com',
			privateKey,
			tokenUri: 'https://oauth.example.test/token',
			apiBaseUrl: 'https://chat.example.test',
			fetch: fetcher,
		});

		await expect(
			client.postMessage(
				{ space: 'spaces/room-17', thread: 'spaces/room-17/threads/thread-31' },
				'A local Node message',
			),
		).resolves.toEqual({ name: 'spaces/room-17/messages/message-44' });
		expect(fetcher).toHaveBeenCalledTimes(2);
	});

	it('rejects a thread from another space before making a live call', async () => {
		const keyPair = await generateKeyPair('RS256', { extractable: true });
		const fetcher = vi.fn();
		const client = createGoogleChatClient({
			clientEmail: 'assistant@synthetic-project.iam.gserviceaccount.com',
			privateKey: await exportPKCS8(keyPair.privateKey),
			fetch: fetcher,
		});

		await expect(
			client.postMessage(
				{ space: 'spaces/room-17', thread: 'spaces/other-room/threads/thread-31' },
				'Wrong destination',
			),
		).rejects.toThrow('thread must belong to its space');
		expect(fetcher).not.toHaveBeenCalled();
	});
});
