import { describe, expect, it, vi } from 'vitest';

describe('conversationFromPayload()', () => {
	it('derives conversation metadata from spaceType when deprecated type differs', async () => {
		vi.stubEnv('GOOGLE_CHAT_APP_URL', 'https://example.test/channels/google-chat/interactions');
		vi.stubEnv('GOOGLE_CHAT_CLIENT_EMAIL', 'chat@example.test');
		vi.stubEnv('GOOGLE_CHAT_PRIVATE_KEY', 'unused-in-this-test');
		const { conversationFromPayload } = await import('../src/channels/google-chat.ts');

		const ref = conversationFromPayload({
			space: {
				name: 'spaces/current-field',
				spaceType: 'DIRECT_MESSAGE',
				type: 'ROOM',
			},
			thread: { name: 'spaces/current-field/threads/thread-1' },
		});

		expect(ref).toEqual({
			space: 'spaces/current-field',
			thread: 'spaces/current-field/threads/thread-1',
			spaceType: 'DIRECT_MESSAGE',
		});
	});
});
