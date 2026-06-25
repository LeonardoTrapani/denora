import { createMessengerChannel, type MessengerConversationRef } from '@flue/messenger';
import { defineTool, dispatch } from '@flue/runtime';
import * as v from 'valibot';
import assistant from '../agents/assistant.ts';
import { MessengerClient } from '../messenger-client.ts';

export const client = new MessengerClient({
	pageId: requiredEnv('MESSENGER_PAGE_ID'),
	pageAccessToken: requiredEnv('MESSENGER_PAGE_ACCESS_TOKEN'),
	graphVersion: 'v25.0',
});

export const channel = createMessengerChannel({
	appSecret: requiredEnv('MESSENGER_APP_SECRET'),
	verifyToken: requiredEnv('MESSENGER_VERIFY_TOKEN'),
	pageId: requiredEnv('MESSENGER_PAGE_ID'),

	// Paths: GET and POST /channels/messenger/webhook
	async webhook({ payload }) {
		for (const entry of payload.entry) {
			for (const event of entry.messaging ?? []) {
				// Echoes of the Page's own sends and other non-message events are
				// left to application policy.
				if (event.message === undefined || event.message.is_echo) continue;
				const conversation = channel.conversationRef(event);
				if (conversation === undefined || event.message.text === undefined) {
					continue;
				}
				await dispatch(assistant, {
					id: channel.conversationKey(conversation),
					input: {
						type: 'messenger.message',
						messageId: event.message.mid,
						text: event.message.text,
						attachmentTypes: (event.message.attachments ?? []).map((attachment) => attachment.type),
						quickReplyPayload: event.message.quick_reply?.payload,
					},
				});
			}
		}
	},
});

export function postMessage(ref: MessengerConversationRef) {
	return defineTool({
		name: 'post_messenger_message',
		description: 'Post a message to the Facebook Messenger conversation bound to this agent.',
		input: v.object({ text: v.pipe(v.string(), v.minLength(1)) }),
		async run({ input }) {
			const result = await client.messages.sendText({
				to: ref.participant,
				text: input.text,
			});
			return {
				...(result.messageId === undefined ? {} : { messageId: result.messageId }),
			};
		},
	});
}

function requiredEnv(name: string): string {
	const value = process.env[name];
	if (!value) throw new Error(`${name} is required.`);
	return value;
}
