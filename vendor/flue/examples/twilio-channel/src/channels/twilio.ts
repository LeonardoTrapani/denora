import { defineTool, dispatch } from '@flue/runtime';
import * as v from 'valibot';
import { createTwilioChannel, type TwilioConversationRef } from '@flue/twilio';
import assistant from '../agents/assistant.ts';
import { TwilioClient } from '../twilio-client.ts';

export const client = new TwilioClient({
	accountSid: requiredEnv('TWILIO_ACCOUNT_SID'),
	authToken: requiredEnv('TWILIO_AUTH_TOKEN'),
});

export const channel = createTwilioChannel({
	accountSid: requiredEnv('TWILIO_ACCOUNT_SID'),
	authToken: requiredEnv('TWILIO_AUTH_TOKEN'),
	webhookUrl: requiredEnv('TWILIO_WEBHOOK_URL'),
	destination: {
		type: 'address',
		address: requiredEnv('TWILIO_PHONE_NUMBER'),
	},

	// Path: /channels/twilio/webhook
	async webhook({ payload, conversation }) {
		if (payload.OptOutType === 'STOP') return;
		const numMedia = Number(payload.NumMedia ?? '0');
		await dispatch(assistant, {
			id: channel.conversationKey(conversation),
			input: {
				type: 'twilio.message',
				messageSid: payload.MessageSid,
				from: payload.From,
				text: payload.Body,
				media: Array.from({ length: numMedia }, (_, index) => ({
					index,
					contentType: payload[`MediaContentType${index}`],
				})),
			},
		});
	},
});

export function postMessage(ref: TwilioConversationRef) {
	return defineTool({
		name: 'post_twilio_message',
		description: 'Post a message to the Twilio conversation bound to this agent.',
		input: v.object({ text: v.pipe(v.string(), v.minLength(1)) }),
		async run({ input }) {
			const result = await client.messages.create({
				to: ref.participant,
				body: input.text,
				...(ref.type === 'messaging-service'
					? { messagingServiceSid: ref.messagingServiceSid }
					: { from: ref.address }),
			});
			return { messageSid: result.sid };
		},
	});
}

function requiredEnv(name: string): string {
	const value = process.env[name];
	if (!value) throw new Error(`${name} is required.`);
	return value;
}
