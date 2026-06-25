import { describe, expect, it, vi } from 'vitest';
import { TwilioClient } from '../src/twilio-client.ts';

describe('TwilioClient', () => {
	it('sends through a Messaging Service with Fetch in workerd', async () => {
		const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
			Response.json({
				sid: 'SM50505050505050505050505050505050',
				status: 'accepted',
			}),
		);
		const client = new TwilioClient({
			accountSid: 'AC60606060606060606060606060606060',
			authToken: 'worker-auth-token',
			apiBaseUrl: 'https://api.twilio.test',
			fetch,
		});

		const result = await client.messages.create({
			to: 'whatsapp:+15557018018',
			messagingServiceSid: 'MG70707070707070707070707070707070',
			body: 'Worker response',
		});

		expect(result.sid).toBe('SM50505050505050505050505050505050');
		expect(fetch).toHaveBeenCalledOnce();
		expect(String(fetch.mock.calls[0]?.[0])).toBe(
			'https://api.twilio.test/2010-04-01/Accounts/AC60606060606060606060606060606060/Messages.json',
		);
		const body = new URLSearchParams(String(fetch.mock.calls[0]?.[1]?.body));
		expect(Object.fromEntries(body)).toEqual({
			To: 'whatsapp:+15557018018',
			MessagingServiceSid: 'MG70707070707070707070707070707070',
			Body: 'Worker response',
		});
	});
});
