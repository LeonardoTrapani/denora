import { Resend } from 'resend';

export function createResendClient(apiKey: string, options: { baseUrl?: string } = {}): Resend {
	return new Resend(apiKey, options);
}
