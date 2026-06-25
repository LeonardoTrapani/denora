import Stripe from 'stripe';
import { describe, expect, it, vi } from 'vitest';
import { createStripeClient } from '../src/stripe-client.ts';

describe('Stripe', () => {
	it('retrieves the bound customer through Fetch in workerd', async () => {
		const fetcher = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
			Response.json({
				id: 'cus_worker_5252',
				object: 'customer',
				email: 'worker@example.test',
				name: 'Worker Customer',
				delinquent: false,
			}),
		);
		const client = createStripeClient('sk_test_worker_only', fetcher);

		const customer = await client.customers.retrieve(
			'cus_worker_5252',
			{},
			{ stripeContext: 'ctx_worker_6161' },
		);

		expect(customer).toMatchObject({
			id: 'cus_worker_5252',
			email: 'worker@example.test',
			name: 'Worker Customer',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		const [url, init] = fetcher.mock.calls[0] ?? [];
		expect(String(url)).toBe('https://api.stripe.com/v1/customers/cus_worker_5252');
		expect(init?.method).toBe('GET');
		const headers = new Headers(init?.headers);
		expect(headers.get('authorization')).toBe('Bearer sk_test_worker_only');
		expect(headers.get('stripe-context')).toBe('ctx_worker_6161');
		expect(() => Stripe.createNodeHttpClient()).toThrow('not available in non-Node environments');
	});
});
