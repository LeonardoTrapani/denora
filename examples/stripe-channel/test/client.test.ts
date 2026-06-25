import { describe, expect, it, vi } from 'vitest';
import { createStripeClient, stripeRequestOptions } from '../src/stripe-client.ts';

describe('Stripe', () => {
	it('retrieves the bound customer through Fetch in Node', async () => {
		const fetcher = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
			Response.json({
				id: 'cus_node_4242',
				object: 'customer',
				email: 'node@example.test',
				name: 'Node Customer',
				delinquent: false,
			}),
		);
		const client = createStripeClient('sk_test_node_only', fetcher);

		const customer = await client.customers.retrieve(
			'cus_node_4242',
			{},
			{ stripeAccount: 'acct_node_3131' },
		);

		expect(customer).toMatchObject({
			id: 'cus_node_4242',
			email: 'node@example.test',
			name: 'Node Customer',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		const [url, init] = fetcher.mock.calls[0] ?? [];
		expect(String(url)).toBe('https://api.stripe.com/v1/customers/cus_node_4242');
		expect(init?.method).toBe('GET');
		const headers = new Headers(init?.headers);
		expect(headers.get('authorization')).toBe('Bearer sk_test_node_only');
		expect(headers.get('stripe-account')).toBe('acct_node_3131');
	});

	it('prefers Stripe context when both verified scoping values are present', () => {
		expect(stripeRequestOptions('acct_node_3131', 'acct_node_3131/store_west')).toEqual({
			stripeContext: 'acct_node_3131/store_west',
		});
	});
});
