import { describe, expect, it, vi } from 'vitest';
import {
	createShopifyClient,
	retrieveShopifyOrder,
	SHOPIFY_ADMIN_API_VERSION,
} from '../src/shopify-client.ts';

describe('Shopify Client', () => {
	it('retrieves the bound order through injected Fetch when executed in Node', async () => {
		const shopDomain = 'node-outfitter.myshopify.com';
		const expectedUrl = `https://${shopDomain}/admin/api/${SHOPIFY_ADMIN_API_VERSION}/graphql.json`;
		const fetcher = vi.fn<typeof globalThis.fetch>(async (url, init) => {
			if (String(url) !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${url}`);
			}
			expect(init?.method).toBe('POST');
			const headers = new Headers(init?.headers);
			expect(headers.get('x-shopify-access-token')).toBe('shpat_node_test_token');
			expect(headers.get('content-type')).toBe('application/json');
			expect(JSON.parse(String(init?.body))).toMatchObject({
				variables: { id: 'gid://shopify/Order/7123456701' },
			});
			return Response.json({
				data: {
					order: {
						id: 'gid://shopify/Order/7123456701',
						name: '#1042',
						displayFinancialStatus: 'PAID',
						displayFulfillmentStatus: 'UNFULFILLED',
						email: 'buyer-node@example.test',
						totalPriceSet: {
							shopMoney: {
								amount: '86.40',
								currencyCode: 'USD',
							},
						},
					},
				},
			});
		});
		const client = createShopifyClient({
			shopDomain,
			accessToken: 'shpat_node_test_token',
			fetcher,
		});

		const order = await retrieveShopifyOrder(client, '7123456701');

		expect(order).toMatchObject({
			id: 'gid://shopify/Order/7123456701',
			name: '#1042',
			displayFinancialStatus: 'PAID',
		});
		expect(fetcher).toHaveBeenCalledOnce();
	});
});
