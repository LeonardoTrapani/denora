import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	createShopifyClient,
	retrieveShopifyOrder,
	SHOPIFY_ADMIN_API_VERSION,
} from '../src/shopify-client.ts';

describe('Shopify Client', () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it('retrieves the bound order through injected Fetch when executed in workerd', async () => {
		const shopDomain = 'worker-stationer.myshopify.com';
		const expectedUrl = `https://${shopDomain}/admin/api/${SHOPIFY_ADMIN_API_VERSION}/graphql.json`;
		const fetcher = vi.fn<typeof globalThis.fetch>(async (url, init) => {
			if (String(url) !== expectedUrl) {
				throw new Error(`Unexpected network destination: ${url}`);
			}
			expect(init?.method).toBe('POST');
			const headers = new Headers(init?.headers);
			expect(headers.get('x-shopify-access-token')).toBe('shpat_worker_test_token');
			expect(headers.get('content-type')).toBe('application/json');
			expect(JSON.parse(String(init?.body))).toMatchObject({
				variables: { id: 'gid://shopify/Order/9007199254741999' },
			});
			return Response.json({
				data: {
					order: {
						id: 'gid://shopify/Order/9007199254741999',
						name: '#2088',
						displayFinancialStatus: 'AUTHORIZED',
						displayFulfillmentStatus: 'UNFULFILLED',
						email: 'buyer-worker@example.test',
						totalPriceSet: {
							shopMoney: {
								amount: '41.75',
								currencyCode: 'CAD',
							},
						},
					},
				},
			});
		});
		const client = createShopifyClient({
			shopDomain,
			accessToken: 'shpat_worker_test_token',
			fetcher,
		});

		const order = await retrieveShopifyOrder(client, '9007199254741999');

		expect(order).toMatchObject({
			id: 'gid://shopify/Order/9007199254741999',
			name: '#2088',
			displayFinancialStatus: 'AUTHORIZED',
		});
		expect(fetcher).toHaveBeenCalledOnce();
		expect(globalThis.process).toBeDefined();
		expect(globalThis.Buffer).toBeDefined();
	});
});
