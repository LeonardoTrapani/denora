import { Client } from '@notionhq/client';

type NotionFetch = NonNullable<NonNullable<ConstructorParameters<typeof Client>[0]>['fetch']>;

export function createNotionClient(
	token: string,
	fetcher: typeof globalThis.fetch = globalThis.fetch,
): Client {
	const notionFetch: NotionFetch = (url, init) =>
		fetcher(url, {
			method: init?.method,
			headers: init?.headers,
			body: init?.body,
		});
	return new Client({ auth: token, fetch: notionFetch });
}
