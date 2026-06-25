import { isLosslessNumber, isSafeNumber, parse } from 'lossless-json';

export interface ZendeskClientOptions {
	subdomain: string;
	email: string;
	apiToken: string;
	fetcher?: typeof globalThis.fetch;
}

interface ZendeskTicket {
	id: string | number;
	subject: string | null;
	status: string;
	requester_id: string | number;
	assignee_id: string | number | null;
	organization_id: string | number | null;
	[key: string]: unknown;
}

export interface ZendeskClient {
	getTicket(ticketId: string): Promise<ZendeskTicket>;
}

export function createZendeskClient({
	subdomain,
	email,
	apiToken,
	fetcher = globalThis.fetch,
}: ZendeskClientOptions): ZendeskClient {
	validateSubdomain(subdomain);
	if (!email) throw new TypeError('Zendesk email must be non-empty.');
	if (!apiToken) throw new TypeError('Zendesk API token must be non-empty.');
	if (typeof fetcher !== 'function') throw new TypeError('Zendesk Fetch must be callable.');

	const baseUrl = `https://${subdomain}.zendesk.com/api/v2`;
	const authorization = `Basic ${Buffer.from(`${email}/token:${apiToken}`, 'utf8').toString(
		'base64',
	)}`;

	return {
		async getTicket(ticketId) {
			if (!/^[1-9]\d*$/.test(ticketId)) {
				throw new TypeError('Zendesk ticket id must be a positive decimal string.');
			}

			const response = await fetcher(`${baseUrl}/tickets/${ticketId}.json`, {
				method: 'GET',
				headers: {
					accept: 'application/json',
					authorization,
				},
			});

			if (!response.ok) {
				const details = (await response.text()).trim();
				throw new Error(
					`Zendesk request failed with status ${response.status}${details ? `: ${details}` : '.'}`,
				);
			}

			let body: unknown;
			try {
				body = normalizeJsonValue(parse(await response.text()));
			} catch {
				throw new TypeError('Zendesk returned invalid JSON.');
			}
			if (!isRecord(body) || !isTicket(body.ticket)) {
				throw new TypeError('Zendesk returned an invalid ticket response.');
			}
			return body.ticket;
		},
	};
}

function validateSubdomain(subdomain: string): void {
	if (
		!subdomain ||
		subdomain.length > 63 ||
		!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/i.test(subdomain)
	) {
		throw new TypeError('Zendesk subdomain must be a bare DNS label.');
	}
}

function isTicket(value: unknown): value is ZendeskTicket {
	if (!isRecord(value)) return false;
	if (!isZendeskId(value.id)) return false;
	if (!(typeof value.subject === 'string' || value.subject === null)) return false;
	if (typeof value.status !== 'string') return false;
	if (!isZendeskId(value.requester_id)) return false;
	if (!(value.assignee_id === null || isZendeskId(value.assignee_id))) return false;
	return value.organization_id === null || isZendeskId(value.organization_id);
}

function isZendeskId(value: unknown): value is string | number {
	if (typeof value === 'string') return /^[1-9]\d*$/.test(value);
	return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

function normalizeJsonValue(value: unknown): JsonValue | undefined {
	if (
		value === null ||
		typeof value === 'boolean' ||
		typeof value === 'string' ||
		(typeof value === 'number' && Number.isFinite(value))
	) {
		return value;
	}
	if (isLosslessNumber(value)) {
		return isSafeNumber(value.value) ? Number(value.value) : value.value;
	}
	if (Array.isArray(value)) {
		const result: JsonValue[] = [];
		for (const item of value) {
			const normalized = normalizeJsonValue(item);
			if (normalized === undefined) return undefined;
			result.push(normalized);
		}
		return result;
	}
	if (!isRecord(value)) return undefined;
	const result: { [key: string]: JsonValue } = {};
	for (const [key, item] of Object.entries(value)) {
		const normalized = normalizeJsonValue(item);
		if (normalized === undefined) return undefined;
		result[key] = normalized;
	}
	return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return (
		typeof value === 'object' &&
		value !== null &&
		!Array.isArray(value) &&
		!isLosslessNumber(value) &&
		Object.getPrototypeOf(value) === Object.prototype
	);
}
