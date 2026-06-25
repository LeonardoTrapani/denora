import { IntercomClient, IntercomEnvironment } from 'intercom-client';

export type IntercomRegion = 'us' | 'eu' | 'au';

export interface IntercomClientOptions {
	region?: IntercomRegion;
	fetch?: typeof globalThis.fetch;
	maxRetries?: number;
}

export function createIntercomClient(
	token: string,
	options: IntercomClientOptions = {},
): IntercomClient {
	if (!token) throw new TypeError('Intercom access token must be non-empty.');
	return new IntercomClient({
		token,
		version: '2.14',
		environment: environmentForRegion(options.region ?? 'us'),
		...(options.fetch === undefined ? {} : { fetch: options.fetch }),
		...(options.maxRetries === undefined ? {} : { maxRetries: options.maxRetries }),
	});
}

function environmentForRegion(
	region: IntercomRegion,
): (typeof IntercomEnvironment)[keyof typeof IntercomEnvironment] {
	switch (region) {
		case 'us':
			return IntercomEnvironment.UsProduction;
		case 'eu':
			return IntercomEnvironment.EuProduction;
		case 'au':
			return IntercomEnvironment.AuProduction;
	}
}
