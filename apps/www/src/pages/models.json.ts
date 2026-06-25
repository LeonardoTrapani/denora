import type { APIRoute } from 'astro';

const modelsUrl = 'https://unpkg.com/@earendil-works/pi-ai/dist/models.generated.js';

type ModelRegistry = Record<string, Record<string, unknown>>;

async function extractModelSpecifiers(source: string) {
	const moduleUrl = `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`;
	const { MODELS } = (await import(moduleUrl)) as { MODELS?: ModelRegistry };

	if (!MODELS) {
		throw new Error(`No MODELS export found in ${modelsUrl}`);
	}

	const specifiers: string[] = [];

	for (const [providerId, models] of Object.entries(MODELS)) {
		for (const modelId of Object.keys(models)) {
			specifiers.push(`${providerId}/${modelId}`);
		}
	}

	return specifiers;
}

export const GET: APIRoute = async () => {
	const response = await fetch(modelsUrl);

	if (!response.ok) {
		throw new Error(
			`Failed to fetch model list from ${modelsUrl}: ${response.status} ${response.statusText}`,
		);
	}

	const modelSpecifiers = await extractModelSpecifiers(await response.text());

	if (modelSpecifiers.length === 0) {
		throw new Error(`No model specifiers found in ${modelsUrl}`);
	}

	return new Response(JSON.stringify(modelSpecifiers, null, 2), {
		headers: {
			'Content-Type': 'application/json; charset=utf-8',
		},
	});
};
