import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['test/cloudflare-deployment-extension.integration.test.ts'],
	},
});
