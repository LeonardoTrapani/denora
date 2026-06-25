export default {
	resolve: {
		alias: {
			'@flue/sdk': new URL('../sdk/src/index.ts', import.meta.url).pathname,
		},
	},
	test: {
		environment: 'happy-dom',
	},
};
