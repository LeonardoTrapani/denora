import { defineConfig } from 'vite';

export default defineConfig({
	root: 'src/ui',
	build: {
		outDir: '../../dist/client',
		emptyOutDir: true,
	},
});
