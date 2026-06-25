import mdx from '@astrojs/mdx';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'astro/config';

export default defineConfig({
	site: 'https://flueframework.com',
	base: '/docs',
	trailingSlash: 'always',
	outDir: './dist/docs',
	output: 'static',
	integrations: [mdx()],
	markdown: {
		shikiConfig: {
			theme: 'github-dark',
		},
	},
	vite: {
		plugins: tailwindcss(),
	},
});
