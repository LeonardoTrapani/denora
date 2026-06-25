import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

export const collections = {
	docs: defineCollection({
		loader: glob({
			base: './src/content/docs',
			pattern: '**/[^_]*.{markdown,mdown,mkdn,mkd,mdwn,md,mdx}',
		}),
		schema: z.object({
			title: z.string(),
			description: z.string().optional(),
			lastReviewedAt: z.coerce.date().optional(),
			subtitle: z.string().optional(),
			package: z
				.object({
					name: z.string(),
					href: z.url(),
				})
				.optional(),
			tableOfContents: z
				.union([
					z.boolean(),
					z.object({
						minHeadingLevel: z.number().int().min(1).max(6).optional(),
						maxHeadingLevel: z.number().int().min(1).max(6).optional(),
					}),
				])
				.optional(),
		}),
	}),
};
