import * as fs from 'node:fs';
import * as path from 'node:path';

export function resolveSourceRoot(root: string): string {
	for (const sourceDirectory of ['.flue', 'src']) {
		const candidate = path.join(root, sourceDirectory);
		try {
			if (fs.statSync(candidate).isDirectory()) return candidate;
		} catch {}
	}
	return root;
}
