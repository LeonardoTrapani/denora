import { AsyncLocalStorage } from 'node:async_hooks';
import { format } from 'node:util';
import type { LocalHttpRuntimeOutput } from './local-http-runtime.ts';

type CapturedConsoleMethod = 'log' | 'info' | 'debug' | 'warn' | 'error';

const capturedMethods: readonly CapturedConsoleMethod[] = ['log', 'info', 'debug', 'warn', 'error'];

export async function withScopedConsoleCapture<T>(
	onOutput: ((output: LocalHttpRuntimeOutput) => void) | undefined,
	fn: () => Promise<T>,
): Promise<T> {
	if (!onOutput) return fn();
	const context = new AsyncLocalStorage<boolean>();
	const originals = new Map<CapturedConsoleMethod, (...args: unknown[]) => void>();
	const wrappers = new Map<CapturedConsoleMethod, (...args: unknown[]) => void>();
	let restored = false;
	const restore = () => {
		if (restored) return;
		restored = true;
		for (const method of capturedMethods) {
			const original = originals.get(method);
			const wrapper = wrappers.get(method);
			if (original && wrapper && console[method] === wrapper) console[method] = original;
		}
	};
	for (const method of capturedMethods) {
		const original = console[method] as (...args: unknown[]) => void;
		const wrapper = (...args: unknown[]) => {
			if (!context.getStore()) return original.apply(console, args);
			context.exit(() =>
				onOutput({
					stream: method === 'warn' || method === 'error' ? 'stderr' : 'stdout',
					line: format(...args),
				}),
			);
		};
		originals.set(method, original);
		wrappers.set(method, wrapper);
		console[method] = wrapper;
	}
	try {
		return await context.run(true, fn);
	} finally {
		restore();
	}
}
