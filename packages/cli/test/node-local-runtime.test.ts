import { afterEach, describe, expect, it } from 'vitest';
import type { NodeApplicationLoader } from '../src/lib/node-application-loader.ts';
import type { LoadedNodeApplication } from '../src/lib/node-http-listener.ts';
import { createNodeLocalRuntime, type NodeLocalRuntime } from '../src/lib/node-local-runtime.ts';

const runtimes: NodeLocalRuntime[] = [];

afterEach(async () => {
	await Promise.allSettled(runtimes.splice(0).map((runtime) => runtime.stop()));
});

describe('NodeLocalRuntime', () => {
	it('waits for active work before loading the replacement application', async () => {
		let releaseIdle!: () => void;
		const idle = new Promise<void>((resolve) => {
			releaseIdle = resolve;
		});
		const events: string[] = [];
		const first = application({
			pauseAdmissions: () => events.push('paused'),
			waitForIdle: () => idle,
			stop: async () => {
				events.push('stopped-first');
			},
		});
		const second = application();
		const loader = sequenceLoader([first, second], events);
		const runtime = await createNodeLocalRuntime({
			root: '/fixture',
			sourceRoot: '/fixture',
			port: 0,
			temporaryLocalExposure: false,
			createLoader: async () => loader,
		});
		runtimes.push(runtime);
		await runtime.start();
		events.length = 0;

		const reload = runtime.reload();
		await viWaitFor(() => expect(events).toEqual(['paused']));
		releaseIdle();
		await reload;

		expect(events).toEqual(['paused', 'stopped-first', 'loaded-2']);
	});

	it('does not install an application loaded after shutdown begins', async () => {
		let releaseLoad!: () => void;
		let markLoadStarted!: () => void;
		const loadStarted = new Promise<void>((resolve) => {
			markLoadStarted = resolve;
		});
		const loadGate = new Promise<void>((resolve) => {
			releaseLoad = resolve;
		});
		let stoppedLoadedApplication = 0;
		const loader: NodeApplicationLoader = {
			async load() {
				markLoadStarted();
				await loadGate;
				return application({
					stop: async () => {
						stoppedLoadedApplication += 1;
					},
				});
			},
			async close() {},
		};
		const runtime = await createNodeLocalRuntime({
			root: '/fixture',
			sourceRoot: '/fixture',
			port: 0,
			temporaryLocalExposure: false,
			createLoader: async () => loader,
		});
		runtimes.push(runtime);
		const startup = runtime.start();
		await loadStarted;
		const stopping = runtime.stop();

		releaseLoad();
		await Promise.all([startup, stopping]);

		expect(stoppedLoadedApplication).toBe(1);
		await expect(fetch(runtime.url)).rejects.toThrow();
	});
});

function application(
	overrides: Partial<LoadedNodeApplication> = {},
): LoadedNodeApplication {
	return {
		fetch: () => new Response('ok'),
		enterActivity: () => ({ release() {} }),
		pauseAdmissions() {},
		waitForIdle: async () => undefined,
		stop: async () => undefined,
		closeSync() {},
		...overrides,
	};
}

function sequenceLoader(
	applications: LoadedNodeApplication[],
	events: string[],
): NodeApplicationLoader {
	let index = 0;
	return {
		async load() {
			const next = applications[index];
			if (!next) throw new Error('No application fixture remains.');
			index += 1;
			events.push(`loaded-${index}`);
			return next;
		},
		async close() {},
	};
}

async function viWaitFor(assertion: () => void): Promise<void> {
	const deadline = Date.now() + 1_000;
	while (true) {
		try {
			assertion();
			return;
		} catch (error) {
			if (Date.now() >= deadline) throw error;
			await Promise.resolve();
		}
	}
}
