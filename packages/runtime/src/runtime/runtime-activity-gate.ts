import { RuntimeUnavailableError } from '../errors.ts';

export interface RuntimeActivityLease {
	release(): void;
}

export interface RuntimeActivityGate {
	enter(): RuntimeActivityLease;
	pause(): void;
	waitForIdle(): Promise<void>;
}

export function createRuntimeActivityGate(): RuntimeActivityGate {
	let isPaused = false;
	let active = 0;
	let idleWaiters: Array<() => void> = [];

	function releaseIdleWaiters(): void {
		if (active !== 0) return;
		const waiters = idleWaiters;
		idleWaiters = [];
		for (const resolve of waiters) resolve();
	}

	return {
		enter() {
			if (isPaused) throw new RuntimeUnavailableError({ state: 'draining' });
			active += 1;
			let released = false;
			return {
				release() {
					if (released) return;
					released = true;
					active -= 1;
					releaseIdleWaiters();
				},
			};
		},
		pause() {
			isPaused = true;
		},
		waitForIdle() {
			if (active === 0) return Promise.resolve();
			return new Promise<void>((resolve) => idleWaiters.push(resolve));
		},
	};
}
