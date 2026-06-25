import { describe, expect, it } from 'vitest';
import { RuntimeUnavailableError } from '../src/errors.ts';
import { createRuntimeActivityGate } from '../src/runtime/runtime-activity-gate.ts';

describe('RuntimeActivityGate', () => {
	it('rejects every lease acquired after admissions pause', () => {
		const gate = createRuntimeActivityGate();

		gate.pause();

		expect(() => gate.enter()).toThrow(RuntimeUnavailableError);
	});

	it('waits for leases acquired before admissions pause', async () => {
		const gate = createRuntimeActivityGate();
		const lease = gate.enter();
		gate.pause();
		let idle = false;
		const waiting = gate.waitForIdle().then(() => {
			idle = true;
		});

		await Promise.resolve();
		expect(idle).toBe(false);
		lease.release();
		await waiting;
		expect(idle).toBe(true);
	});
});
