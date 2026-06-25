import { cloneJsonSerializable } from '../json-snapshot.ts';
import type { DispatchReceipt, NamedAgentDispatchRequest } from '../types.ts';
import type { DispatchQueue } from './dispatch-queue.ts';

export interface DispatchRuntime {
	agents: ReadonlyArray<{ name: string }>;
}

export async function enqueueDispatch(options: {
	request: NamedAgentDispatchRequest;
	dispatchQueue: DispatchQueue;
	rt: DispatchRuntime;
}): Promise<DispatchReceipt> {
	const agent = options.request.agent;
	const input = validateAndCloneDispatchRequest(options.request, agent, options.rt);
	return options.dispatchQueue.enqueue({
		dispatchId: crypto.randomUUID(),
		agent,
		id: options.request.id,
		input,
		acceptedAt: new Date().toISOString(),
	});
}

function validateAndCloneDispatchRequest(
	request: NamedAgentDispatchRequest,
	agent: string,
	rt: DispatchRuntime,
): unknown {
	if (typeof agent !== 'string' || agent.trim() === '') {
		throw new Error('[flue] dispatch() requires a non-empty target agent.');
	}
	if (typeof request.id !== 'string' || request.id.trim() === '') {
		throw new Error('[flue] dispatch() requires a non-empty "id" target agent instance id.');
	}
	if (request.input === undefined) {
		throw new Error(
			'[flue] dispatch() requires an "input" payload. Use null for an intentional empty payload.',
		);
	}
	if (!agentExists(rt, agent)) {
		throw new Error(`[flue] dispatch() target agent "${agent}" is not registered.`);
	}
	return cloneJsonSerializable(request.input, 'dispatch().input');
}

function agentExists(rt: DispatchRuntime, agentName: string): boolean {
	return rt.agents.some((agent) => agent.name === agentName);
}
