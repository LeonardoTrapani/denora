import type { AgentSubmission } from '../agent-execution-store.ts';
import { observe } from './events.ts';

export interface AgentInteractionStart {
	agentName: string;
	instanceId: string;
	kind: AgentSubmission['kind'];
	submissionId: string;
	dispatchId?: string;
}

export function installDevLifecycleLogger(
	write: (message: string) => void = console.log,
): {
	onAgentInteractionStart(interaction: AgentInteractionStart): void;
	dispose(): void;
} {
	const workflowNames = new Map<string, string>();
	const dispose = observe((event) => {
		if (event.type === 'run_start' || event.type === 'run_resume') {
			workflowNames.set(event.runId, event.workflowName);
			write(
				`[workflow] ${event.workflowName}@${event.runId} ${event.type === 'run_start' ? 'started' : 'resumed'}`,
			);
			return;
		}
		if (event.type !== 'run_end') return;
		const workflowName = workflowNames.get(event.runId);
		workflowNames.delete(event.runId);
		const subject = workflowName ? `${workflowName}@${event.runId}` : event.runId;
		write(
			event.isError
				? `[workflow] ${subject} failed in ${event.durationMs}ms`
				: `[workflow] ${subject} completed in ${event.durationMs}ms`,
		);
	});

	return {
		onAgentInteractionStart(interaction) {
			write(`[agent] ${interaction.agentName}@${interaction.instanceId} started`);
		},
		dispose,
	};
}
