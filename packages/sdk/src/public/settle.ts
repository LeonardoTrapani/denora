import type { BackoffOptions } from '@durable-streams/client';
import type { HttpClient } from '../http.ts';
import type { AgentSendResult } from './invoke.ts';
import { createFlueEventStream } from './stream.ts';
import type { AttachedAgentEvent, FlueEvent, RunRecord } from '../types.ts';

export interface AgentWaitOptions {
	signal?: AbortSignal;
	backoffOptions?: BackoffOptions;
	onEvent?: (event: AttachedAgentEvent) => void | Promise<void>;
}

export interface WorkflowRunOptions {
	input?: unknown;
	signal?: AbortSignal;
	backoffOptions?: BackoffOptions;
	onEvent?: (event: FlueEvent) => void | Promise<void>;
}

export interface WorkflowRunResult<TResult = unknown> {
	runId: string;
	result: TResult;
}

export type FlueExecutionTarget = 'agent_submission' | 'workflow_run';
export type FlueExecutionFailure = 'failed' | 'terminal_event_missing';

export class FlueExecutionError extends Error {
	readonly target: FlueExecutionTarget;
	readonly targetId: string;
	readonly failure: FlueExecutionFailure;
	readonly error: unknown;

	constructor(options: {
		target: FlueExecutionTarget;
		targetId: string;
		failure: FlueExecutionFailure;
		error?: unknown;
	}) {
		super(executionErrorMessage(options));
		this.name = 'FlueExecutionError';
		this.target = options.target;
		this.targetId = options.targetId;
		this.failure = options.failure;
		this.error = options.error;
	}
}

export async function waitForAgentSubmission<TResult>(
	http: HttpClient,
	admission: AgentSendResult,
	options: AgentWaitOptions = {},
): Promise<TResult> {
	const stream = createFlueEventStream<AttachedAgentEvent>(
		{
			offset: admission.offset,
			signal: options.signal,
			backoffOptions: options.backoffOptions,
		},
		{ url: admission.streamUrl, fetch: http.fetchWithHeaders.bind(http) },
	);

	for await (const event of stream) {
		if (event.submissionId !== admission.submissionId) continue;
		await options.onEvent?.(event);
		throwIfAborted(options.signal);
		if (event.type !== 'submission_settled') continue;
		if (event.outcome === 'completed') return event.result as TResult;
		throw new FlueExecutionError({
			target: 'agent_submission',
			targetId: admission.submissionId,
			failure: 'failed',
			error: event.error,
		});
	}

	throwIfAborted(options.signal);
	throw new FlueExecutionError({
		target: 'agent_submission',
		targetId: admission.submissionId,
		failure: 'terminal_event_missing',
	});
}

export async function runWorkflow<TResult>(
	http: HttpClient,
	name: string,
	options: WorkflowRunOptions = {},
): Promise<WorkflowRunResult<TResult>> {
	const admission = await http.json<{ runId: string }>({
		method: 'POST',
		path: `/workflows/${encodeURIComponent(name)}`,
		body: options.input,
		signal: options.signal,
	});
	const stream = createFlueEventStream<FlueEvent>(
		{ signal: options.signal, backoffOptions: options.backoffOptions },
		{
			url: http.url(`/runs/${encodeURIComponent(admission.runId)}`),
			fetch: http.fetchWithHeaders.bind(http),
		},
	);

	for await (const event of stream) {
		await options.onEvent?.(event);
		throwIfAborted(options.signal);
		if (event.type !== 'run_end' || event.runId !== admission.runId) continue;
		if (!event.isError) return { runId: admission.runId, result: event.result as TResult };
		throw new FlueExecutionError({
			target: 'workflow_run',
			targetId: admission.runId,
			failure: 'failed',
			error: event.error,
		});
	}

	throwIfAborted(options.signal);
	const record = await http.json<RunRecord>({
		path: `/runs/${encodeURIComponent(admission.runId)}?meta`,
		signal: options.signal,
	});
	if (record.status === 'completed') {
		return { runId: admission.runId, result: record.result as TResult };
	}
	if (record.status === 'errored') {
		throw new FlueExecutionError({
			target: 'workflow_run',
			targetId: admission.runId,
			failure: 'failed',
			error: record.error,
		});
	}
	throw new FlueExecutionError({
		target: 'workflow_run',
		targetId: admission.runId,
		failure: 'terminal_event_missing',
	});
}

function throwIfAborted(signal?: AbortSignal): void {
	if (signal?.aborted) throw signal.reason ?? new DOMException('Aborted', 'AbortError');
}

function executionErrorMessage(options: {
	target: FlueExecutionTarget;
	targetId: string;
	failure: FlueExecutionFailure;
	error?: unknown;
}): string {
	const target = options.target === 'agent_submission' ? 'Agent submission' : 'Workflow run';
	if (options.failure === 'terminal_event_missing') {
		return `${target} ${options.targetId} ended without a terminal event`;
	}
	const message = errorMessage(options.error);
	return `${target} ${options.targetId} failed${message ? `: ${message}` : ''}`;
}

function errorMessage(error: unknown): string | undefined {
	if (typeof error === 'string') return error;
	if (typeof error !== 'object' || error === null || !('message' in error)) return undefined;
	return typeof error.message === 'string' ? error.message : undefined;
}
