import {
	type AgentPromptImage,
	DurableStreamError,
	FetchError,
	type FlueClient,
	type FlueEventStream,
	type LiveMode,
} from '@flue/sdk';
import {
	type AgentReducerEvent,
	type AgentSnapshot,
	type AgentState,
	emptyAgentState,
	reduceAgentEvent,
} from './agent-reducer.ts';

export interface SendMessageOptions {
	images?: AgentPromptImage[];
}

export type AgentHistory = number | 'all';

export class AgentSession {
	private state: AgentState = { ...emptyAgentState };
	private snapshot: AgentSnapshot = publicSnapshot(this.state);
	private listeners = new Set<() => void>();
	private stream: FlueEventStream | undefined;
	private disposed = false;
	private active = false;
	private generation = 0;
	private dormantFresh = false;
	private reconnectOffset: string | undefined;
	private admittedOffset: string | undefined;
	private reconnectAttempt = 0;
	private reconnectWake: (() => void) | undefined;
	private hydrationState: AgentState = { ...emptyAgentState };
	private hydrationOffset: string | undefined;
	private hydrationLocalEvents: AgentReducerEvent[] = [];
	private localId = 0;

	constructor(
		private client: FlueClient,
		private name: string,
		private id: string,
		private history: AgentHistory = 100,
		private live: LiveMode = true,
	) {}

	start(): void {
		if (this.active) return;
		this.active = true;
		this.disposed = false;
		this.generation++;
		void (this.state.historyReady ? this.connect(this.generation) : this.hydrate(this.generation));
	}

	subscribe = (listener: () => void): (() => void) => {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	};

	getSnapshot = (): AgentSnapshot => this.snapshot;

	async sendMessage(message: string, options: SendMessageOptions = {}): Promise<void> {
		const localId = `local:${this.name}:${this.id}:${++this.localId}`;
		this.dispatch({ type: 'local_send_submitted', localId, message, images: options.images });
		this.wakeReconnect();
		try {
			const receipt = await this.client.agents.send(this.name, this.id, {
				message,
				images: options.images,
			});
			this.dispatch({ type: 'local_send_admitted', localId, submissionId: receipt.submissionId });
			this.admittedOffset = receipt.offset;
			if (this.dormantFresh) {
				this.dormantFresh = false;
				this.reconnectOffset = receipt.offset;
				queueMicrotask(() => void this.connect(this.generation));
			}
		} catch (error) {
			const normalized = toError(error);
			this.dispatch({ type: 'local_send_failed', localId, error: normalized });
			throw error;
		}
	}

	dispose(): void {
		if (!this.active) return;
		this.active = false;
		this.disposed = true;
		this.generation++;
		this.stream?.cancel();
		this.stream = undefined;
		this.wakeReconnect();
	}

	private async hydrate(generation = this.generation): Promise<void> {
		if (!this.isCurrent(generation) || this.stream || this.dormantFresh || this.state.historyReady)
			return;
		this.dispatch({ type: 'local_connecting', error: this.snapshot.error });
		const options = this.hydrationOffset
			? { live: false as const, offset: this.hydrationOffset }
			: this.history === 'all'
				? { live: false as const, offset: '-1' }
				: { live: false as const, offset: '-1', tail: this.history };
		let stream: FlueEventStream;
		try {
			stream = this.client.agents.stream(this.name, this.id, options);
		} catch (error) {
			if (isStatus(error, 404)) {
				this.dormantFresh = true;
				this.dispatch({ type: 'local_stream_not_found' });
			} else if (isFatal(error)) {
				this.dispatch({ type: 'local_stream_failed', error: toError(error) });
			} else {
				await this.retry(toError(error), generation, 'hydrate');
			}
			return;
		}
		this.stream = stream;
		try {
			for await (const event of stream) {
				if (!this.isCurrent(generation)) return;
				this.hydrationState = reduceAgentEvent(
					this.hydrationState,
					event as AgentReducerEvent,
				);
			}
			if (!this.isCurrent(generation) || this.stream !== stream) return;
			this.reconnectAttempt = 0;
			this.commitHydration(stream.offset, generation);
		} catch (error) {
			if (!this.isCurrent(generation) || this.stream !== stream) return;
			this.hydrationOffset = stream.offset !== '-1' ? stream.offset : this.hydrationOffset;
			if (isStatus(error, 404)) {
				if (this.admittedOffset) {
					this.commitHydration(this.admittedOffset, generation);
				} else {
					this.dormantFresh = true;
					this.dispatch({ type: 'local_stream_not_found' });
				}
				return;
			}
			if (isFatal(error)) {
				this.dispatch({ type: 'local_stream_failed', error: toError(error) });
				return;
			}
			await this.retry(toError(error), generation, 'hydrate');
		} finally {
			if (this.stream === stream) this.stream = undefined;
		}
	}

	private commitHydration(offset: string, generation: number): void {
		this.reconnectOffset = offset;
		this.state = this.hydrationLocalEvents.reduce(reduceAgentEvent, this.hydrationState);
		this.state = reduceAgentEvent(this.state, { type: 'local_history_ready' });
		this.hydrationLocalEvents = [];
		this.publish();
		this.stream = undefined;
		queueMicrotask(() => void this.connect(generation));
	}

	private async connect(generation = this.generation): Promise<void> {
		if (!this.isCurrent(generation) || this.stream || this.dormantFresh || !this.state.historyReady)
			return;
		const offset = this.reconnectOffset ?? this.admittedOffset;
		if (!offset) return;
		this.dispatch({ type: 'local_connecting', error: this.snapshot.error });
		let stream: FlueEventStream;
		try {
			stream = this.client.agents.stream(this.name, this.id, { live: this.live, offset });
		} catch (error) {
			if (isFatal(error)) {
				this.dispatch({ type: 'local_stream_failed', error: toError(error) });
			} else {
				await this.retry(toError(error), generation, 'connect');
			}
			return;
		}
		this.stream = stream;
		let delivered = false;
		try {
			for await (const event of stream) {
				if (!this.isCurrent(generation)) return;
				delivered = true;
				this.reconnectAttempt = 0;
				this.dispatch(event as AgentReducerEvent);
			}
			if (this.isCurrent(generation) && this.stream === stream) {
				this.reconnectOffset = stream.offset;
				await this.retry(new Error('Agent event stream ended unexpectedly'), generation, 'connect');
			}
		} catch (error) {
			if (!this.isCurrent(generation) || this.stream !== stream) return;
			this.reconnectOffset = delivered ? stream.offset : this.reconnectOffset;
			if (!delivered && isStatus(error, 404) && this.admittedOffset) {
				this.reconnectOffset = this.admittedOffset;
				await this.retry(toError(error), generation, 'connect');
				return;
			}
			if (isFatal(error)) {
				this.dispatch({ type: 'local_stream_failed', error: toError(error) });
				return;
			}
			await this.retry(toError(error), generation, 'connect');
		} finally {
			if (this.stream === stream) this.stream = undefined;
		}
	}

	private async retry(
		error: Error,
		generation = this.generation,
		phase: 'hydrate' | 'connect' = 'connect',
	): Promise<void> {
		if (!this.isCurrent(generation)) return;
		this.dispatch({ type: 'local_connecting', error });
		const delay = Math.min(1000 * 2 ** this.reconnectAttempt++, 30_000);
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				this.reconnectWake = undefined;
				resolve();
			}, delay);
			this.reconnectWake = () => {
				clearTimeout(timer);
				this.reconnectWake = undefined;
				resolve();
			};
		});
		if (this.isCurrent(generation)) {
			setTimeout(
				() => void (phase === 'hydrate' ? this.hydrate(generation) : this.connect(generation)),
				0,
			);
		}
	}

	private isCurrent(generation: number): boolean {
		return this.active && !this.disposed && generation === this.generation;
	}

	private wakeReconnect(): void {
		this.reconnectWake?.();
	}

	private dispatch(event: AgentReducerEvent): void {
		if (
			!this.state.historyReady &&
			!('eventIndex' in event) &&
			(event.type === 'local_send_submitted' ||
				event.type === 'local_send_admitted' ||
				event.type === 'local_send_failed')
		) {
			this.hydrationLocalEvents.push(event);
		}
		const next = reduceAgentEvent(this.state, event);
		if (next === this.state) return;
		this.state = next;
		this.publish();
	}

	private publish(): void {
		this.snapshot = publicSnapshot(this.state);
		for (const listener of this.listeners) listener();
	}
}

function publicSnapshot(state: AgentState): AgentSnapshot {
	return {
		messages: state.messages,
		status: state.status,
		historyReady: state.historyReady,
		error: state.error,
	};
}

function isStatus(error: unknown, status: number): boolean {
	return (
		(error instanceof FetchError || error instanceof DurableStreamError) && error.status === status
	);
}

function isFatal(error: unknown): boolean {
	return isStatus(error, 401) || isStatus(error, 403) || isStatus(error, 404);
}

function toError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}
