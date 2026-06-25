import type { AttachedAgentEvent } from '@flue/sdk';
import { describe, expect, it } from 'vitest';
import { emptyAgentState, reduceAgentEvent } from '../src/agent-reducer.ts';
import eventsSource from './fixtures/agent-events.jsonl?raw';

function fixtureEvents(): AttachedAgentEvent[] {
	return eventsSource
		.trim()
		.split('\n')
		.map((line: string) => JSON.parse(line) as AttachedAgentEvent);
}

describe('reduceAgentEvent() runtime fixture', () => {
	it('folds deltas, thinking, parallel tools, terminal reconciliation, and multiple turns', () => {
		const events = fixtureEvents();
		const beforeIdle = events.slice(0, -2).reduce(reduceAgentEvent, emptyAgentState);

		expect(beforeIdle.messages.map((message) => message.id)).toEqual([
			'submission:submission-1:user:0',
			'turn:turn-1',
			'submission:submission-2:user:0',
			'turn:turn-2',
		]);
		expect(beforeIdle.messages[1]?.parts).toEqual([
			{ type: 'reasoning', text: 'compare', state: 'done' },
			{ type: 'text', text: 'Checking', state: 'done' },
			{
				type: 'dynamic-tool',
				toolName: 'inspect',
				toolCallId: 'tool-a',
				state: 'output-available',
				input: { image: 1 },
				output: { label: 'document' },
				errorText: undefined,
			},
			{
				type: 'dynamic-tool',
				toolName: 'inspect',
				toolCallId: 'tool-b',
				state: 'output-error',
				input: { image: 2 },
				output: undefined,
				errorText: 'unreadable',
			},
		]);
		expect(beforeIdle.messages[1]?.metadata).toMatchObject({
			model: { provider: 'fixture-provider', id: 'fixture-model' },
			usage: { totalTokens: 14 },
		});
		expect(beforeIdle.status).toBe('idle');
	});

	it('renders thinking and text as their deltas arrive', () => {
		const events = fixtureEvents();
		const throughThinking = events.slice(0, 6).reduce(reduceAgentEvent, emptyAgentState);
		const throughText = events.slice(0, 7).reduce(reduceAgentEvent, emptyAgentState);

		expect(throughThinking.messages[1]?.parts).toEqual([
			{ type: 'reasoning', text: 'compare', state: 'done' },
		]);
		expect(throughText.messages[1]?.parts).toEqual([
			{ type: 'reasoning', text: 'compare', state: 'done' },
			{ type: 'text', text: 'Checking', state: 'streaming' },
		]);
	});

	it('preserves distinct local image data URLs when redacted echoes use the same media type', () => {
		const events = fixtureEvents();
		let state = reduceAgentEvent(emptyAgentState, {
			type: 'local_send_submitted',
			localId: 'local-images',
			message: 'Inspect these',
			images: [
				{ type: 'image', mimeType: 'image/png', data: 'Zmlyc3Q=' },
				{ type: 'image', mimeType: 'image/png', data: 'c2Vjb25k' },
			],
		});
		state = reduceAgentEvent(state, {
			type: 'local_send_admitted',
			localId: 'local-images',
			submissionId: 'submission-1',
		});
		const echoedUser = events[0];
		if (!echoedUser) throw new Error('Fixture is empty');
		state = reduceAgentEvent(state, echoedUser);

		expect(state.messages).toHaveLength(1);
		expect(state.messages[0]?.parts).toEqual([
			{ type: 'text', text: 'Inspect these', state: 'done' },
			{ type: 'file', mediaType: 'image/png', url: 'data:image/png;base64,Zmlyc3Q=' },
			{ type: 'file', mediaType: 'image/png', url: 'data:image/png;base64,c2Vjb25k' },
		]);
	});

	it('drops late deltas and tools until terminal reconciliation supplies the message', () => {
		const events = fixtureEvents();
		const window = events.slice(3, 12);
		const state = window.reduce(reduceAgentEvent, emptyAgentState);

		expect(state.messages).toHaveLength(1);
		expect(state.messages[0]?.id).toBe('turn:turn-1');
		expect(state.messages[0]?.parts).toHaveLength(4);
	});

	it('keeps duplicate terminal reconciliation idempotent', () => {
		const events = fixtureEvents();
		const once = events.reduce(reduceAgentEvent, emptyAgentState);
		const terminal = events[11];
		if (!terminal) throw new Error('Fixture has no terminal message');
		const twice = reduceAgentEvent(once, terminal);

		expect(twice.messages).toEqual(once.messages);
		expect(twice.messages).toHaveLength(4);
	});

	it('surfaces terminal submission failure before the final idle boundary', () => {
		const events = fixtureEvents();
		let state = reduceAgentEvent(emptyAgentState, {
			type: 'local_send_submitted',
			localId: 'local-2',
			message: 'again',
		});
		state = reduceAgentEvent(state, {
			type: 'local_send_admitted',
			localId: 'local-2',
			submissionId: 'submission-2',
		});
		for (const event of events.slice(14, 19)) state = reduceAgentEvent(state, event);

		expect(state.status).toBe('error');
		expect(state.error?.message).toBe('fixture terminal failure');
	});
});
