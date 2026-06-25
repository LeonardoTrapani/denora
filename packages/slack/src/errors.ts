export class InvalidSlackConversationKeyError extends Error {
	constructor() {
		super('Invalid Slack conversation key.');
		this.name = 'InvalidSlackConversationKeyError';
	}
}

export class InvalidSlackInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid Slack ${field}.`);
		this.name = 'InvalidSlackInputError';
		this.field = field;
	}
}
