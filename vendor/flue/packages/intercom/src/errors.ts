export class InvalidIntercomInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid Intercom channel input: ${field}.`);
		this.name = 'InvalidIntercomInputError';
		this.field = field;
	}
}

export class InvalidIntercomConversationKeyError extends TypeError {
	constructor() {
		super('Invalid Intercom conversation key.');
		this.name = 'InvalidIntercomConversationKeyError';
	}
}
