export class InvalidMessengerInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid Messenger channel input: ${field}.`);
		this.name = 'InvalidMessengerInputError';
		this.field = field;
	}
}

export class InvalidMessengerConversationKeyError extends TypeError {
	constructor() {
		super('Invalid Messenger conversation key.');
		this.name = 'InvalidMessengerConversationKeyError';
	}
}
