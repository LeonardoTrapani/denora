export class InvalidTelegramInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid Telegram channel input: ${field}.`);
		this.name = 'InvalidTelegramInputError';
		this.field = field;
	}
}

export class InvalidTelegramConversationKeyError extends TypeError {
	constructor() {
		super('Invalid Telegram conversation key.');
		this.name = 'InvalidTelegramConversationKeyError';
	}
}
