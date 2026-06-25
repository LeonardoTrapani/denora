export class InvalidDiscordConversationKeyError extends Error {
	constructor() {
		super('Invalid Discord conversation key.');
		this.name = 'InvalidDiscordConversationKeyError';
	}
}

export class InvalidDiscordInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid Discord ${field}.`);
		this.name = 'InvalidDiscordInputError';
		this.field = field;
	}
}
