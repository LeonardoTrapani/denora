export class InvalidWhatsAppInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid WhatsApp channel input: ${field}.`);
		this.name = 'InvalidWhatsAppInputError';
		this.field = field;
	}
}

export class InvalidWhatsAppConversationKeyError extends TypeError {
	constructor() {
		super('Invalid WhatsApp conversation key.');
		this.name = 'InvalidWhatsAppConversationKeyError';
	}
}
