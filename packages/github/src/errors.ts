export class InvalidGitHubConversationKeyError extends Error {
	constructor() {
		super('Invalid GitHub conversation key.');
		this.name = 'InvalidGitHubConversationKeyError';
	}
}

export class InvalidGitHubInputError extends TypeError {
	readonly field: string;

	constructor(field: string) {
		super(`Invalid GitHub ${field}.`);
		this.name = 'InvalidGitHubInputError';
		this.field = field;
	}
}
