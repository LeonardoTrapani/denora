import {
	defineAgent,
	defineWorkflow,
	type WorkflowRouteHandler,
	type WorkflowRunsHandler,
} from '@flue/runtime';

export const route: WorkflowRouteHandler = async (_c, next) => next();
export const runs: WorkflowRunsHandler = async (_c, next) => next();
const agent = defineAgent(() => ({ model: false }));
export default defineWorkflow({
	agent,
	async run({ log }) {
		log.info('boom workflow about to explode', { reason: 'demo' });
		throw new Error('intentional explosion for the Sentry demo');
	},
});
