import { createGitHubChannel } from '@flue/github';
import { defineTool, dispatch } from '@flue/runtime';
import { Octokit } from '@octokit/rest';
import * as v from 'valibot';
import assistant from '../agents/assistant.ts';

export const client = new Octokit({
	auth: requiredEnv('GITHUB_TOKEN'),
});

export const channel = createGitHubChannel({
	webhookSecret: requiredEnv('GITHUB_WEBHOOK_SECRET'),

	// Path: /channels/github/webhook
	async webhook({ delivery }) {
		if (delivery.name === 'issue_comment' && delivery.payload.action === 'created') {
			const { repository, issue, comment } = delivery.payload;
			const issueRef = {
				owner: repository.owner.login,
				repo: repository.name,
				issueNumber: issue.number,
			};
			await dispatch(assistant, {
				id: channel.conversationKey(issueRef),
				input: {
					type: 'github.issue_comment.created',
					deliveryId: delivery.deliveryId,
					installationId: delivery.payload.installation?.id,
					issue: issueRef,
					sender: delivery.payload.sender,
					title: issue.title,
					comment: { id: comment.id, body: comment.body },
				},
			});
			return;
		}

		if (delivery.name === 'pull_request_review_comment' && delivery.payload.action === 'created') {
			const { repository, pull_request, comment } = delivery.payload;
			const issueRef = {
				owner: repository.owner.login,
				repo: repository.name,
				issueNumber: pull_request.number,
			};
			await dispatch(assistant, {
				id: channel.conversationKey(issueRef),
				input: {
					type: 'github.pull_request_review_comment.created',
					deliveryId: delivery.deliveryId,
					installationId: delivery.payload.installation?.id,
					issue: issueRef,
					sender: delivery.payload.sender,
					title: pull_request.title,
					comment: {
						id: comment.id,
						// GitHub replies attach to the top-level review comment in a thread.
						threadId: comment.in_reply_to_id ?? comment.id,
						body: comment.body,
						path: comment.path,
						line: comment.line ?? null,
					},
				},
			});
			return;
		}
	},
});

export function commentOnIssue(ref: { owner: string; repo: string; issueNumber: number }) {
	return defineTool({
		name: 'comment_on_github_issue',
		description: 'Post a comment to the GitHub issue or pull request bound to this agent.',
		input: v.object({ body: v.pipe(v.string(), v.minLength(1)) }),
		async run({ input }) {
			const result = await client.rest.issues.createComment({
				owner: ref.owner,
				repo: ref.repo,
				issue_number: ref.issueNumber,
				body: input.body,
			});
			return { commentId: result.data.id, url: result.data.html_url };
		},
	});
}

function requiredEnv(name: string): string {
	const value = process.env[name];
	if (!value) throw new Error(`${name} is required.`);
	return value;
}
