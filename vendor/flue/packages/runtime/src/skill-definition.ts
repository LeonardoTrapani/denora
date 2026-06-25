import { SkillDefinitionValidationError, type ValidationIssue } from './errors.ts';
import { buildPackagedSkill, createSkillReference } from './skill-package.ts';
import type { SkillReference } from './types.ts';

export interface DefineSkillOptions {
	name: string;
	description: string;
	instructions?: string;
	license?: string;
	compatibility?: string;
	metadata?: Readonly<Record<string, string>>;
	allowedTools?: string;
	files?: Readonly<Record<string, string | Uint8Array>>;
}

const encoder = new TextEncoder();

export function defineSkill(options: DefineSkillOptions): SkillReference {
	const normalized = validateOptions(options);
	const files = [
		{ path: 'SKILL.md', content: encoder.encode(serializeSkillMarkdown(normalized)) },
		...Object.entries(normalized.files).map(([path, value]) => ({
			path,
			content: typeof value === 'string' ? encoder.encode(value) : value,
		})),
	];
	return createSkillReference(
		buildPackagedSkill({
			name: normalized.name,
			description: normalized.description,
			files,
		}),
	);
}

interface NormalizedSkillOptions {
	name: string;
	description: string;
	instructions: string;
	license?: string;
	compatibility?: string;
	metadata?: Record<string, string>;
	allowedTools?: string;
	files: Record<string, string | Uint8Array>;
}

function validateOptions(options: DefineSkillOptions): NormalizedSkillOptions {
	const issues: ValidationIssue[] = [];
	if (!isRecord(options)) {
		throw new SkillDefinitionValidationError({
			issues: [{ path: [], message: 'Expected a skill definition object.' }],
		});
	}
	const name = requiredString(options.name, 'name', issues);
	if (name.length > 64) issues.push({ path: ['name'], message: 'Must be at most 64 characters.' });
	if (name && !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(name)) {
		issues.push({
			path: ['name'],
			message: 'Must contain only lowercase ASCII letters, numbers, and single hyphens.',
		});
	}
	const description = requiredString(options.description, 'description', issues);
	if ([...description].length > 1024) {
		issues.push({ path: ['description'], message: 'Must be at most 1024 characters.' });
	}
	const instructions = optionalString(options.instructions, 'instructions', issues) ?? '';
	const license = optionalString(options.license, 'license', issues);
	const compatibility = optionalString(options.compatibility, 'compatibility', issues);
	if (compatibility !== undefined && [...compatibility].length > 500) {
		issues.push({ path: ['compatibility'], message: 'Must be at most 500 characters.' });
	}
	const allowedTools = optionalString(options.allowedTools, 'allowedTools', issues);
	const metadata: Record<string, string> | undefined =
		options.metadata === undefined ? undefined : Object.create(null);
	if (options.metadata !== undefined) {
		if (!isRecord(options.metadata)) {
			issues.push({ path: ['metadata'], message: 'Must be a string-to-string mapping.' });
		} else {
			for (const [key, value] of Object.entries(options.metadata)) {
				if (typeof value !== 'string') {
					issues.push({ path: ['metadata', key], message: 'Must be a string.' });
				} else if (metadata) {
					metadata[key] = value;
				}
			}
		}
	}
	const files: Record<string, string | Uint8Array> = Object.create(null);
	if (options.files !== undefined) {
		if (!isRecord(options.files)) {
			issues.push({ path: ['files'], message: 'Must be a file-path mapping.' });
		} else {
			for (const [path, content] of Object.entries(options.files)) {
				validateFilePath(path, issues);
				if (typeof content !== 'string' && !(content instanceof Uint8Array)) {
					issues.push({ path: ['files', path], message: 'Must be a string or Uint8Array.' });
				} else {
					files[path] = typeof content === 'string' ? content : new Uint8Array(content);
				}
			}
		}
	}
	if (issues.length > 0) throw new SkillDefinitionValidationError({ issues });
	return { name, description, instructions, license, compatibility, metadata, allowedTools, files };
}

function requiredString(value: unknown, field: string, issues: ValidationIssue[]): string {
	if (typeof value !== 'string' || value.trim().length === 0) {
		issues.push({ path: [field], message: 'Must be a non-empty string.' });
		return '';
	}
	return value.trim();
}

function optionalString(
	value: unknown,
	field: string,
	issues: ValidationIssue[],
): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== 'string') {
		issues.push({ path: [field], message: 'Must be a string when provided.' });
		return undefined;
	}
	const trimmed = value.trim();
	return trimmed.length > 0 ? trimmed : undefined;
}

function validateFilePath(path: string, issues: ValidationIssue[]): void {
	const segments = path.split('/');
	if (
		path.length === 0 ||
		path === 'SKILL.md' ||
		path.startsWith('/') ||
		path.endsWith('/') ||
		path.includes('\\') ||
		path.includes('\0') ||
		segments.some((segment) => segment.length === 0 || segment === '.' || segment === '..')
	) {
		issues.push({
			path: ['files', path],
			message: 'Must be a safe relative path and must not be SKILL.md.',
		});
	}
}

function serializeSkillMarkdown(options: NormalizedSkillOptions): string {
	const lines = [
		'---',
		`name: ${JSON.stringify(options.name)}`,
		`description: ${JSON.stringify(options.description)}`,
	];
	if (options.license !== undefined) lines.push(`license: ${JSON.stringify(options.license)}`);
	if (options.compatibility !== undefined) {
		lines.push(`compatibility: ${JSON.stringify(options.compatibility)}`);
	}
	if (options.metadata !== undefined) {
		lines.push('metadata:');
		for (const key of Object.keys(options.metadata).sort()) {
			lines.push(`  ${JSON.stringify(key)}: ${JSON.stringify(options.metadata[key])}`);
		}
	}
	if (options.allowedTools !== undefined) {
		lines.push(`allowed-tools: ${JSON.stringify(options.allowedTools)}`);
	}
	lines.push('---', '');
	if (options.instructions.length > 0) lines.push(options.instructions);
	return `${lines.join('\n')}\n`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}
