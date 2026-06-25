import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const QUERY = '"cloudflare/pages-action" path:.github/workflows/';
const DEFAULT_RANGES = [
	[0, 610],
	[611, 915],
	[916, 1220],
	[1221, 1831],
	[1832, 2441],
	[2442, 4882],
	[4883, 9765],
	[9766, 19531],
	[19532, 39062],
	[39063, 10000000],
];
const PAGE_SIZE = 100;
const MAX_PAGES = 10;
const SEARCH_DELAY_MS = 7000;
const RATE_LIMIT_WAIT_MS = 65000;

function print(value) {
	process.stdout.write(`${value}\n`);
}

function report(value) {
	process.stderr.write(`${value}\n`);
}

function usage() {
	print(`Usage: node scripts/find-pages-action-repos.mjs [options]

Options:
  --cutoff <ISO date>     pushedAt cutoff (default: 30 days ago)
  --out <directory>       run/output directory (default: .cache/pages-action-repos)
  --verify                redownload search pages into a verification pass
  --finalize-only         generate outputs from saved search pages only
  --help                  display this message

Search collection checkpoints every API result page. Rerunning resumes missing pages.`);
}

function defaultCutoff() {
	const date = new Date();
	date.setUTCDate(date.getUTCDate() - 30);
	return date.toISOString();
}

function parseArgs(args) {
	const options = {
		cutoff: defaultCutoff(),
		out: resolve('.cache/pages-action-repos'),
		verify: false,
		finalizeOnly: false,
	};
	for (let i = 0; i < args.length; i++) {
		switch (args[i]) {
			case '--cutoff':
				options.cutoff = new Date(args[++i]).toISOString();
				break;
			case '--out':
				options.out = resolve(args[++i]);
				break;
			case '--verify':
				options.verify = true;
				break;
			case '--finalize-only':
				options.finalizeOnly = true;
				break;
			case '--help':
				usage();
				return;
			default:
				throw new Error(`Unknown argument: ${args[i]}`);
		}
	}
	return options;
}

function gh(args) {
	return execFileSync('gh', args, { encoding: 'utf8', maxBuffer: 100 * 1024 * 1024 });
}

function readJson(path) {
	return JSON.parse(readFileSync(path, 'utf8'));
}

function writeJson(path, value) {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function rangeName([min, max]) {
	return `size-${min}-${max}`;
}

function pagePath(directory, range, page) {
	return join(directory, rangeName(range), `page-${String(page).padStart(2, '0')}.json`);
}

function sleep(ms) {
	return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

let nextSearchAt = 0;

async function codeSearch(range, page) {
	const delay = Math.max(0, nextSearchAt - Date.now());
	if (delay > 0) await sleep(delay);
	nextSearchAt = Date.now() + SEARCH_DELAY_MS;
	for (;;) {
		try {
			return JSON.parse(
				gh([
					'api',
					'-X',
					'GET',
					'search/code',
					'-f',
					`q=${QUERY} size:${range[0]}..${range[1]}`,
					'-f',
					`per_page=${PAGE_SIZE}`,
					'-f',
					`page=${page}`,
				]),
			);
		} catch (error) {
			const message = `${error.stderr ?? error.message}`;
			if (!message.toLowerCase().includes('rate limit')) throw error;
			report(
				`Rate limited; waiting ${RATE_LIMIT_WAIT_MS / 1000}s before retrying ${rangeName(range)} page ${page}`,
			);
			await sleep(RATE_LIMIT_WAIT_MS);
			nextSearchAt = 0;
		}
	}
}

async function fetchRange(directory, range) {
	const firstPath = pagePath(directory, range, 1);
	const first = existsSync(firstPath)
		? readJson(firstPath)
		: await fetchAndSavePage(directory, range, 1);
	const reportedPages = Math.max(1, Math.ceil(first.total_count / PAGE_SIZE));
	if (reportedPages >= MAX_PAGES && range[0] !== range[1]) {
		const middle = Math.floor((range[0] + range[1]) / 2);
		report(`${rangeName(range)} reported ${first.total_count}; splitting`);
		return [
			...(await fetchRange(directory, [range[0], middle])),
			...(await fetchRange(directory, [middle + 1, range[1]])),
		];
	}
	for (let page = 2; page <= Math.min(reportedPages, MAX_PAGES); page++) {
		const path = pagePath(directory, range, page);
		if (!existsSync(path)) await fetchAndSavePage(directory, range, page);
	}
	const last = readJson(pagePath(directory, range, Math.min(reportedPages, MAX_PAGES)));
	if (last.items.length === PAGE_SIZE && range[0] !== range[1]) {
		const middle = Math.floor((range[0] + range[1]) / 2);
		report(`${rangeName(range)} has a full last available page; splitting for safety`);
		return [
			...(await fetchRange(directory, [range[0], middle])),
			...(await fetchRange(directory, [middle + 1, range[1]])),
		];
	}
	report(`${rangeName(range)} complete (${reportedPages} reported pages)`);
	return [range];
}

async function fetchAndSavePage(directory, range, page) {
	const result = await codeSearch(range, page);
	writeJson(pagePath(directory, range, page), result);
	report(
		`Saved ${rangeName(range)} page ${page} (${result.items.length} items; reported total ${result.total_count})`,
	);
	return result;
}

function savedPages(directory) {
	if (!existsSync(directory)) return [];
	const files = [];
	for (const range of readdirSync(directory, { withFileTypes: true })) {
		if (!range.isDirectory() || !range.name.startsWith('size-')) continue;
		for (const page of readdirSync(join(directory, range.name))) {
			if (page.endsWith('.json')) files.push(join(directory, range.name, page));
		}
	}
	return files.sort();
}

function itemKey(item) {
	return `${item.repository.full_name}\t${item.path}`;
}

function collectFiles(directories) {
	const files = new Map();
	for (const directory of directories) {
		for (const path of savedPages(directory)) {
			for (const item of readJson(path).items) files.set(itemKey(item), item);
		}
	}
	return [...files.values()].sort(
		(a, b) =>
			a.repository.full_name.localeCompare(b.repository.full_name) || a.path.localeCompare(b.path),
	);
}

function fetchRepositories(names, metadataDirectory) {
	const repositories = new Map();
	const missing = [];
	for (const name of names) {
		const path = join(metadataDirectory, `${encodeURIComponent(name)}.json`);
		if (existsSync(path)) {
			const repository = readJson(path);
			if (repository) repositories.set(repository.nameWithOwner, repository);
		} else {
			missing.push(name);
		}
	}
	for (let offset = 0; offset < missing.length; offset += 100) {
		const batch = missing.slice(offset, offset + 100);
		const fields = batch
			.map((name, index) => {
				const [owner, repository] = name.split('/');
				return `r${index}: repository(owner: ${JSON.stringify(owner)}, name: ${JSON.stringify(repository)}) { nameWithOwner url pushedAt isArchived isPrivate }`;
			})
			.join('\n');
		const result = JSON.parse(gh(['api', 'graphql', '-f', `query=query { ${fields} }`]));
		for (let index = 0; index < batch.length; index++) {
			const repository = result.data[`r${index}`];
			writeJson(join(metadataDirectory, `${encodeURIComponent(batch[index])}.json`), repository);
			if (repository) repositories.set(repository.nameWithOwner, repository);
		}
		report(
			`Saved repository metadata ${Math.min(offset + batch.length, missing.length)}/${missing.length}`,
		);
	}
	return [...repositories.values()].sort((a, b) => a.nameWithOwner.localeCompare(b.nameWithOwner));
}

function finalize(options, searchDirectories) {
	const files = collectFiles(searchDirectories);
	const names = [...new Set(files.map((item) => item.repository.full_name))].sort();
	const repositories = fetchRepositories(names, join(options.out, 'repository-metadata'));
	const active = repositories.filter((repository) => repository.pushedAt >= options.cutoff);
	const output = join(options.out, 'output');
	writeJson(join(output, 'matching-workflow-files.json'), files);
	writeJson(join(output, 'matching-repos.json'), repositories);
	writeJson(join(output, `active-repos-since-${options.cutoff.slice(0, 10)}.json`), active);
	writeFileSync(
		join(output, `active-repos-since-${options.cutoff.slice(0, 10)}.tsv`),
		`${active.map((repository) => `${repository.nameWithOwner}\t${repository.pushedAt}\t${repository.url}`).join('\n')}\n`,
	);
	const primaryFiles = new Set(collectFiles([searchDirectories[0]]).map(itemKey));
	const verificationFiles = searchDirectories[1]
		? new Set(collectFiles([searchDirectories[1]]).map(itemKey))
		: undefined;
	const summary = {
		query: QUERY,
		cutoff: options.cutoff,
		searchDirectories: searchDirectories.map((directory) => resolve(directory)),
		matchingWorkflowFiles: files.length,
		matchingRepositories: repositories.length,
		activeRepositories: active.length,
		verification: verificationFiles
			? {
					primaryWorkflowFiles: primaryFiles.size,
					verificationWorkflowFiles: verificationFiles.size,
					primaryOnly: [...primaryFiles].filter((key) => !verificationFiles.has(key)).length,
					verificationOnly: [...verificationFiles].filter((key) => !primaryFiles.has(key)).length,
				}
			: undefined,
		generatedAt: new Date().toISOString(),
	};
	writeJson(join(output, 'run-summary.json'), summary);
	print(JSON.stringify(summary, null, 2));
}

const options = parseArgs(process.argv.slice(2));
if (options) {
	const primaryDirectory = join(options.out, 'search');
	const directories = [primaryDirectory];
	mkdirSync(options.out, { recursive: true });
	if (!options.finalizeOnly) {
		for (const range of DEFAULT_RANGES) await fetchRange(primaryDirectory, range);
		if (options.verify) {
			const verificationDirectory = join(options.out, 'verification-search');
			for (const range of DEFAULT_RANGES) await fetchRange(verificationDirectory, range);
			directories.push(verificationDirectory);
		}
	} else if (options.verify && existsSync(join(options.out, 'verification-search'))) {
		directories.push(join(options.out, 'verification-search'));
	}
	finalize(options, directories);
}
