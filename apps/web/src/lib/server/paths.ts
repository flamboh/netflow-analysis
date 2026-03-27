import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));

function hasWorkspacePackage(dir: string): boolean {
	const packageJsonPath = path.join(dir, 'package.json');
	if (!fs.existsSync(packageJsonPath)) {
		return false;
	}

	try {
		const raw = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8')) as {
			workspaces?: unknown;
		};
		return Array.isArray(raw.workspaces);
	} catch {
		return false;
	}
}

function hasRepoMarker(dir: string): boolean {
	return fs.existsSync(path.join(dir, 'datasets.json')) || fs.existsSync(path.join(dir, '.git'));
}

function findRepoRoot(startDir: string): string {
	let dir = startDir;

	while (true) {
		if (hasRepoMarker(dir)) {
			return dir;
		}

		const parent = path.dirname(dir);
		if (parent === dir) {
			break;
		}

		dir = parent;
	}

	for (const candidate of [process.cwd(), path.resolve(startDir, '../../../../../')]) {
		if (hasRepoMarker(candidate) || hasWorkspacePackage(candidate)) {
			return candidate;
		}
	}

	throw new Error(`Unable to resolve repo root from ${startDir}`);
}

const repoRoot = findRepoRoot(currentDir);

function resolveRepoPath(value: string | undefined, fallback: string): string {
	const target = value?.trim() || fallback;
	return path.isAbsolute(target) ? target : path.resolve(repoRoot, target);
}

export function getRepoRoot(): string {
	return repoRoot;
}

export function getDatasetsConfigPath(): string {
	return resolveRepoPath(process.env.DATASETS_CONFIG_PATH, 'datasets.json');
}

export function getNetflowDbDir(): string {
	return resolveRepoPath(process.env.NETFLOW_DB_DIR, 'tools/netflow-db');
}

export function getMaadDir(): string {
	return resolveRepoPath(process.env.MAAD_DIR, 'vendor/maad');
}

export function getLogPath(): string {
	return resolveRepoPath(process.env.LOG_PATH, 'tools/netflow-db/logs');
}
