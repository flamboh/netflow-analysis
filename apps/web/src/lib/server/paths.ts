import path from 'path';
import { fileURLToPath } from 'url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(currentDir, '../../../../../');

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
