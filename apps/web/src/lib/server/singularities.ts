const SHOW_SINGULARITIES_ENV = 'SHOW_SINGULARITIES';

export function shouldShowSingularities(): boolean {
	return process.env[SHOW_SINGULARITIES_ENV]?.trim() === 'true';
}
