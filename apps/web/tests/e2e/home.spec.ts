import { expect, test } from '@playwright/test';

test('home shell renders core navigation', async ({ page }) => {
	await page.goto('/');

	await expect(page.getByRole('link', { name: 'ATLANTIS' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Home' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Files' })).toBeVisible();
	await expect(page).toHaveTitle(/ATLANTIS/i);
});
