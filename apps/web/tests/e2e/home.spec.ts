import { expect, test } from '@playwright/test';

test('home shell renders core navigation', async ({ page }) => {
	await page.goto('/');

	await expect(page.getByRole('link', { name: 'NetFlow Analysis' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Home' })).toBeVisible();
	await expect(page.getByRole('link', { name: 'Files' })).toBeVisible();
	await expect(page).toHaveTitle(/NetFlow/i);
});
