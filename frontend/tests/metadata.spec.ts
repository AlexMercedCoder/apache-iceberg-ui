import { test, expect } from '@playwright/test';

test.describe('Metadata Viewer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Connect
    await page.getByLabel('Catalog Name (Alias)').fill('default');
    await page.getByLabel('Catalog URI').fill('http://localhost:8181');
    await page.getByRole('button', { name: 'Connect' }).click();
    
    // Wait for explorer
    await expect(page.getByText('Explorer')).toBeVisible();
    
    // Expand first namespace and click first table
    // Note: This assumes data exists. If no data, these tests will fail or need mocking.
    const namespace = page.locator('.MuiListItem-root').first();
    await namespace.click();
    await expect(page.locator('.MuiCollapse-entered')).toBeVisible();
    
    const table = page.locator('.MuiCollapse-entered .MuiListItem-root').first();
    await table.click();
    
    // Wait for Metadata Viewer tab to be active
    await expect(page.getByRole('tab', { name: 'Metadata Viewer' })).toHaveAttribute('aria-selected', 'true');
  });

  test('should show metadata tabs', async ({ page }) => {
    await expect(page.getByRole('tab', { name: 'Overview' })).toBeVisible();
    await expect(page.getByRole('tab', { name: 'Schema' })).toBeVisible();
    await expect(page.getByRole('tab', { name: 'Snapshots' })).toBeVisible();
    await expect(page.getByRole('tab', { name: 'Visualization' })).toBeVisible();
  });

  test('should show visualization charts', async ({ page }) => {
    await page.getByRole('tab', { name: 'Visualization' }).click();
    // Check for charts
    await expect(page.getByText('File Size Distribution')).toBeVisible();
    await expect(page.getByText('Partition Record Counts')).toBeVisible();
  });
});
