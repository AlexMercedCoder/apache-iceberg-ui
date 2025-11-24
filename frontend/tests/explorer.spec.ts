import { test, expect } from '@playwright/test';

test.describe('Table Explorer', () => {
  test.beforeEach(async ({ page }) => {
    // Mock successful connection state or perform connection
    // For now, we assume the app starts in disconnected state
    await page.goto('/');
    
    // Connect to default catalog (assuming backend is running and has default catalog)
    // If backend is fresh, we might need to add a catalog first
    // This test assumes a "default" catalog exists or we can add one
    
    // Try to connect to "default"
    await page.getByLabel('Catalog Name (Alias)').fill('default');
    await page.getByLabel('Catalog URI').fill('http://localhost:8181');
    await page.getByRole('button', { name: 'Connect' }).click();
    
    // Wait for explorer to load
    await expect(page.getByText('Explorer')).toBeVisible();
  });

  test('should list namespaces', async ({ page }) => {
    // Check if any namespace folder is visible
    // This depends on the backend having data
    // We can check for "No tables found" or a folder icon
    await expect(page.locator('.MuiList-root')).toBeVisible();
  });

  test('should allow switching catalogs', async ({ page }) => {
    await expect(page.getByLabel('Catalog')).toBeVisible();
    await page.getByLabel('Catalog').click();
    await expect(page.getByRole('listbox')).toBeVisible();
  });

  test('should list tables when namespace is expanded', async ({ page }) => {
    // Click first namespace
    const namespace = page.locator('.MuiListItem-root').first();
    await namespace.click();
    
    // Expect tables to appear (wait for collapse animation)
    await expect(page.locator('.MuiCollapse-entered')).toBeVisible();
    
    // Check for at least one table or "No tables found"
    const tableItems = page.locator('.MuiCollapse-entered .MuiListItem-root');
    await expect(tableItems.first()).toBeVisible();
  });
});
