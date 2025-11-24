import { test, expect } from '@playwright/test';

test.describe('Connection Flow', () => {
  test('should load the connection form initially', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('Connect to Iceberg Catalog')).toBeVisible();
    await expect(page.getByLabel('Catalog Name (Alias)')).toBeVisible();
  });

  test('should show error for invalid connection', async ({ page }) => {
    await page.goto('/');
    
    // Fill form with dummy data
    await page.getByLabel('Catalog Name (Alias)').fill('test_catalog');
    await page.getByLabel('Catalog URI').fill('http://localhost:9999'); // Invalid URI
    await page.getByLabel('Warehouse Path').fill('s3://test/wh');
    
    await page.getByRole('button', { name: 'Connect' }).click();
    
    // Expect alert or error message (browser dialog handling might be needed)
    // Since we use window.alert, we need to handle the dialog
    page.on('dialog', dialog => {
        expect(dialog.message()).toContain('Connection failed');
        dialog.dismiss();
    });
  });
});
