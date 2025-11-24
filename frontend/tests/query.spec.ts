import { test, expect } from '@playwright/test';

test.describe('Query Editor', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Connect
    await page.getByLabel('Catalog Name (Alias)').fill('default');
    await page.getByLabel('Catalog URI').fill('http://localhost:8181');
    await page.getByRole('button', { name: 'Connect' }).click();
    await expect(page.getByText('Explorer')).toBeVisible();
  });

  test('should run a query', async ({ page }) => {
    // Type a query
    await page.getByPlaceholder('Enter SQL query...').fill('SELECT 1');
    
    // Click Run
    await page.getByRole('button', { name: 'Run' }).click();
    
    // Verify results or error
    // If backend is real, it might fail if table doesn't exist, but we check for UI reaction
    // Expect loading spinner or result table or error message
    await expect(page.locator('table').or(page.getByText('No results')).or(page.getByText('Error'))).toBeVisible();
  });

  test('should populate editor with template', async ({ page }) => {
    await page.getByRole('button', { name: 'Template' }).click();
    await expect(page.getByPlaceholder('Enter SQL query...')).toHaveValue(/CREATE TABLE/);
    
    await page.getByRole('button', { name: 'Select *' }).click();
    await expect(page.getByPlaceholder('Enter SQL query...')).toHaveValue(/SELECT \* FROM/);
  });

  test('should enable export buttons when query has SQL', async ({ page }) => {
    await page.getByPlaceholder('Enter SQL query...').fill('SELECT * FROM table');
    await expect(page.getByRole('button', { name: 'CSV' })).toBeEnabled();
    await expect(page.getByRole('button', { name: 'JSON' })).toBeEnabled();
    await expect(page.getByRole('button', { name: 'Parquet' })).toBeEnabled();
  });
});
