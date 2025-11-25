import React, { useState } from 'react';
import { Box, Typography, Paper, Divider, List, ListItem, ListItemText, Chip, Tabs, Tab } from '@mui/material';

function Documentation() {
  const [tabIndex, setTabIndex] = useState(0);

  const handleTabChange = (event, newValue) => {
    setTabIndex(newValue);
  };

  const CodeBlock = ({ children }) => (
    <Box sx={{ 
      bgcolor: 'background.paper', 
      p: 2, 
      borderRadius: 1, 
      fontFamily: 'monospace', 
      border: '1px solid',
      borderColor: 'divider',
      my: 1,
      overflowX: 'auto',
      whiteSpace: 'pre'
    }}>
      {children}
    </Box>
  );

  const Section = ({ title, children }) => (
    <Box sx={{ mb: 4 }}>
      <Typography variant="h5" gutterBottom color="primary">{title}</Typography>
      <Divider sx={{ mb: 2 }} />
      {children}
    </Box>
  );

  const TabPanel = ({ children, value, index }) => (
    <div role="tabpanel" hidden={value !== index} style={{ height: '100%' }}>
      {value === index && (
        <Box sx={{ p: 3, pb: 10, height: 'calc(100vh - 300px)', overflow: 'auto' }}>
          {children}
        </Box>
      )}
    </div>
  );

  return (
    <Box sx={{ maxWidth: 1200, mx: 'auto', pb: 4 }}>
      <Typography variant="h3" gutterBottom>Documentation</Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Comprehensive guide to the Iceberg UI: querying, management, and best practices.
      </Typography>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabIndex} onChange={handleTabChange} indicatorColor="primary" textColor="primary" variant="scrollable" scrollButtons="auto">
          <Tab label="Getting Started" />
          <Tab label="Querying" />
          <Tab label="Data Management" />
          <Tab label="Best Practices" />
        </Tabs>
      </Paper>

      {/* TAB 1: GETTING STARTED */}
      <TabPanel value={tabIndex} index={0}>
        <Section title="Connecting to Catalogs">
          <Typography paragraph>
            You can connect to multiple Iceberg catalogs simultaneously.
          </Typography>
          <Typography variant="subtitle2">Manual Connection</Typography>
          <List dense>
            <ListItem><ListItemText primary="1. Click 'Connect' in the header." /></ListItem>
            <ListItem><ListItemText primary="2. Choose a friendly name (e.g., 'prod', 'dev')." /></ListItem>
            <ListItem><ListItemText primary="3. Select type (REST, Hive, Glue, etc.)." /></ListItem>
            <ListItem><ListItemText primary="4. Enter URI and credentials." /></ListItem>
          </List>

          <Typography variant="subtitle2">Auto-Connect (env.json)</Typography>
          <Typography paragraph>Pre-configure catalogs in <code>backend/env.json</code>:</Typography>
          <CodeBlock>
{`{
  "catalogs": {
    "prod": {
      "type": "rest",
      "uri": "https://api.tabular.io/ws",
      "credential": "client:secret",
      "warehouse": "s3://my-bucket/warehouse"
    },
    "local": {
      "type": "rest",
      "uri": "http://localhost:8181"
    }
  }
}`}
          </CodeBlock>
        </Section>

        <Section title="Interface Overview">
          <Typography paragraph>
            <strong>Explorer:</strong> Browse namespaces and tables. Use the dropdown to switch catalogs.
          </Typography>
          <Typography paragraph>
            <strong>Query Editor:</strong> Write and execute SQL. Supports multiple tabs.
          </Typography>
          <Typography paragraph>
            <strong>Metadata Viewer:</strong> Inspect Schema, Snapshots, Files, and Manifests.
          </Typography>
          <Typography paragraph>
            <strong>Dark Mode:</strong> Toggle the theme using the sun/moon icon in the header.
          </Typography>
        </Section>
      </TabPanel>

      {/* TAB 2: QUERYING */}
      <TabPanel value={tabIndex} index={1}>
        <Section title="SQL Syntax">
          <Typography paragraph>
            The UI uses DataFusion for query execution. Standard SQL is supported.
          </Typography>
          <CodeBlock>
{`-- Basic Select
SELECT * FROM db.customers LIMIT 10;

-- Filtering
SELECT name, email FROM db.customers 
WHERE region = 'US' AND active = true;

-- Aggregation
SELECT region, COUNT(*) as count 
FROM db.customers 
GROUP BY region 
HAVING count > 100;`}
          </CodeBlock>
        </Section>

        <Section title="Cross-Catalog Joins">
          <Typography paragraph>
            Join tables across different catalogs using fully qualified names: <code>catalog.namespace.table</code>.
          </Typography>
          <CodeBlock>
{`SELECT 
  prod.user_id, 
  prod.name, 
  stg.experiment_group
FROM 
  production.users.profiles prod
JOIN 
  staging.experiments.assignments stg 
ON 
  prod.user_id = stg.user_id;`}
          </CodeBlock>
          <Chip label="Requirement" color="warning" size="small" sx={{ mr: 1 }} />
          <Typography variant="caption">Both catalogs must be connected.</Typography>
        </Section>

        <Section title="Time Travel">
          <Typography paragraph>Query historical data using snapshots or timestamps.</Typography>
          <CodeBlock>
{`-- By Snapshot ID
SELECT * FROM db.orders FOR SYSTEM_TIME AS OF SNAPSHOT 123456789;

-- By Timestamp
SELECT * FROM db.orders FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';`}
          </CodeBlock>
        </Section>

        <Section title="Metadata Tables">
          <Typography paragraph>Append <code>$table_name</code> to query metadata.</Typography>
          <List dense>
            <ListItem><ListItemText primary="$snapshots" secondary="History of table states" /></ListItem>
            <ListItem><ListItemText primary="$files" secondary="Data files in the current snapshot" /></ListItem>
            <ListItem><ListItemText primary="$manifests" secondary="Manifest files" /></ListItem>
            <ListItem><ListItemText primary="$partitions" secondary="Partition statistics" /></ListItem>
          </List>
          <CodeBlock>SELECT * FROM db.orders$snapshots;</CodeBlock>
        </Section>
      </TabPanel>

      {/* TAB 3: DATA MANAGEMENT */}
      <TabPanel value={tabIndex} index={2}>
        <Section title="File Uploads">
          <Typography paragraph>
            Upload CSV, JSON, or Parquet files.
          </Typography>
          
          <Typography variant="subtitle2">1. Create New Table</Typography>
          <Typography paragraph>
            Click the upload icon on a <strong>Namespace</strong>.
            <br/>- Schema is inferred from the file.
            <br/>- Table is created automatically.
          </Typography>

          <Typography variant="subtitle2">2. Append to Existing Table</Typography>
          <Typography paragraph>
            Click the upload icon on a <strong>Table</strong>.
            <br/>- Data is appended to the existing table.
            <br/>- Schema must match (or be compatible).
          </Typography>
        </Section>

        <Section title="DML Operations">
          <Typography paragraph>
            Modify data using INSERT and DELETE.
          </Typography>
          <CodeBlock>
{`-- Insert Values
INSERT INTO db.logs VALUES (1, 'Error', NOW());

-- Insert from Select (CTAS pattern)
INSERT INTO db.archive_logs 
SELECT * FROM db.logs WHERE level = 'Error';

-- Delete
DELETE FROM db.logs WHERE created_at < DATE('2023-01-01');`}
          </CodeBlock>
        </Section>

        <Section title="Schema Evolution">
          <Typography paragraph>
            Iceberg supports full schema evolution.
          </Typography>
          <List>
            <ListItem><ListItemText primary="Add Column" secondary="ALTER TABLE ... ADD COLUMN" /></ListItem>
            <ListItem><ListItemText primary="Drop Column" secondary="ALTER TABLE ... DROP COLUMN" /></ListItem>
            <ListItem><ListItemText primary="Rename Column" secondary="ALTER TABLE ... RENAME COLUMN" /></ListItem>
            <ListItem><ListItemText primary="Update Type" secondary="ALTER TABLE ... ALTER COLUMN ... TYPE" /></ListItem>
          </List>
          <Typography variant="caption" color="text.secondary">
            * Note: Schema evolution commands are supported via SQL if the backend catalog supports them.
          </Typography>
        </Section>
      </TabPanel>

      {/* TAB 4: BEST PRACTICES */}
      <TabPanel value={tabIndex} index={3}>
        <Section title="Query Performance">
          <Typography paragraph>
            <strong>1. Filter Early:</strong> Always use WHERE clauses on partition columns to prune data.
          </Typography>
          <CodeBlock>
{`-- Good (Prunes partitions)
SELECT * FROM logs WHERE date = '2024-01-01';

-- Bad (Scans all files)
SELECT * FROM logs;`}
          </CodeBlock>
          
          <Typography paragraph>
            <strong>2. Limit Results:</strong> Use <code>LIMIT</code> when exploring data to avoid fetching huge datasets.
          </Typography>

          <Typography paragraph>
            <strong>3. Use Metadata Tables:</strong> Check <code>$files</code> to see how many files your query might scan.
          </Typography>
        </Section>

        <Section title="Data Maintenance">
          <Typography paragraph>
            <strong>Compaction:</strong> Regularly compact small files to improve read performance.
          </Typography>
          <Typography paragraph>
            <strong>Expire Snapshots:</strong> Remove old snapshots to free up storage space.
          </Typography>
          <CodeBlock>
{`-- Example Maintenance Procedure (Conceptual)
CALL system.rewrite_data_files(table => 'db.logs');
CALL system.expire_snapshots(table => 'db.logs', older_than => ...);`}
          </CodeBlock>
        </Section>

        <Section title="Catalog Management">
          <Typography paragraph>
            - Use separate catalogs for Prod/Dev/Staging.
            - Use <code>env.json</code> to share configuration with your team (but don't commit secrets!).
            - Use descriptive names for your catalogs to avoid confusion in cross-catalog joins.
          </Typography>
        </Section>
      </TabPanel>
    </Box>
  );
}

export default Documentation;
