import React from 'react';
import { Box, Typography, Paper, Divider, List, ListItem, ListItemText, Chip } from '@mui/material';

function Documentation() {
  const CodeBlock = ({ children }) => (
    <Box sx={{ 
      bgcolor: 'background.paper', 
      p: 2, 
      borderRadius: 1, 
      fontFamily: 'monospace', 
      border: '1px solid',
      borderColor: 'divider',
      my: 1,
      overflowX: 'auto'
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

  return (
    <Box sx={{ maxWidth: 1200, mx: 'auto', pb: 4 }}>
      <Typography variant="h3" gutterBottom>Documentation</Typography>
      <Typography variant="subtitle1" color="text.secondary" paragraph>
        Learn how to use the Iceberg UI to query, manage, and analyze your Apache Iceberg tables.
      </Typography>

      <Section title="🎉 What's New">
        <Typography paragraph>
          <strong>Multi-Catalog Support:</strong> Connect to multiple catalogs simultaneously and switch between them using the dropdown selector.
        </Typography>
        <Typography paragraph>
          <strong>Cross-Catalog Joins:</strong> Query and join tables from different catalogs in a single SQL statement.
        </Typography>
        <Typography paragraph>
          <strong>DML Operations:</strong> Execute INSERT and DELETE statements directly on your Iceberg tables.
        </Typography>
        <Typography paragraph>
          <strong>File Uploads:</strong> Upload CSV, JSON, and Parquet files to append data to your tables.
        </Typography>
      </Section>

      <Section title="Multi-Catalog Management">
        <Typography paragraph>
          Connect to multiple Iceberg catalogs and manage them from a single interface.
        </Typography>
        
        <Typography variant="subtitle2">Connecting to a Catalog</Typography>
        <Typography paragraph>
          1. Click the "Connect" button<br/>
          2. Enter a friendly name for your catalog (e.g., "production", "staging")<br/>
          3. Select the catalog type (REST, Hive, Glue, etc.)<br/>
          4. Provide connection details (URI, warehouse, credentials)<br/>
          5. Click "Connect"
        </Typography>

        <Typography variant="subtitle2">Switching Between Catalogs</Typography>
        <Typography paragraph>
          Use the dropdown selector in the Explorer sidebar to switch between connected catalogs. Each catalog maintains its own namespaces and tables.
        </Typography>

        <Typography variant="subtitle2">Logging Out</Typography>
        <Typography paragraph>
          Click "Log Out" in the header to disconnect from all catalogs and return to the connection screen.
        </Typography>
      </Section>

      <Section title="Querying Tables">
        <Typography paragraph>
          The Query Editor supports standard SQL syntax via DataFusion. You can query tables using their namespace-qualified names.
        </Typography>
        <CodeBlock>
          SELECT * FROM db.customers LIMIT 10;
        </CodeBlock>
        <CodeBlock>
          SELECT id, name, email FROM db.customers WHERE region = 'US';
        </CodeBlock>
        
        <Typography variant="h6" sx={{ mt: 2 }}>Supported Features</Typography>
        <List>
          <ListItem>
            <ListItemText primary="Predicate Pushdown" secondary="WHERE clauses are pushed down to Iceberg to minimize data scanning." />
          </ListItem>
          <ListItem>
            <ListItemText primary="Column Projection" secondary="Only requested columns are read from storage." />
          </ListItem>
          <ListItem>
            <ListItemText primary="Query Caching" secondary="Results are cached automatically for improved performance." />
          </ListItem>
        </List>
      </Section>

      <Section title="Cross-Catalog Joins">
        <Typography paragraph>
          Query and join tables from different catalogs in a single SQL statement. Use fully qualified names with the catalog prefix.
        </Typography>
        
        <Typography variant="subtitle2">Syntax</Typography>
        <CodeBlock>
          SELECT u.name, o.amount{'\n'}
          FROM catalog1.db.users u{'\n'}
          JOIN catalog2.db.orders o ON u.id = o.user_id;
        </CodeBlock>

        <Typography variant="subtitle2">Example: Production + Staging</Typography>
        <CodeBlock>
          SELECT prod.id, prod.name, stg.test_results{'\n'}
          FROM production.analytics.customers prod{'\n'}
          LEFT JOIN staging.analytics.test_data stg ON prod.id = stg.customer_id;
        </CodeBlock>

        <Chip label="Tip" color="info" size="small" sx={{ mr: 1 }} />
        <Typography variant="caption">
          Make sure both catalogs are connected before running cross-catalog queries.
        </Typography>
      </Section>

      <Section title="DML Operations">
        <Typography paragraph>
          Execute data manipulation operations directly on your Iceberg tables.
        </Typography>

        <Typography variant="subtitle2">INSERT - Values</Typography>
        <CodeBlock>
          INSERT INTO db.customers VALUES{'\n'}
            (1, 'Alice', 'alice@example.com'),{'\n'}
            (2, 'Bob', 'bob@example.com');
        </CodeBlock>

        <Typography variant="subtitle2">INSERT - From SELECT</Typography>
        <CodeBlock>
          INSERT INTO db.us_customers{'\n'}
          SELECT * FROM db.customers WHERE region = 'US';
        </CodeBlock>

        <Typography variant="subtitle2">DELETE</Typography>
        <CodeBlock>
          DELETE FROM db.customers WHERE created_at {'<'} '2023-01-01';
        </CodeBlock>

        <Chip label="Note" color="warning" size="small" sx={{ mr: 1 }} />
        <Typography variant="caption">
          DML operations are atomic and create new table snapshots. You can use time travel to view previous states.
        </Typography>
      </Section>

      <Section title="File Uploads">
        <Typography paragraph>
          Upload CSV, JSON, or Parquet files to append data to your Iceberg tables.
        </Typography>

        <Typography variant="subtitle2">How to Upload</Typography>
        <Typography paragraph>
          1. Navigate to a table in the Explorer sidebar<br/>
          2. Click the cloud upload icon next to the table name<br/>
          3. Select your file (CSV, JSON, or Parquet)<br/>
          4. Click "Upload"
        </Typography>

        <Typography variant="subtitle2">Supported Formats</Typography>
        <List>
          <ListItem>
            <ListItemText primary="CSV" secondary="Comma-separated values with header row" />
          </ListItem>
          <ListItem>
            <ListItemText primary="JSON" secondary="JSON array or newline-delimited JSON" />
          </ListItem>
          <ListItem>
            <ListItemText primary="Parquet" secondary="Apache Parquet columnar format" />
          </ListItem>
        </List>

        <Chip label="Tip" color="info" size="small" sx={{ mr: 1 }} />
        <Typography variant="caption">
          The file schema must match the table schema. Data is appended atomically.
        </Typography>
      </Section>

      <Section title="Time Travel">
        <Typography paragraph>
          Query historical data using Iceberg's time travel features. You can query by Snapshot ID or Timestamp.
        </Typography>
        
        <Typography variant="subtitle2">By Snapshot ID</Typography>
        <CodeBlock>
          SELECT * FROM db.orders FOR SYSTEM_TIME AS OF SNAPSHOT 123456789;
        </CodeBlock>

        <Typography variant="subtitle2">By Timestamp</Typography>
        <CodeBlock>
          SELECT * FROM db.orders{'\n'}
          FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';
        </CodeBlock>
        
        <Chip label="Tip" color="info" size="small" sx={{ mr: 1 }} />
        <Typography variant="caption">
          Check the "Snapshots" tab in the Metadata Viewer to find Snapshot IDs and Timestamps.
        </Typography>
      </Section>

      <Section title="Metadata Tables">
        <Typography paragraph>
          Inspect table internals by querying metadata tables. Append the metadata type to the table name with a <code>$</code>.
        </Typography>

        <Typography variant="subtitle2">Snapshots</Typography>
        <CodeBlock>
          SELECT * FROM db.orders$snapshots;
        </CodeBlock>

        <Typography variant="subtitle2">Files</Typography>
        <CodeBlock>
          SELECT * FROM db.orders$files;
        </CodeBlock>

        <Typography variant="subtitle2">Statistics</Typography>
        <CodeBlock>
          SELECT * FROM db.orders$stats;
        </CodeBlock>

        <Typography variant="subtitle2">Partitions</Typography>
        <CodeBlock>
          SELECT * FROM db.orders$partitions;
        </CodeBlock>
      </Section>

      <Section title="Creating Tables">
        <Typography paragraph>
          You can create new tables from query results (CTAS - Create Table As Select).
        </Typography>
        <CodeBlock>
          CREATE TABLE db.us_customers AS{'\n'}
          SELECT * FROM db.customers WHERE region = 'US';
        </CodeBlock>

        <Typography variant="subtitle2">With Explicit Schema</Typography>
        <CodeBlock>
          CREATE TABLE db.new_table (id LONG, name STRING, amount DOUBLE);
        </CodeBlock>
      </Section>

      <Section title="Exporting Results">
        <Typography paragraph>
          Export query results to CSV, JSON, or Parquet format for further analysis.
        </Typography>

        <Typography variant="subtitle2">How to Export</Typography>
        <Typography paragraph>
          1. Run your query in the Query Editor<br/>
          2. Click the "Export" button<br/>
          3. Select your desired format (CSV, JSON, or Parquet)<br/>
          4. The file will be downloaded to your browser
        </Typography>
      </Section>

      <Section title="Namespaces">
        <Typography paragraph>
          Tables are organized into namespaces. Always use the fully qualified name <code>namespace.table</code> in your queries.
        </Typography>
        <Typography paragraph>
          For cross-catalog queries, use <code>catalog.namespace.table</code>.
        </Typography>
        <Typography paragraph>
          You can create new namespaces using the "NS" button in the Explorer sidebar.
        </Typography>
      </Section>

      <Section title="Keyboard Shortcuts">
        <Typography paragraph>
          <strong>Ctrl/Cmd + Enter:</strong> Execute query<br/>
          <strong>Ctrl/Cmd + S:</strong> Save query (if implemented)<br/>
          <strong>Esc:</strong> Clear selection
        </Typography>
      </Section>
    </Box>
  );
}

export default Documentation;
