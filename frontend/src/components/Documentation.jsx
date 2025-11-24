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
        </List>
      </Section>

      <Section title="Time Travel">
        <Typography paragraph>
          Query historical data using Iceberg's time travel features. You can query by Snapshot ID or Timestamp.
        </Typography>
        
        <Typography variant="subtitle2">By Snapshot ID</Typography>
        <CodeBlock>
          SELECT * FROM db.orders FOR SYSTEM_TIME AS OF SNAPSHOT 123456789;
        </CodeBlock>

        <Typography variant="subtitle2">By Timestamp (Milliseconds)</Typography>
        <CodeBlock>
          SELECT * FROM db.orders FOR SYSTEM_TIME AS OF TIMESTAMP 1678886400000;
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
      </Section>

      <Section title="Creating Tables">
        <Typography paragraph>
          You can create new tables from query results (CTAS - Create Table As Select).
        </Typography>
        <CodeBlock>
          CREATE TABLE db.us_customers AS SELECT * FROM db.customers WHERE region = 'US';
        </CodeBlock>
      </Section>

      <Section title="Namespaces">
        <Typography paragraph>
          Tables are organized into namespaces. Always use the fully qualified name <code>namespace.table</code> in your queries.
        </Typography>
        <Typography paragraph>
          You can create new namespaces using the "NS" button in the Explorer sidebar.
        </Typography>
      </Section>
    </Box>
  );
}

export default Documentation;
