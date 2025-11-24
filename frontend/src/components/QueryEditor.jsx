import React, { useState, useEffect } from 'react';
import { Box, TextField, Button, Paper, Typography, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, CircularProgress, Alert, Collapse, IconButton } from '@mui/material';
import { PlayArrow, Info, ExpandMore, ExpandLess } from '@mui/icons-material';
import api from '../api';

function QueryEditor({ initialNamespace, initialSql }) {
  const [sql, setSql] = useState('SELECT * FROM ');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showHelp, setShowHelp] = useState(false);

  useEffect(() => {
    if (initialSql) {
      setSql(initialSql);
    }
  }, [initialSql]);

  const handleRun = async () => {
    setLoading(true);
    setError(null);
    setResults([]);
    try {
      const res = await api.post('/query', { 
        sql, 
        namespace: initialNamespace // Optional context
      });
      setResults(res.data.results);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleTemplate = (type) => {
    if (type === 'create') {
      setSql(`CREATE TABLE ${initialNamespace ? initialNamespace + '.' : ''}new_table (
    id INT,
    data STRING
) USING iceberg;`);
    }
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Paper sx={{ p: 2 }}>
        <Box sx={{ mb: 1, display: 'flex', gap: 1, alignItems: 'center' }}>
          <Button variant="contained" startIcon={<PlayArrow />} onClick={handleRun} disabled={loading}>
            Run
          </Button>
          <Button variant="outlined" onClick={() => handleTemplate('create')}>
            Create Table Template
          </Button>
          <IconButton 
            size="small" 
            onClick={() => setShowHelp(!showHelp)}
            sx={{ ml: 'auto' }}
            title="Show query help"
          >
            {showHelp ? <ExpandLess /> : <ExpandMore />}
          </IconButton>
        </Box>
        
        <Collapse in={showHelp}>
          <Alert severity="info" sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom><strong>Supported Queries:</strong></Typography>
            <Typography variant="body2" component="div">
              <strong>✓ SELECT</strong> - Read data from tables<br />
              <em>Basic: SELECT * FROM namespace.table LIMIT 10</em><br />
              <em>Optimized: SELECT name, email FROM db.customers WHERE id {'>'} 100</em>
            </Typography>
            <Typography variant="body2" component="div" sx={{ mt: 1 }}>
              <strong>✓ CREATE TABLE</strong> - Create new tables<br />
              <em>Example: CREATE TABLE db.mytable (id INT, name STRING) USING iceberg</em>
            </Typography>
            <Typography variant="body2" component="div" sx={{ mt: 1 }}>
              <strong>✓ Time Travel</strong> - Query historical data<br />
              <em>By Snapshot: SELECT * FROM db.customers FOR SYSTEM_TIME AS OF SNAPSHOT 12345</em><br />
              <em>By Timestamp: SELECT * FROM db.customers FOR SYSTEM_TIME AS OF TIMESTAMP 1700000000000</em>
            </Typography>
            <Typography variant="body2" component="div" sx={{ mt: 1 }}>
              <strong>✓ Metadata Tables</strong> - Query table metadata<br />
              <em>Snapshots: SELECT * FROM db.customers$snapshots</em><br />
              <em>Statistics: SELECT * FROM db.customers$stats</em><br />
              <em>Files: SELECT * FROM db.customers$files</em>
            </Typography>
            <Typography variant="subtitle2" sx={{ mt: 2 }} gutterBottom><strong>Performance Optimizations:</strong></Typography>
            <Typography variant="body2" component="div">
              • <strong>Predicate Pushdown:</strong> WHERE clauses filter data at storage level (huge perf boost!)<br />
              • <strong>Column Projection:</strong> SELECT specific columns reads only those columns<br />
              • <strong>Partition Pruning:</strong> Automatically skips irrelevant partitions
            </Typography>
            <Typography variant="subtitle2" sx={{ mt: 2 }} gutterBottom><strong>Not Supported:</strong></Typography>
            <Typography variant="body2">
              ✗ INSERT, UPDATE, DELETE, DROP, ALTER
            </Typography>
            <Typography variant="caption" display="block" sx={{ mt: 1 }}>
              <strong>Tip:</strong> Always use namespace-qualified names (e.g., <code>db.customers</code>). 
              Check the Snapshots tab in metadata view to find snapshot IDs for time travel.
            </Typography>
          </Alert>
        </Collapse>
        <TextField
          fullWidth
          multiline
          rows={6}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          placeholder="Enter SQL query..."
          sx={{ fontFamily: 'monospace' }}
        />
      </Paper>

      <Paper sx={{ flexGrow: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        {loading && <Box sx={{ p: 2, display: 'flex', justifyContent: 'center' }}><CircularProgress /></Box>}
        {error && <Typography color="error" sx={{ p: 2 }}>{error}</Typography>}
        
        {!loading && !error && results.length > 0 && (
          <TableContainer sx={{ flexGrow: 1 }}>
            <Table stickyHeader size="small">
              <TableHead>
                <TableRow>
                  {Object.keys(results[0]).map((key) => (
                    <TableCell key={key}>{key}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {results.map((row, i) => (
                  <TableRow key={i}>
                    {Object.values(row).map((val, j) => (
                      <TableCell key={j}>{String(val)}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
        {!loading && !error && results.length === 0 && (
          <Typography sx={{ p: 2, color: 'text.secondary' }}>No results</Typography>
        )}
      </Paper>
    </Box>
  );
}

export default QueryEditor;
