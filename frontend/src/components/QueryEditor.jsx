import React, { useState, useEffect } from 'react';
import { Box, TextField, Button, Paper, Typography, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, CircularProgress, Alert, Collapse, IconButton, Drawer, List, ListItem, ListItemText, ListItemSecondaryAction, Divider, Tabs, Tab } from '@mui/material';
import { PlayArrow, Info, ExpandMore, ExpandLess, History, Bookmark, BookmarkBorder, Delete, Save } from '@mui/icons-material';
import api from '../api';

function QueryEditor({ initialNamespace, initialSql }) {
  const [sql, setSql] = useState('SELECT * FROM ');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showHelp, setShowHelp] = useState(false);
  
  // History & Saved State
  const [history, setHistory] = useState([]);
  const [savedQueries, setSavedQueries] = useState([]);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerTab, setDrawerTab] = useState(0);

  useEffect(() => {
    if (initialSql) {
      setSql(initialSql);
    }
    // Load from localStorage
    const storedHistory = JSON.parse(localStorage.getItem('queryHistory') || '[]');
    setHistory(storedHistory);
  }, [initialSql]);

  // Load history/saved from local storage
  useEffect(() => {
      const storedHistory = localStorage.getItem('queryHistory');
      if (storedHistory) setHistory(JSON.parse(storedHistory));
      
      const storedSaved = localStorage.getItem('savedQueries');
      if (storedSaved) setSavedQueries(JSON.parse(storedSaved));
  }, []);

  const addToHistory = (query) => {
      const newHistory = [{ sql: query, timestamp: Date.now() }, ...history].slice(0, 50);
      setHistory(newHistory);
      localStorage.setItem('queryHistory', JSON.stringify(newHistory));
  };

  const saveQuery = () => {
      const name = prompt("Enter a name for this query:");
      if (name) {
          const newSaved = [...savedQueries, { name, sql, timestamp: Date.now() }];
          setSavedQueries(newSaved);
          localStorage.setItem('savedQueries', JSON.stringify(newSaved));
      }
  };

  const deleteSaved = (index) => {
      const newSaved = savedQueries.filter((_, i) => i !== index);
      setSavedQueries(newSaved);
      localStorage.setItem('savedQueries', JSON.stringify(newSaved));
  };

  const handleRun = async () => {
    setLoading(true);
    setError(null);
    setResults([]);
    try {
      const res = await api.post('/query', { 
        sql, 
        namespace: initialNamespace, // Optional context
        catalog
      });
      setResults(res.data.data);
      addToHistory(sql);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async (format) => {
    setLoading(true);
    try {
      const res = await api.post('/query/export', { 
        sql, 
        format,
        catalog
      }, {
        responseType: 'blob'
      });
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([res.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `export.${format}`);
      document.body.appendChild(link);
      link.click();
      link.remove();
    } catch (err) {
      setError("Export failed: " + (err.response?.data?.detail || err.message));
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
    } else if (type === 'select') {
        setSql(`SELECT * FROM ${initialNamespace ? initialNamespace + '.' : ''}table_name LIMIT 10;`);
    }
  };

  const handleDrop = (e) => {
      e.preventDefault();
      const tableName = e.dataTransfer.getData("text/plain");
      if (tableName) {
          setSql(prev => prev + tableName);
      }
  };

  const handleDragOver = (e) => {
      e.preventDefault();
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', gap: 2 }}>
      <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Paper sx={{ p: 2 }}>
        <Box sx={{ mb: 1, display: 'flex', gap: 1, alignItems: 'center' }}>
          <Button variant="contained" startIcon={<PlayArrow />} onClick={handleRun} disabled={loading}>
            Run
          </Button>
          <Button variant="outlined" startIcon={<Save />} onClick={saveQuery} disabled={!sql}>
            Save
          </Button>
          <Button variant="outlined" onClick={() => handleTemplate('select')}>
            Select *
          </Button>
          <Button variant="outlined" onClick={() => handleTemplate('create')}>
            Template
          </Button>
          <Button variant="outlined" startIcon={<History />} onClick={() => setDrawerOpen(true)}>
            History
          </Button>
          
          {/* Export Buttons */}
          <Button variant="text" size="small" onClick={() => handleExport('csv')} disabled={loading || !sql}>
            CSV
          </Button>
          <Button variant="text" size="small" onClick={() => handleExport('json')} disabled={loading || !sql}>
            JSON
          </Button>
          <Button variant="text" size="small" onClick={() => handleExport('parquet')} disabled={loading || !sql}>
            Parquet
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
          placeholder="Enter SQL query... (Drag tables here)"
          sx={{ fontFamily: 'monospace' }}
          onDrop={handleDrop}
          onDragOver={handleDragOver}
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

      <Drawer anchor="right" open={drawerOpen} onClose={() => setDrawerOpen(false)}>
          <Box sx={{ width: 350, p: 2 }}>
              <Typography variant="h6" gutterBottom>Query Manager</Typography>
              <Tabs value={drawerTab} onChange={(e, v) => setDrawerTab(v)} sx={{ mb: 2 }}>
                  <Tab label="History" />
                  <Tab label="Saved" />
              </Tabs>
              
              {drawerTab === 0 && (
                  <List>
                      {history.map((item, i) => (
                          <React.Fragment key={i}>
                              <ListItem button onClick={() => { setSql(item.sql); setDrawerOpen(false); }}>
                                  <ListItemText 
                                    primary={item.sql} 
                                    secondary={new Date(item.timestamp).toLocaleString()} 
                                    primaryTypographyProps={{ noWrap: true, style: { fontFamily: 'monospace' } }}
                                  />
                              </ListItem>
                              <Divider />
                          </React.Fragment>
                      ))}
                      {history.length === 0 && <Typography variant="body2" color="text.secondary">No history yet.</Typography>}
                  </List>
              )}

              {drawerTab === 1 && (
                  <List>
                      {savedQueries.map((item, i) => (
                          <React.Fragment key={i}>
                              <ListItem button onClick={() => { setSql(item.sql); setDrawerOpen(false); }}>
                                  <ListItemText 
                                    primary={item.name} 
                                    secondary={item.sql}
                                    secondaryTypographyProps={{ noWrap: true, style: { fontFamily: 'monospace' } }}
                                  />
                                  <ListItemSecondaryAction>
                                      <IconButton edge="end" onClick={() => deleteSaved(i)}>
                                          <Delete />
                                      </IconButton>
                                  </ListItemSecondaryAction>
                              </ListItem>
                              <Divider />
                          </React.Fragment>
                      ))}
                      {savedQueries.length === 0 && <Typography variant="body2" color="text.secondary">No saved queries.</Typography>}
                  </List>
              )}
          </Box>
      </Drawer>
    </Box>
  );
}

export default QueryEditor;
