import React, { useState, useEffect } from 'react';
import { Box, Tabs, Tab, Typography, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Button, TextField } from '@mui/material';
import api from '../api';

function MetadataViewer({ namespace, table }) {
  const [tab, setTab] = useState(0);
  const [metadata, setMetadata] = useState(null);
  const [loading, setLoading] = useState(false);
  
  // Maintenance state
  const [olderThan, setOlderThan] = useState('');

  useEffect(() => {
    fetchMetadata();
  }, [namespace, table]);

  const fetchMetadata = async () => {
    setLoading(true);
    try {
      const res = await api.get(`/tables/${namespace}/${table}/metadata`);
      setMetadata(res.data);
    } catch (err) {
      console.error("Failed to fetch metadata", err);
    } finally {
      setLoading(false);
    }
  };

  const handleExpireSnapshots = async () => {
    try {
      const payload = olderThan ? { older_than_ms: parseInt(olderThan) } : {};
      await api.post(`/tables/${namespace}/${table}/maintenance`, payload);
      alert("Maintenance triggered successfully");
      fetchMetadata(); // Refresh to see if snapshots changed
    } catch (err) {
      alert("Maintenance failed: " + err.message);
    }
  };

  if (loading) return <Typography>Loading metadata...</Typography>;
  if (!metadata) return <Typography>No metadata available</Typography>;

  return (
    <Box sx={{ width: '100%' }}>
      <Typography variant="h5" gutterBottom>{namespace}.{table}</Typography>
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)}>
          <Tab label="Schema" />
          <Tab label="Snapshots" />
          <Tab label="Properties" />
          <Tab label="Maintenance" />
        </Tabs>
      </Box>

      {/* Schema Tab */}
      <TabPanel value={tab} index={0}>
        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Name</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Required</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {metadata.schema.fields.map((field) => (
                <TableRow key={field.id}>
                  <TableCell>{field.id}</TableCell>
                  <TableCell>{field.name}</TableCell>
                  <TableCell>{String(field.type)}</TableCell>
                  <TableCell>{field.required ? 'Yes' : 'No'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Snapshots Tab */}
      <TabPanel value={tab} index={1}>
        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Operation</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {metadata.snapshots.map((snap) => (
                <TableRow key={snap['snapshot-id']} selected={snap['snapshot-id'] === metadata.current_snapshot_id}>
                  <TableCell>{snap['snapshot-id']}</TableCell>
                  <TableCell>{new Date(snap['timestamp-ms']).toLocaleString()}</TableCell>
                  <TableCell>{snap.summary?.operation || 'N/A'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Properties Tab */}
      <TabPanel value={tab} index={2}>
        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Key</TableCell>
                <TableCell>Value</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {Object.entries(metadata.properties).map(([k, v]) => (
                <TableRow key={k}>
                  <TableCell>{k}</TableCell>
                  <TableCell>{v}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Maintenance Tab */}
      <TabPanel value={tab} index={3}>
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>Expire Snapshots</Typography>
          <Typography variant="body2" paragraph>
            Remove old snapshots to free up space.
          </Typography>
          <TextField
            label="Older Than (Timestamp MS)"
            value={olderThan}
            onChange={(e) => setOlderThan(e.target.value)}
            helperText="Leave empty to keep only current state (depending on policy)"
            fullWidth
            margin="normal"
          />
          <Button variant="contained" color="warning" onClick={handleExpireSnapshots}>
            Expire Snapshots
          </Button>
        </Paper>
      </TabPanel>
    </Box>
  );
}

function TabPanel(props) {
  const { children, value, index, ...other } = props;
  return (
    <div role="tabpanel" hidden={value !== index} {...other}>
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

export default MetadataViewer;
