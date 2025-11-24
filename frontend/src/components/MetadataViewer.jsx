import React, { useState, useEffect } from 'react';
import { Box, Tabs, Tab, Typography, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Button, TextField, Alert } from '@mui/material';
import api from '../api';
import MaintenanceControls from './MaintenanceControls';
import SchemaEditor from './SchemaEditor';
import MetadataCharts from './MetadataCharts';

function MetadataViewer({ namespace, table }) {
  const [tab, setTab] = useState(0);
  const [metadata, setMetadata] = useState(null);
  const [loading, setLoading] = useState(false);
  
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
          <Tab label="Visualization" />
        </Tabs>
      </Box>

      {/* Schema Tab */}
      <TabPanel value={tab} index={0}>
        <SchemaEditor 
            namespace={namespace} 
            table={table} 
            schema={metadata.schema} 
            onRefresh={fetchMetadata} 
        />
      </TabPanel>

      {/* Snapshots Tab */}
      <TabPanel value={tab} index={1}>
        <Alert severity="info" sx={{ mb: 2 }}>
         <strong>Time Travel:</strong> Copy a Snapshot ID to query historical data. 
          Use syntax: <code>SELECT * FROM {namespace}.{table} FOR SYSTEM_TIME AS OF SNAPSHOT &lt;ID&gt;</code>
        </Alert>
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
        <MaintenanceControls namespace={namespace} table={table} />
      </TabPanel>

      {/* Visualization Tab */}
      <TabPanel value={tab} index={4}>
        <MetadataCharts namespace={namespace} table={table} />
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
