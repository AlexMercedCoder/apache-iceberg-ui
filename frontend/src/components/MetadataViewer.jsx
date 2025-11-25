import React, { useState, useEffect } from 'react';
import { Box, Typography, Tabs, Tab, Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, CircularProgress } from '@mui/material';
import api from '../api';
import MaintenanceControls from './MaintenanceControls';
import SchemaEditor from './SchemaEditor';
import MetadataCharts from './MetadataCharts';

function TabPanel({ children, value, index, ...other }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

function MetadataViewer({ catalog, namespace, table }) {
  const [metadata, setMetadata] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tab, setTab] = useState(0);

  useEffect(() => {
    if (namespace && table) {
      fetchMetadata();
    }
  }, [catalog, namespace, table]);

  const fetchMetadata = async () => {
    setLoading(true);
    try {
      const res = await api.get(`/catalogs/${catalog}/tables/${namespace}/${table}/metadata`);
      setMetadata(res.data);
    } catch (err) {
      console.error("Failed to fetch metadata", err);
    } finally {
      setLoading(false);
    }
  };

  if (loading) return <CircularProgress />;
  if (!metadata) return <Typography>No metadata available</Typography>;

  return (
    <Box sx={{ width: '100%' }}>
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)}>
          <Tab label="Overview" />
          <Tab label="Schema" />
          <Tab label="Snapshots" />
          <Tab label="Maintenance" />
          <Tab label="Visualization" />
        </Tabs>
      </Box>
      
      <TabPanel value={tab} index={0}>
        <Typography variant="h6">Table Properties</Typography>
        <TableContainer component={Paper} sx={{ mt: 2 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Property</TableCell>
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

      <TabPanel value={tab} index={1}>
        <SchemaEditor 
            catalog={catalog}
            namespace={namespace} 
            table={table} 
            schema={metadata.schema} 
            onUpdate={fetchMetadata} 
        />
      </TabPanel>

      <TabPanel value={tab} index={2}>
        <Typography variant="h6">Snapshots</Typography>
        <TableContainer component={Paper} sx={{ mt: 2 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Operation</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {metadata.snapshots.map((s) => (
                <TableRow key={s.snapshot_id}>
                  <TableCell>{s.snapshot_id || s['snapshot-id']}</TableCell>
                  <TableCell>{(s.timestamp_ms || s['timestamp-ms']) ? new Date(s.timestamp_ms || s['timestamp-ms']).toLocaleString() : 'N/A'}</TableCell>
                  <TableCell>{s.summary?.operation || 'N/A'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Maintenance Tab */}
      <TabPanel value={tab} index={3}>
        <MaintenanceControls catalog={catalog} namespace={namespace} table={table} />
      </TabPanel>

      {/* Visualization Tab */}
      <TabPanel value={tab} index={4}>
        <MetadataCharts catalog={catalog} namespace={namespace} table={table} />
      </TabPanel>
    </Box>
  );
}

export default MetadataViewer;
