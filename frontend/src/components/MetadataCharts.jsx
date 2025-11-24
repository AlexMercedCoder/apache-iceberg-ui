import React, { useEffect, useState } from 'react';
import { Box, Typography, Paper, Grid, CircularProgress, Alert } from '@mui/material';
import api from '../api';

function MetadataCharts({ namespace, table }) {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true);
      try {
        const res = await api.get(`/tables/${namespace}/${table}/stats`);
        setStats(res.data);
      } catch (err) {
        setError("Failed to load stats");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };
    fetchStats();
  }, [namespace, table]);

  if (loading) return <CircularProgress />;
  if (error) return <Alert severity="error">{error}</Alert>;
  if (!stats) return null;

  // Process data for charts
  const fileSizes = stats.files.map(f => f.file_size_in_bytes);
  const totalSize = fileSizes.reduce((a, b) => a + b, 0);
  const avgSize = totalSize / (fileSizes.length || 1);
  
  // Simple histogram for file sizes
  const maxFileSize = Math.max(...fileSizes, 0);
  
  // Partition stats (assuming 'partition' column exists and is a struct or similar)
  // The $partitions table structure depends on partitioning. 
  // Often it has 'partition' column.
  const partitionCounts = stats.partitions.map(p => ({
      label: JSON.stringify(p.partition || 'Unpartitioned'),
      count: p.record_count || 0
  }));

  return (
    <Box>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>File Statistics</Typography>
            <Typography variant="body2">Total Files: {stats.files.length}</Typography>
            <Typography variant="body2">Avg Size: {(avgSize / 1024 / 1024).toFixed(2)} MB</Typography>
            <Box sx={{ mt: 2, height: 200, display: 'flex', alignItems: 'flex-end', gap: 1, overflowX: 'auto' }}>
                {stats.files.slice(0, 50).map((f, i) => (
                    <Box 
                        key={i} 
                        title={`${(f.file_size_in_bytes / 1024).toFixed(1)} KB`}
                        sx={{ 
                            width: 10, 
                            height: `${(f.file_size_in_bytes / (maxFileSize || 1)) * 100}%`, 
                            bgcolor: 'primary.main',
                            minHeight: 2
                        }} 
                    />
                ))}
            </Box>
            <Typography variant="caption" display="block" align="center">File Size Distribution (First 50)</Typography>
          </Paper>
        </Grid>

        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>Partition Record Counts</Typography>
            {partitionCounts.length === 0 ? (
                <Typography variant="body2" color="text.secondary">No partition data available</Typography>
            ) : (
                <Box sx={{ mt: 2 }}>
                    {partitionCounts.slice(0, 10).map((p, i) => (
                        <Box key={i} sx={{ mb: 1 }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                                <Typography variant="caption" noWrap sx={{ maxWidth: '70%' }}>{p.label}</Typography>
                                <Typography variant="caption">{p.count}</Typography>
                            </Box>
                            <Box sx={{ width: '100%', bgcolor: 'grey.200', height: 8, borderRadius: 1 }}>
                                <Box sx={{ 
                                    width: `${Math.min((p.count / (Math.max(...partitionCounts.map(x=>x.count)) || 1)) * 100, 100)}%`, 
                                    bgcolor: 'secondary.main', 
                                    height: '100%', 
                                    borderRadius: 1 
                                }} />
                            </Box>
                        </Box>
                    ))}
                </Box>
            )}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
}

export default MetadataCharts;
