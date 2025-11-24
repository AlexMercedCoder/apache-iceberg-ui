import React, { useState } from 'react';
import { Box, Button, FormControl, InputLabel, MenuItem, Select, Typography, Alert, CircularProgress, Card, CardContent, TextField } from '@mui/material';
import { DeleteSweep } from '@mui/icons-material';
import api from '../api';

function MaintenanceControls({ catalog, namespace, table }) {
  const [olderThan, setOlderThan] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  const handleExpire = async () => {
    setLoading(true);
    setMessage('');
    setError('');

    try {
      const body = olderThan ? { older_than_ms: new Date(olderThan).getTime() } : {};
      await api.post(`/catalogs/${catalog}/tables/${namespace}/${table}/maintenance`, body);
      setMessage('Snapshots expired successfully');
    } catch (err) {
      setError('Failed to expire snapshots');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>Table Maintenance</Typography>
      
      <Card variant="outlined" sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="subtitle1" gutterBottom>Expire Snapshots</Typography>
          <Typography variant="body2" color="text.secondary" paragraph>
            Remove old snapshots to free up space.
          </Typography>
          
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-start' }}>
            <TextField
                label="Expire older than (optional)"
                type="datetime-local"
                value={olderThan}
                onChange={(e) => setOlderThan(e.target.value)}
                InputLabelProps={{ shrink: true }}
                helperText="Leave empty to keep only current snapshot"
            />
            
            <Button 
              variant="contained" 
              color="warning" 
              startIcon={<DeleteSweep />}
              onClick={handleExpire}
              disabled={loading}
            >
              {loading ? <CircularProgress size={24} /> : 'Expire Snapshots'}
            </Button>
          </Box>
          
          {message && <Alert severity="success" sx={{ mt: 2 }}>{message}</Alert>}
          {error && <Alert severity="error" sx={{ mt: 2 }}>{error}</Alert>}
        </CardContent>
      </Card>
    </Box>
  );
}

export default MaintenanceControls;
