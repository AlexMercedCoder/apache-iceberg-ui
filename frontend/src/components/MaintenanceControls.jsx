import React, { useState } from 'react';
import { Box, Button, FormControl, InputLabel, MenuItem, Select, Typography, Alert, CircularProgress, Card, CardContent } from '@mui/material';
import { DeleteSweep } from '@mui/icons-material';
import api from '../api';

function MaintenanceControls({ namespace, table }) {
  const [retention, setRetention] = useState('none');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);
  const [error, setError] = useState(null);

  const handleExpire = async () => {
    setLoading(true);
    setMessage(null);
    setError(null);

    let olderThanMs = null;
    const now = Date.now();
    
    if (retention === '1h') olderThanMs = now - (60 * 60 * 1000);
    else if (retention === '1d') olderThanMs = now - (24 * 60 * 60 * 1000);
    else if (retention === '7d') olderThanMs = now - (7 * 24 * 60 * 60 * 1000);
    else if (retention === '30d') olderThanMs = now - (30 * 24 * 60 * 60 * 1000);

    try {
      await api.post(`/tables/${namespace}/${table}/maintenance`, {
        older_than_ms: olderThanMs
      });
      setMessage("Snapshot expiration completed successfully.");
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card variant="outlined">
      <CardContent>
        <Typography variant="h6" gutterBottom display="flex" alignItems="center" gap={1}>
          <DeleteSweep /> Maintenance Operations
        </Typography>
        
        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle1">Expire Snapshots</Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            Remove old snapshots to free up space. This action is irreversible.
          </Typography>
          
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', mt: 2 }}>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Retention Policy</InputLabel>
              <Select
                value={retention}
                label="Retention Policy"
                onChange={(e) => setRetention(e.target.value)}
              >
                <MenuItem value="none">Keep Only Current (Aggressive)</MenuItem>
                <MenuItem value="1h">Older than 1 Hour</MenuItem>
                <MenuItem value="1d">Older than 1 Day</MenuItem>
                <MenuItem value="7d">Older than 1 Week</MenuItem>
                <MenuItem value="30d">Older than 30 Days</MenuItem>
              </Select>
            </FormControl>
            
            <Button 
              variant="contained" 
              color="warning" 
              onClick={handleExpire}
              disabled={loading}
            >
              {loading ? <CircularProgress size={24} /> : "Run Expiration"}
            </Button>
          </Box>
          
          {message && <Alert severity="success" sx={{ mt: 2 }}>{message}</Alert>}
          {error && <Alert severity="error" sx={{ mt: 2 }}>{error}</Alert>}
        </Box>
      </CardContent>
    </Card>
  );
}

export default MaintenanceControls;
