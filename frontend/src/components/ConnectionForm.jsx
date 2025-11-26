import React, { useState } from 'react';
import { Box, TextField, Button, Typography, Paper, Radio, RadioGroup, FormControlLabel, FormControl, FormLabel } from '@mui/material';

function ConnectionForm({ onConnect }) {
  const [uri, setUri] = useState('http://localhost:8181');
  const [authType, setAuthType] = useState('credential'); // 'credential' or 'token'
  const [credential, setCredential] = useState('admin:password');
  const [token, setToken] = useState('');
  const [warehouse, setWarehouse] = useState('s3://warehouse/wh');
  const [catalogName, setCatalogName] = useState('default'); // New state for catalog name
  const [additionalJson, setAdditionalJson] = useState('{\n  "s3.endpoint": "http://localhost:9000",\n  "s3.access-key-id": "admin",\n  "s3.secret-access-key": "password"\n}');

  const handleSubmit = (e) => {
    e.preventDefault();
    const connectionProperties = {
      uri,
      warehouse,
    };

    if (authType === 'credential') {
      connectionProperties.credential = credential;
    } else if (authType === 'token') {
      connectionProperties.token = token;
    }
    // If authType is 'none', we don't add credential or token


    try {
      if (additionalJson) {
        const jsonProps = JSON.parse(additionalJson);
        Object.assign(connectionProperties, jsonProps);
      }
      
      onConnect({
        name: catalogName,
        properties: connectionProperties
      });
    } catch (err) {
      alert("Invalid JSON in additional properties");
    }
  };

  return (
    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
      <Paper sx={{ p: 4, width: '100%', maxWidth: 600 }}>
        <Typography variant="h5" gutterBottom>Connect to Iceberg Catalog</Typography>
        <form onSubmit={handleSubmit}>
          <TextField
            fullWidth
            label="Catalog Name (Alias)"
            value={catalogName}
            onChange={(e) => setCatalogName(e.target.value)}
            margin="normal"
            helperText="Unique name for this catalog connection"
            required
          />
          <TextField
            fullWidth
            label="Catalog URI"
            value={uri}
            onChange={(e) => setUri(e.target.value)}
            margin="normal"
            required
          />
          
          <FormControl component="fieldset" margin="normal">
            <FormLabel component="legend">Authentication Method</FormLabel>
            <RadioGroup row value={authType} onChange={(e) => setAuthType(e.target.value)}>
              <FormControlLabel value="credential" control={<Radio />} label="OAuth2 (Client:Secret)" />
              <FormControlLabel value="token" control={<Radio />} label="Bearer Token" />
              <FormControlLabel value="none" control={<Radio />} label="None (No Auth)" />
            </RadioGroup>
          </FormControl>

          {authType === 'credential' && (
            <TextField
              fullWidth
              label="Credential (client-id:client-secret)"
              value={credential}
              onChange={(e) => setCredential(e.target.value)}
              margin="normal"
              helperText="For OAuth2 Client Credentials flow"
            />
          )}

          {authType === 'token' && (
            <TextField
              fullWidth
              label="Access Token"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              margin="normal"
              helperText="Direct Bearer Token (e.g. PAT)"
            />
          )}

          <TextField
            fullWidth
            label="Warehouse Path"
            value={warehouse}
            onChange={(e) => setWarehouse(e.target.value)}
            margin="normal"
          />
          <TextField
            fullWidth
            label="Additional Properties (JSON)"
            value={additionalJson}
            onChange={(e) => setAdditionalJson(e.target.value)}
            margin="normal"
            multiline
            rows={6}
          />
          <Button type="submit" variant="contained" fullWidth sx={{ mt: 2 }}>
            Connect
          </Button>
        </form>
      </Paper>
    </Box>
  );
}

export default ConnectionForm;
