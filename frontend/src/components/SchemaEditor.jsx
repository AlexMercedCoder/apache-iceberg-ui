import React, { useState } from 'react';
import { 
    Box, Button, Typography, Table, TableBody, TableCell, TableContainer, 
    TableHead, TableRow, Paper, IconButton, Dialog, DialogTitle, 
    DialogContent, DialogActions, TextField, Select, MenuItem, 
    FormControl, InputLabel, Checkbox, FormControlLabel, Alert 
} from '@mui/material';
import { Add, Delete, Edit, Save, Cancel } from '@mui/icons-material';
import api from '../api';

function SchemaEditor({ catalog, namespace, table, schema, onUpdate }) {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [action, setAction] = useState(null); // 'add', 'drop', 'rename', 'update'
  const [formData, setFormData] = useState({});
  const [error, setError] = useState(null);

  const handleAction = async () => {
    try {
      let endpoint = `/catalogs/${catalog}/tables/${namespace}/${table}/schema/${action}`;
      // Map 'update_type' action to 'update' endpoint if needed, or adjust backend
      // Backend expects: /schema/update for type updates
      if (action === 'update_type') endpoint = `/catalogs/${catalog}/tables/${namespace}/${table}/schema/update`;
      
      await api.post(endpoint, formData);
      onUpdate();
      setDialogOpen(false);
      setFormData({});
      setError(null);
    } catch (err) {
      console.error("Schema update failed", err);
      setError(err.response?.data?.detail || "Failed to update schema");
    }
  };

  const openAddDialog = () => {
    setAction('add');
    setFormData({ name: '', type: 'string', required: false });
    setDialogOpen(true);
  };

  const openRenameDialog = (field) => {
    setAction('rename');
    setFormData({ name: field.name, new_name: field.name });
    setDialogOpen(true);
  };

  const openUpdateTypeDialog = (field) => {
    setAction('update_type');
    setFormData({ name: field.name, new_type: field.type });
    setDialogOpen(true);
  };

  const handleDrop = async (colName) => {
    if (!window.confirm(`Are you sure you want to drop column '${colName}'?`)) return;
    try {
      await api.post(`/catalogs/${catalog}/tables/${namespace}/${table}/schema/drop`, { name: colName });
      onUpdate();
    } catch (err) {
      console.error("Drop failed", err);
      alert("Failed to drop column");
    }
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">Schema</Typography>
        <Button startIcon={<Add />} variant="contained" size="small" onClick={openAddDialog}>
          Add Column
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <TableContainer component={Paper}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Required</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {schema.fields.map((field) => (
              <TableRow key={field.id}>
                <TableCell>{field.id}</TableCell>
                <TableCell>{field.name}</TableCell>
                <TableCell>{field.type}</TableCell>
                <TableCell>{field.required ? 'Yes' : 'No'}</TableCell>
                <TableCell>
                  <IconButton size="small" onClick={() => openRenameDialog(field)} title="Rename">
                    <Edit fontSize="small" />
                  </IconButton>
                  <IconButton size="small" onClick={() => openUpdateTypeDialog(field)} title="Change Type">
                    <Save fontSize="small" /> 
                  </IconButton>
                  <IconButton size="small" color="error" onClick={() => handleDrop(field.name)} title="Drop">
                    <Delete fontSize="small" />
                  </IconButton>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)}>
        <DialogTitle>
            {action === 'add' && 'Add Column'}
            {action === 'rename' && 'Rename Column'}
            {action === 'update_type' && 'Update Column Type'}
        </DialogTitle>
        <DialogContent sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2, minWidth: 300 }}>
            {action === 'add' && (
                <>
                    <TextField 
                        label="Name" 
                        fullWidth 
                        value={formData.name} 
                        onChange={(e) => setFormData({...formData, name: e.target.value})} 
                    />
                    <FormControl fullWidth>
                        <InputLabel>Type</InputLabel>
                        <Select 
                            value={formData.type} 
                            label="Type"
                            onChange={(e) => setFormData({...formData, type: e.target.value})}
                        >
                            {['string', 'int', 'long', 'float', 'double', 'boolean', 'date', 'timestamp'].map(t => (
                                <MenuItem key={t} value={t}>{t}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                    <FormControlLabel
                        control={
                            <Checkbox 
                                checked={formData.required} 
                                onChange={(e) => setFormData({...formData, required: e.target.checked})} 
                            />
                        }
                        label="Required"
                    />
                </>
            )}

            {action === 'rename' && (
                <>
                    <Typography variant="body2">Current Name: {formData.name}</Typography>
                    <TextField 
                        label="New Name" 
                        fullWidth 
                        value={formData.new_name} 
                        onChange={(e) => setFormData({...formData, new_name: e.target.value})} 
                    />
                </>
            )}

            {action === 'update_type' && (
                <>
                    <Typography variant="body2">Column: {formData.name}</Typography>
                     <FormControl fullWidth>
                        <InputLabel>New Type</InputLabel>
                        <Select 
                            value={formData.new_type} 
                            label="New Type"
                            onChange={(e) => setFormData({...formData, new_type: e.target.value})}
                        >
                            {['string', 'int', 'long', 'float', 'double', 'boolean', 'date', 'timestamp'].map(t => (
                                <MenuItem key={t} value={t}>{t}</MenuItem>
                            ))}
                        </Select>
                    </FormControl>
                </>
            )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleAction} variant="contained">Save</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}

export default SchemaEditor;
