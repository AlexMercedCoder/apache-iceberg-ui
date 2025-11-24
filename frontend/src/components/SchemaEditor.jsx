import React, { useState } from 'react';
import { Box, Button, TextField, Select, MenuItem, FormControl, InputLabel, IconButton, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Dialog, DialogTitle, DialogContent, DialogActions, Alert, Checkbox, FormControlLabel } from '@mui/material';
import { Add, Delete, Edit, Save, Cancel } from '@mui/icons-material';
import api from '../api';

function SchemaEditor({ namespace, table, schema, onRefresh }) {
  const [openAdd, setOpenAdd] = useState(false);
  const [newCol, setNewCol] = useState({ name: '', type: 'string', required: false });
  const [editingCol, setEditingCol] = useState(null); // { name: 'oldName', newName: 'newName' }
  const [error, setError] = useState(null);

  const handleAdd = async () => {
    try {
      await api.post(`/tables/${namespace}/${table}/schema/add`, newCol);
      setOpenAdd(false);
      setNewCol({ name: '', type: 'string', required: false });
      onRefresh();
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    }
  };

  const handleDrop = async (colName) => {
    if (!window.confirm(`Are you sure you want to drop column '${colName}'?`)) return;
    try {
      await api.post(`/tables/${namespace}/${table}/schema/drop`, { name: colName });
      onRefresh();
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    }
  };

  const handleRename = async () => {
    try {
      await api.post(`/tables/${namespace}/${table}/schema/rename`, { 
        name: editingCol.name, 
        new_name: editingCol.newName 
      });
      setEditingCol(null);
      onRefresh();
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    }
  };

  const handleUpdateType = async (colName, newType) => {
      // Note: Iceberg only allows promoting types (e.g. int -> long), not arbitrary changes.
      try {
        await api.post(`/tables/${namespace}/${table}/schema/update`, {
            name: colName,
            new_type: newType
        });
        onRefresh();
      } catch (err) {
          setError(err.response?.data?.detail || err.message);
      }
  };

  return (
    <Box>
      <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Button startIcon={<Add />} variant="contained" onClick={() => setOpenAdd(true)}>
          Add Column
        </Button>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert>}

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
                <TableCell>
                  {editingCol?.name === field.name ? (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <TextField 
                        size="small" 
                        value={editingCol.newName} 
                        onChange={(e) => setEditingCol({ ...editingCol, newName: e.target.value })}
                      />
                      <IconButton size="small" onClick={handleRename} color="primary"><Save /></IconButton>
                      <IconButton size="small" onClick={() => setEditingCol(null)}><Cancel /></IconButton>
                    </Box>
                  ) : (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {field.name}
                      <IconButton size="small" onClick={() => setEditingCol({ name: field.name, newName: field.name })}>
                        <Edit fontSize="small" />
                      </IconButton>
                    </Box>
                  )}
                </TableCell>
                <TableCell>
                    {/* Allow type update via select? Or just display? Iceberg type evolution is limited. */}
                    {/* For simplicity, let's just display it, but maybe allow update if user really wants to try */}
                    {String(field.type)}
                </TableCell>
                <TableCell>{field.required ? 'Yes' : 'No'}</TableCell>
                <TableCell>
                  <IconButton size="small" color="error" onClick={() => handleDrop(field.name)}>
                    <Delete fontSize="small" />
                  </IconButton>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      {/* Add Column Dialog */}
      <Dialog open={openAdd} onClose={() => setOpenAdd(false)}>
        <DialogTitle>Add New Column</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 1, minWidth: 300 }}>
            <TextField 
              label="Column Name" 
              value={newCol.name} 
              onChange={(e) => setNewCol({ ...newCol, name: e.target.value })}
              fullWidth
            />
            <FormControl fullWidth>
              <InputLabel>Type</InputLabel>
              <Select
                value={newCol.type}
                label="Type"
                onChange={(e) => setNewCol({ ...newCol, type: e.target.value })}
              >
                <MenuItem value="string">String</MenuItem>
                <MenuItem value="int">Integer</MenuItem>
                <MenuItem value="long">Long</MenuItem>
                <MenuItem value="float">Float</MenuItem>
                <MenuItem value="double">Double</MenuItem>
                <MenuItem value="boolean">Boolean</MenuItem>
                <MenuItem value="date">Date</MenuItem>
                <MenuItem value="timestamp">Timestamp</MenuItem>
              </Select>
            </FormControl>
            <FormControlLabel
              control={
                <Checkbox 
                  checked={newCol.required} 
                  onChange={(e) => setNewCol({ ...newCol, required: e.target.checked })} 
                />
              }
              label="Required"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenAdd(false)}>Cancel</Button>
          <Button onClick={handleAdd} variant="contained">Add</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}

export default SchemaEditor;
