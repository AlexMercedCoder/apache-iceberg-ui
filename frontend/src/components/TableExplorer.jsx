import React, { useState, useEffect } from 'react';
import { List, ListItem, ListItemText, ListItemIcon, Collapse, Typography, Button, TextField, Dialog, DialogTitle, DialogContent, DialogActions, IconButton } from '@mui/material';
import { ExpandLess, ExpandMore, Folder, TableChart, Add, Code } from '@mui/icons-material';
import api from '../api';

function TableExplorer({ onSelectTable, onQueryTable }) {
  const [namespaces, setNamespaces] = useState([]);
  const [expandedNamespace, setExpandedNamespace] = useState(null);
  const [tables, setTables] = useState({}); // { namespace: [tables] }
  const [openCreateNs, setOpenCreateNs] = useState(false);
  const [newNsName, setNewNsName] = useState('');

  useEffect(() => {
    fetchNamespaces();
  }, []);

  const fetchNamespaces = async () => {
    try {
      const res = await api.get('/namespaces');
      // res.data.namespaces is list of lists/tuples, e.g. [['default']] or [['ns1'], ['ns2']]
      // We assume simple 1-level namespaces for now or flatten them
      const nsList = res.data.namespaces.map(n => n[0]); 
      setNamespaces(nsList);
    } catch (err) {
      console.error("Failed to fetch namespaces", err);
    }
  };

  const handleExpandNamespace = async (ns) => {
    if (expandedNamespace === ns) {
      setExpandedNamespace(null);
    } else {
      setExpandedNamespace(ns);
      if (!tables[ns]) {
        try {
          const res = await api.get(`/tables/${ns}`);
          setTables(prev => ({ ...prev, [ns]: res.data.tables }));
        } catch (err) {
          console.error(`Failed to fetch tables for ${ns}`, err);
        }
      }
    }
  };

  const handleCreateNamespace = async () => {
    try {
      await api.post('/namespaces', { namespace: newNsName });
      setOpenCreateNs(false);
      setNewNsName('');
      fetchNamespaces();
    } catch (err) {
      alert("Failed to create namespace");
    }
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px' }}>
        <Typography variant="h6">Explorer</Typography>
        <Button size="small" startIcon={<Add />} onClick={() => setOpenCreateNs(true)}>
          NS
        </Button>
      </div>
      <List>
        {namespaces.map((ns) => (
          <React.Fragment key={ns}>
            <ListItem button onClick={() => handleExpandNamespace(ns)}>
              <ListItemIcon><Folder /></ListItemIcon>
              <ListItemText primary={ns} />
              {expandedNamespace === ns ? <ExpandLess /> : <ExpandMore />}
            </ListItem>
            <Collapse in={expandedNamespace === ns} timeout="auto" unmountOnExit>
              <List component="div" disablePadding>
                {tables[ns]?.map((table) => (
                  <ListItem 
                    button 
                    key={table} 
                    sx={{ pl: 4 }} 
                    onClick={() => onSelectTable(ns, table)}
                    secondaryAction={
                      <IconButton 
                        edge="end" 
                        size="small" 
                        onClick={(e) => {
                          e.stopPropagation();
                          onQueryTable(ns, table);
                        }}
                        title="Query this table"
                      >
                        <Code fontSize="small" />
                      </IconButton>
                    }
                  >
                    <ListItemIcon><TableChart fontSize="small" /></ListItemIcon>
                    <ListItemText primary={table} />
                  </ListItem>
                ))}
                {(!tables[ns] || tables[ns].length === 0) && (
                  <ListItem sx={{ pl: 4 }}>
                    <ListItemText primary="No tables" secondary="Use Query to create" />
                  </ListItem>
                )}
              </List>
            </Collapse>
          </React.Fragment>
        ))}
      </List>

      <Dialog open={openCreateNs} onClose={() => setOpenCreateNs(false)}>
        <DialogTitle>Create Namespace</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Namespace Name"
            fullWidth
            value={newNsName}
            onChange={(e) => setNewNsName(e.target.value)}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenCreateNs(false)}>Cancel</Button>
          <Button onClick={handleCreateNamespace}>Create</Button>
        </DialogActions>
      </Dialog>
    </div>
  );
}

export default TableExplorer;
