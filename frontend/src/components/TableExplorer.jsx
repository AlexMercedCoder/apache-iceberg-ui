import React, { useState, useEffect } from 'react';
import { 
  List, ListItem, ListItemText, Collapse, ListItemIcon, 
  Typography, CircularProgress, Box, Select, MenuItem, FormControl, InputLabel, IconButton
} from '@mui/material';
import { ExpandLess, ExpandMore, Folder, TableChart, CloudUpload } from '@mui/icons-material';
import api from '../api';
import FileUploadDialog from './FileUploadDialog';

function TableExplorer({ catalog, catalogs, onCatalogChange, onSelectTable }) {
  const [namespaces, setNamespaces] = useState([]);
  const [expanded, setExpanded] = useState({});
  const [tables, setTables] = useState({});
  const [loading, setLoading] = useState(false);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [uploadTarget, setUploadTarget] = useState(null); // { namespace, table }

  useEffect(() => {
    if (catalog) {
      loadNamespaces();
    }
  }, [catalog]);

  const loadNamespaces = async () => {
    setLoading(true);
    try {
      const res = await api.get(`/catalogs/${catalog}/namespaces`);
      setNamespaces(res.data.namespaces);
    } catch (err) {
      console.error("Failed to load namespaces", err);
    } finally {
      setLoading(false);
    }
  };

  const handleToggle = async (ns) => {
    const nsName = Array.isArray(ns) ? ns.join('.') : ns;
    
    setExpanded(prev => ({ ...prev, [nsName]: !prev[nsName] }));
    
    if (!tables[nsName] && !expanded[nsName]) {
      try {
        const res = await api.get(`/catalogs/${catalog}/tables/${nsName}`);
        setTables(prev => ({ ...prev, [nsName]: res.data.tables }));
      } catch (err) {
        console.error("Failed to load tables", err);
      }
    }
  };

  const handleUploadClick = (e, namespace, table) => {
    e.stopPropagation();
    setUploadTarget({ namespace, table });
    setUploadOpen(true);
  };

  return (
    <Box>
      <Box sx={{ p: 2, borderBottom: 1, borderColor: 'divider' }}>
        <FormControl fullWidth size="small" sx={{ mb: 2 }}>
            <InputLabel>Catalog</InputLabel>
            <Select
                value={catalog}
                label="Catalog"
                onChange={(e) => onCatalogChange(e.target.value)}
            >
                {catalogs.map(c => (
                    <MenuItem key={c} value={c}>{c}</MenuItem>
                ))}
            </Select>
        </FormControl>
        <Typography variant="h6" gutterBottom>Explorer</Typography>
      </Box>
      
      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
          <CircularProgress size={24} />
        </Box>
      ) : (
        <List component="nav">
          {namespaces.map((ns) => {
            const nsName = Array.isArray(ns) ? ns.join('.') : ns;
            return (
              <React.Fragment key={nsName}>
                <ListItem button onClick={() => handleToggle(ns)}>
                  <ListItemIcon><Folder /></ListItemIcon>
                  <ListItemText primary={nsName} />
                  {expanded[nsName] ? <ExpandLess /> : <ExpandMore />}
                </ListItem>
                <Collapse in={expanded[nsName]} timeout="auto" unmountOnExit>
                  <List component="div" disablePadding>
                    {tables[nsName]?.map((table) => (
                      <ListItem 
                        key={table} 
                        button 
                        sx={{ pl: 4 }}
                        onClick={() => onSelectTable(nsName, table)}
                        secondaryAction={
                            <IconButton edge="end" aria-label="upload" onClick={(e) => handleUploadClick(e, nsName, table)}>
                                <CloudUpload fontSize="small" />
                            </IconButton>
                        }
                      >
                        <ListItemIcon><TableChart fontSize="small" /></ListItemIcon>
                        <ListItemText primary={table} />
                      </ListItem>
                    ))}
                    {(!tables[nsName] || tables[nsName].length === 0) && (
                        <ListItem sx={{ pl: 4 }}>
                            <ListItemText secondary="No tables found" />
                        </ListItem>
                    )}
                  </List>
                </Collapse>
              </React.Fragment>
            );
          })}
        </List>
      )}
      
      {uploadTarget && (
        <FileUploadDialog 
            open={uploadOpen} 
            onClose={() => setUploadOpen(false)}
            catalog={catalog}
            namespace={uploadTarget.namespace}
            table={uploadTarget.table}
        />
      )}
    </Box>
  );
}

export default TableExplorer;
