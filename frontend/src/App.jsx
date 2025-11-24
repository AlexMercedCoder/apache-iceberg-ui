import React, { useState, useEffect } from 'react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { CssBaseline, Box, AppBar, Toolbar, Typography, Drawer, Button, Tabs, Tab, Container, Dialog, DialogContent, DialogTitle, IconButton } from '@mui/material';
import { Close } from '@mui/icons-material';
import ConnectionForm from './components/ConnectionForm';
// ... imports ...

function App() {
  const [connected, setConnected] = useState(false);
  const [catalogs, setCatalogs] = useState([]);
  const [activeCatalog, setActiveCatalog] = useState('default');
  const [selectedTable, setSelectedTable] = useState(null);
  const [tab, setTab] = useState(0);
  const [addCatalogOpen, setAddCatalogOpen] = useState(false);

  // ... useEffect ...

  const checkConnection = async () => {
    // ... existing checkConnection ...
    try {
      // Fetch list of connected catalogs
      const res = await api.get('/catalogs');
      const catalogList = res.data.catalogs;
      if (catalogList.length > 0) {
        setCatalogs(catalogList);
        setConnected(true);
        // Default to first catalog if active one is invalid
        if (!catalogList.includes(activeCatalog)) {
            setActiveCatalog(catalogList[0]);
        }
      } else {
        setConnected(false);
      }
    } catch (err) {
      console.error("Failed to check connection:", err);
      setConnected(false);
    }
  };

  const handleConnect = async (properties) => {
    try {
      // Submit the connection to the backend
      await api.post('/connect', properties);
      // After successful connection, check and update the catalog list
      await checkConnection();
      setAddCatalogOpen(false); // Close dialog if open
    } catch (err) {
      console.error("Failed to connect:", err);
      alert(`Connection failed: ${err.response?.data?.detail || err.message}`);
    }
  };

  // ... handleLogout ...

  // ... handleTableSelect ...

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', height: '100vh' }}>
        <AppBar position="fixed" sx={{ zIndex: theme.zIndex.drawer + 1 }}>
          <Toolbar>
            <Typography variant="h6" noWrap component="div" sx={{ flexGrow: 1, display: 'flex', alignItems: 'center', gap: 2 }}>
              <img src="/iceberg-logo-icon.png" alt="Iceberg" style={{ height: 32 }} />
              Iceberg UI
            </Typography>
            {connected && (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <Typography variant="body2">Active: {activeCatalog}</Typography>
                    <Button color="inherit" onClick={handleLogout}>Log Out</Button>
                </Box>
            )}
          </Toolbar>
        </AppBar>
        
        <Drawer
          variant="permanent"
          sx={{
            width: 300,
            flexShrink: 0,
            [`& .MuiDrawer-paper`]: { width: 300, boxSizing: 'border-box', mt: 8 },
          }}
        >
          {connected ? (
            <TableExplorer 
                catalog={activeCatalog} 
                catalogs={catalogs}
                onCatalogChange={setActiveCatalog}
                onSelectTable={handleTableSelect}
                onAddCatalog={() => setAddCatalogOpen(true)}
            />
          ) : (
            <Box sx={{ p: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Connect to a catalog to view tables.
              </Typography>
            </Box>
          )}
        </Drawer>

        <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8, overflow: 'auto' }}>
          {!connected ? (
            <Container maxWidth="sm">
              <ConnectionForm onConnect={handleConnect} />
            </Container>
          ) : (
            <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Tabs value={tab} onChange={(e, v) => setTab(v)} sx={{ mb: 2 }}>
                <Tab label="Query Editor" />
                <Tab label="Metadata Viewer" disabled={!selectedTable} />
                <Tab label="Documentation" />
              </Tabs>
              
              <Box sx={{ flexGrow: 1, overflow: 'hidden' }}>
                {tab === 0 && <QueryEditor initialNamespace={selectedTable?.namespace} catalog={activeCatalog} />}
                {tab === 1 && selectedTable && (
                  <MetadataViewer 
                    catalog={activeCatalog}
                    namespace={selectedTable.namespace} 
                    table={selectedTable.table} 
                  />
                )}
                {tab === 2 && <Documentation />}
              </Box>
            </Box>
          )}
        </Box>
      </Box>

      {/* Add Catalog Dialog */}
      <Dialog open={addCatalogOpen} onClose={() => setAddCatalogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>
            Add Catalog
            <IconButton
                aria-label="close"
                onClick={() => setAddCatalogOpen(false)}
                sx={{
                    position: 'absolute',
                    right: 8,
                    top: 8,
                    color: 'grey.500',
                }}
            >
                <Close />
            </IconButton>
        </DialogTitle>
        <DialogContent>
            <ConnectionForm onConnect={handleConnect} />
        </DialogContent>
      </Dialog>
    </ThemeProvider>
  );
}

export default App;
