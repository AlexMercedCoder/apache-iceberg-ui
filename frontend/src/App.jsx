import React, { useState, useEffect } from 'react';
import { ThemeProvider, createTheme, CssBaseline, Box, AppBar, Toolbar, Typography, Drawer, List, ListItem, ListItemIcon, ListItemText, IconButton } from '@mui/material';
import { TableChart, Code, Settings, Menu as MenuIcon, Storage, Help } from '@mui/icons-material';
import logo from './assets/logo.png';
import ConnectionForm from './components/ConnectionForm';
import TableExplorer from './components/TableExplorer';
import QueryEditor from './components/QueryEditor';
import MetadataViewer from './components/MetadataViewer';
import Documentation from './components/Documentation';
import api from './api';

const drawerWidth = 240;

function App() {
  const [mode, setMode] = useState('dark');
  const [connected, setConnected] = useState(false);
  const [currentView, setCurrentView] = useState('explorer'); // explorer, query, settings, documentation
  const [selectedTable, setSelectedTable] = useState(null); // { namespace, table }
  const [querySql, setQuerySql] = useState('');

  const theme = createTheme({
    palette: {
      mode: mode,
      primary: {
        main: '#1976d2',
      },
      secondary: {
        main: '#dc004e',
      },
      background: {
        default: mode === 'dark' ? '#121212' : '#f5f5f5',
      }
    },
  });

  useEffect(() => {
    checkConnection();
  }, []);

  const checkConnection = async () => {
    try {
      const res = await api.get('/status');
      if (res.data.connected) {
        setConnected(true);
      }
    } catch (err) {
      console.log("Not connected or backend down");
    }
  };

  const handleConnect = async (config) => {
    try {
      await api.post('/connect', { properties: config });
      setConnected(true);
      setCurrentView('explorer');
    } catch (error) {
      console.error("Connection failed", error);
      alert("Connection failed: " + error.message);
    }
  };

  const handleTableSelect = (namespace, table) => {
    setSelectedTable({ namespace, table });
    setCurrentView('metadata');
  };

  const handleQueryTable = (namespace, table) => {
    setQuerySql(`SELECT * FROM ${namespace}.${table} LIMIT 10`);
    setSelectedTable({ namespace, table });
    setCurrentView('query');
  };

  const renderContent = () => {
    if (!connected) {
      return <ConnectionForm onConnect={handleConnect} />;
    }

    switch (currentView) {
      case 'explorer':
        return <TableExplorer onSelectTable={handleTableSelect} onQueryTable={handleQueryTable} />;
      case 'metadata':
        return selectedTable ? (
          <MetadataViewer namespace={selectedTable.namespace} table={selectedTable.table} />
        ) : (
          <Typography variant="h6" sx={{ p: 3 }}>Select a table to view metadata</Typography>
        );
      case 'query':
        return <QueryEditor initialNamespace={selectedTable?.namespace} initialSql={querySql} />;
      case 'documentation':
        return <Documentation />;
      default:
        return <TableExplorer onSelectTable={handleTableSelect} />;
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', height: '100vh' }}>
        <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
          <Toolbar>
            <img src={logo} alt="Iceberg UI Logo" style={{ height: 40, marginRight: 16 }} />
            <Typography variant="h6" noWrap component="div" sx={{ flexGrow: 1 }}>
              Iceberg UI
            </Typography>
            <IconButton color="inherit" onClick={() => setMode(mode === 'dark' ? 'light' : 'dark')}>
              {mode === 'dark' ? '🌞' : 'Hz'}
            </IconButton>
          </Toolbar>
        </AppBar>
        
        {connected && (
          <Drawer
            variant="permanent"
            sx={{
              width: drawerWidth,
              flexShrink: 0,
              [`& .MuiDrawer-paper`]: { width: drawerWidth, boxSizing: 'border-box' },
            }}
          >
            <Toolbar />
            <Box sx={{ overflow: 'auto' }}>
              <List>
                <ListItem button onClick={() => setCurrentView('explorer')}>
                  <ListItemIcon><Storage /></ListItemIcon>
                  <ListItemText primary="Explorer" />
                </ListItem>
                <ListItem button onClick={() => setCurrentView('query')}>
                  <ListItemIcon><Code /></ListItemIcon>
                  <ListItemText primary="Query" />
                </ListItem>
                <ListItem button onClick={() => setCurrentView('documentation')}>
                  <ListItemIcon><Help /></ListItemIcon>
                  <ListItemText primary="Docs" />
                </ListItem>
              </List>
            </Box>
          </Drawer>
        )}

        <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8, overflow: 'auto' }}>
          {renderContent()}
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App;
