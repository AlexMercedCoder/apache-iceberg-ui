import React, { useState } from 'react';
import { 
    Dialog, DialogTitle, DialogContent, DialogActions, 
    Button, Typography, Box, CircularProgress, Alert 
} from '@mui/material';
import { CloudUpload } from '@mui/icons-material';
import api from '../api';

function FileUploadDialog({ open, onClose, catalog, namespace, table }) {
    const [file, setFile] = useState(null);
    const [uploading, setUploading] = useState(false);
    const [message, setMessage] = useState(null);
    const [error, setError] = useState(null);

    const handleFileChange = (e) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0]);
            setMessage(null);
            setError(null);
        }
    };

    const handleUpload = async () => {
        if (!file) return;

        setUploading(true);
        setMessage(null);
        setError(null);

        const formData = new FormData();
        formData.append('file', file);

        try {
            const res = await api.post(
                `/catalogs/${catalog}/tables/${namespace}/${table}/upload`, 
                formData,
                {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                }
            );
            setMessage(`Success! Appended ${res.data.rows_appended} rows.`);
            setFile(null);
            // Optional: Refresh table data or notify parent
        } catch (err) {
            console.error("Upload failed", err);
            setError(err.response?.data?.detail || "Upload failed");
        } finally {
            setUploading(false);
        }
    };

    const handleClose = () => {
        setFile(null);
        setMessage(null);
        setError(null);
        onClose();
    };

    return (
        <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
            <DialogTitle>Upload Data to {table}</DialogTitle>
            <DialogContent>
                <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2, py: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                        Supported formats: CSV, JSON, Parquet. 
                        Data will be appended to the table.
                    </Typography>
                    
                    <Button
                        variant="outlined"
                        component="label"
                        startIcon={<CloudUpload />}
                    >
                        Select File
                        <input
                            type="file"
                            hidden
                            onChange={handleFileChange}
                            accept=".csv,.json,.parquet"
                        />
                    </Button>
                    
                    {file && (
                        <Typography variant="body1">
                            Selected: {file.name} ({(file.size / 1024).toFixed(1)} KB)
                        </Typography>
                    )}

                    {uploading && <CircularProgress />}
                    
                    {message && <Alert severity="success">{message}</Alert>}
                    {error && <Alert severity="error">{error}</Alert>}
                </Box>
            </DialogContent>
            <DialogActions>
                <Button onClick={handleClose}>Close</Button>
                <Button 
                    onClick={handleUpload} 
                    variant="contained" 
                    disabled={!file || uploading}
                >
                    Upload & Append
                </Button>
            </DialogActions>
        </Dialog>
    );
}

export default FileUploadDialog;
