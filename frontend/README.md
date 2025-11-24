# Iceberg UI - Frontend

React-based frontend for the Iceberg UI application, built with Vite and Material-UI.

## Tech Stack

- **React 18** - UI framework
- **Vite** - Build tool and dev server
- **Material-UI (MUI)** - Component library
- **Axios** - HTTP client for API calls
- **React Router** - Client-side routing (if applicable)

## Development

### Install Dependencies

```bash
npm install
```

### Run Development Server

```bash
npm run dev
```

The app will be available at `http://localhost:5173` by default.

### Build for Production

```bash
npm run build
```

The production build will be output to the `dist/` directory.

### Preview Production Build

```bash
npm run preview
```

## Project Structure

```
frontend/
├── src/
│   ├── components/          # React components
│   │   ├── App.jsx          # Main app component with multi-catalog state
│   │   ├── ConnectionForm.jsx    # Catalog connection form
│   │   ├── TableExplorer.jsx     # Namespace/table browser with catalog selector
│   │   ├── QueryEditor.jsx       # SQL query editor
│   │   ├── MetadataViewer.jsx    # Table metadata viewer
│   │   ├── FileUploadDialog.jsx  # File upload component
│   │   ├── MaintenanceControls.jsx  # Table maintenance operations
│   │   ├── SchemaEditor.jsx      # Schema evolution UI
│   │   └── MetadataCharts.jsx    # Metadata visualization
│   ├── api.js              # Axios API client configuration
│   ├── main.jsx            # Application entry point
│   └── index.css           # Global styles
├── public/                 # Static assets
├── index.html             # HTML template
├── vite.config.js         # Vite configuration
└── package.json           # Dependencies and scripts
```

## Key Components

### App.jsx
Main application component that manages:
- Multi-catalog state (connected catalogs, active catalog)
- Navigation between views
- Theme (light/dark mode)
- Logout functionality

### ConnectionForm.jsx
Handles catalog connections:
- Catalog name/alias input
- Catalog type selection
- Connection properties (URI, warehouse, credentials)
- Support for multiple catalog connections

### TableExplorer.jsx
Sidebar component for browsing:
- Catalog selector dropdown
- Namespace listing
- Table listing
- File upload button per table

### QueryEditor.jsx
SQL query interface:
- Query input with syntax highlighting
- Execute queries with catalog context
- Display results in table format
- Export results (CSV, JSON, Parquet)
- Query history

### FileUploadDialog.jsx
File upload interface:
- Support for CSV, JSON, and Parquet files
- Upload progress indicator
- Success/error feedback

## API Integration

The frontend communicates with the backend API at `http://localhost:8000` (configurable in `api.js`).

### Key Endpoints Used

- `POST /connect` - Connect to a catalog
- `POST /disconnect/{name}` - Disconnect from a catalog
- `GET /catalogs` - List connected catalogs
- `GET /catalogs/{catalog}/namespaces` - List namespaces
- `GET /catalogs/{catalog}/tables/{namespace}` - List tables
- `POST /query` - Execute SQL query
- `POST /catalogs/{catalog}/tables/{namespace}/{table}/upload` - Upload file

## Features

### Multi-Catalog Support
- Connect to multiple catalogs simultaneously
- Switch between catalogs using dropdown selector
- Each catalog maintains independent state

### Cross-Catalog Queries
- Execute SQL queries that join tables from different catalogs
- Automatic table registration and query rewriting

### File Uploads
- Upload CSV, JSON, or Parquet files
- Data is appended to existing Iceberg tables
- Progress feedback and error handling

### DML Operations
- Execute INSERT statements
- Execute DELETE statements with WHERE clauses
- Real-time feedback on affected rows

## Configuration

### API Base URL

Edit `src/api.js` to change the backend API URL:

```javascript
const api = axios.create({
  baseURL: 'http://localhost:8000'
});
```

### Theme Customization

The app uses Material-UI theming. Customize colors and styles in `App.jsx`:

```javascript
const theme = createTheme({
  palette: {
    mode: darkMode ? 'dark' : 'light',
    // Add custom colors here
  }
});
```

## Development Tips

### Hot Module Replacement (HMR)
Vite provides fast HMR out of the box. Changes to React components will be reflected immediately without full page reload.

### ESLint
The project includes ESLint configuration. Run linting with:

```bash
npm run lint
```

### Debugging
Use React DevTools browser extension for component inspection and state debugging.

## Building for Docker

The Dockerfile in the root directory builds the frontend as part of a multi-stage build:

1. Frontend is built in the first stage
2. Static files are copied to the backend's `static/` directory
3. Backend serves the frontend via FastAPI's static file serving

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)

## Contributing

When adding new features:
1. Create components in `src/components/`
2. Update API calls in component files or extract to `api.js`
3. Ensure responsive design (mobile-friendly)
4. Test in both light and dark modes
5. Update this README if adding major features
