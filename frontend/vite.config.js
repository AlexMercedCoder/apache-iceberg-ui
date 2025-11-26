import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: process.env.FRONTEND_PORT ? parseInt(process.env.FRONTEND_PORT) : (process.env.PORT ? parseInt(process.env.PORT) : 5173),
  }
})
