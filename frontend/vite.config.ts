import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 30333,
    proxy: {
      '/api': {
        target: "https://api.investment-assistant.site",
        changeOrigin: true,
      },
      '/ws': {
        target: "wss://api.investment-assistant.site",
        ws: true,
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['react', 'react-dom'],
          router: ['react-router-dom'],
          ui: ['lucide-react'],
          charts: ['recharts'],
          utils: ['axios', 'date-fns'],
        },
      },
    },
  },
  define: {
    global: 'globalThis',
  },
})
