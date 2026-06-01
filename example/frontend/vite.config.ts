import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import path from 'path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: {
      // Point lab-link/svelte at the .svelte.ts source so Vite's Svelte plugin
      // compiles the runes ($state, $effect) rather than shipping pre-bundled output.
      'lab-link/svelte': path.resolve('../../js/src/svelte/index.svelte.ts'),
    },
  },
  server: {
    proxy: {
      '/sync': {
        target: 'http://localhost:8000',
        ws: true,
        changeOrigin: true,
      },
      '/assets': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
