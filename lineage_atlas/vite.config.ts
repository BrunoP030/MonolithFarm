import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';
import { privateDataPlugin } from './server/private-data-plugin';

export default defineConfig({
  plugins: [privateDataPlugin(), react()],
  build: {
    rollupOptions: {
      onwarn(warning, warn) {
        if (warning.code === 'MODULE_LEVEL_DIRECTIVE' && /@xyflow\/react/.test(warning.message)) {
          return;
        }
        warn(warning);
      },
    },
  },
});
