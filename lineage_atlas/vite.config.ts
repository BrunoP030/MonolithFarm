import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';
import { privateDataPlugin } from './server/private-data-plugin';

const previewAllowedHosts = Array.from(
  new Set(
    [
      'monolithfarm.onrender.com',
      process.env.RENDER_EXTERNAL_HOSTNAME,
      ...(process.env.MONOLITH_ATLAS_ALLOWED_HOSTS || '').split(','),
    ]
      .map((host) => host?.trim())
      .filter(Boolean) as string[],
  ),
);

export default defineConfig({
  plugins: [privateDataPlugin(), react()],
  preview: {
    allowedHosts: previewAllowedHosts,
  },
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
