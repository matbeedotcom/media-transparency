import { defineConfig } from 'orval';

export default defineConfig({
  mitds: {
    input: {
      target: '../backend/openapi/openapi.yaml',
    },
    output: {
      mode: 'tags-split',
      target: './src/api/generated',
      schemas: './src/api/generated/models',
      client: 'react-query',
      httpClient: 'axios',
      override: {
        mutator: {
          path: './src/api/axios-instance.ts',
          name: 'customInstance',
        },
        query: {
          useQuery: true,
          useMutation: true,
          signal: true,
        },
      },
      mock: false,
    },
    hooks: {
      afterAllFilesWrite: 'prettier --write',
    },
  },
});
