# Frontend

## Run Locally
```bash
cd frontend
npm install
npm run dev
```

## Structure
- `src/components/`: Viewers (e.g., QualityControl, Profiling, Correlations) and UI elements
- `src/pages/`: App pages such as `Home`, `QualityControl`
- `src/api/client.ts`: HTTP client configuration

## UI Stack
- React 18 + TypeScript
- TailwindCSS + shadcn/ui components

## Development Tips
- Keep components focused and reusable.
- Co-locate simple styles; use utility classes for layout.
- Use the API client for all network calls for consistency.
