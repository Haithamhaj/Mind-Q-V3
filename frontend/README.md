# Master EDA Platform - Frontend

## Overview
This is the frontend application for the Master EDA Platform, built with React 18, TypeScript, and modern UI components.

## Technology Stack
- **Framework**: React 18+ with TypeScript
- **UI Library**: shadcn/ui (built on Radix UI + Tailwind CSS)
- **State Management**: Zustand
- **HTTP Client**: Axios
- **Charts**: Recharts
- **File Upload**: react-dropzone
- **Build Tool**: Vite

## Project Structure
```
frontend/
├── src/
│   ├── pages/           # Page components
│   ├── components/      # Reusable components
│   ├── api/            # API client
│   ├── store/          # Zustand store
│   ├── types/          # TypeScript types
│   └── lib/            # Utilities
├── public/             # Static assets
└── dist/              # Build output
```

## Quick Start

### Development
```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

### Docker
```bash
# Build image
docker build -t eda-frontend .

# Run container
docker run -p 5173:5173 eda-frontend
```

## Environment Variables
Create a `.env.local` file:
```
VITE_API_URL=http://localhost:8000
```

## Available Scripts
- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint

## Development Server
- **URL**: http://localhost:5173
- **Hot Reload**: Enabled
- **TypeScript**: Strict mode enabled

## Current Status
- Phase 0: ✅ Foundation & Architecture (Completed)
- Frontend: ✅ Basic setup completed
- Integration: ⏳ Pending backend connection
