# Deployment with Docker & Environments

## Docker Compose
Use the root `docker-compose.yml` to run frontend and backend together.

```bash
docker-compose up --build
```

Common environment variables can be placed in a `.env` file at the root if needed.

## Backend Dockerfile
Backend has a `backend/Dockerfile` ready for building the API service image.

```bash
cd backend
docker build -t eda-backend .
docker run -p 8000:8000 eda-backend
```

## Frontend
The frontend includes a `Dockerfile` as well. You can adapt `docker-compose.yml` to build and serve the production bundle or use your preferred host.

## Production Considerations
- Set proper CORS and allowed origins in the backend if exposed publicly.
- Use a reverse proxy (e.g., Nginx, Traefik) to terminate TLS and route traffic.
- Persist artifacts or route them to object storage if needed.
