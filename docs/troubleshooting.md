# Troubleshooting & FAQ

## Common Issues

### Ports already in use
- Adjust ports in `docker-compose.yml` or when running `uvicorn`.

### CORS errors in the browser
- Ensure backend CORS settings allow the frontend origin during development.

### Missing Python packages
- Reinstall dependencies: `pip install -r backend/requirements.txt`

### Node modules issues
- Remove `frontend/node_modules` and reinstall: `npm install`

## FAQs
- Where are artifacts stored?
  - `backend/artifacts/`
- How do I run tests?
  - `cd backend && pytest -q`
- Where are API docs?
  - `http://localhost:8000/api/docs` and `http://localhost:8000/api/redoc`
