#!/bin/bash
set -e

echo "🔍 Validating Foundation Setup..."

# Check if docker-compose is running
if ! docker-compose ps | grep -q "Up"; then
    echo "❌ Docker Compose is not running. Run: docker-compose up -d"
    exit 1
fi

# Check backend health
echo "Checking backend health..."
HEALTH=$(curl -s http://localhost:8000/health | grep -o '"status":"ok"')
if [ -z "$HEALTH" ]; then
    echo "❌ Backend health check failed"
    exit 1
fi
echo "✅ Backend is healthy"

# Check frontend
echo "Checking frontend..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000)
if [ "$HTTP_CODE" != "200" ]; then
    echo "❌ Frontend is not responding (HTTP $HTTP_CODE)"
    exit 1
fi
echo "✅ Frontend is accessible"

# Check spec file
if [ ! -f "backend/spec/master_eda_spec_v1.2.2.json" ]; then
    echo "❌ Spec file not found"
    exit 1
fi
echo "✅ Spec file exists"

# Check artifacts directory
if [ ! -d "backend/artifacts" ]; then
    echo "❌ Artifacts directory missing"
    exit 1
fi
echo "✅ Artifacts directory exists"

echo ""
echo "✅✅✅ Foundation validation PASSED ✅✅✅"
echo ""
echo "Next step: Implement Phase 0 (Quality Control)"
