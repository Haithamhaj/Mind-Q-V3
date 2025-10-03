# PowerShell version of foundation validation script
Write-Host "🔍 Validating Foundation Setup..." -ForegroundColor Cyan

# Check if docker-compose is running (if Docker is available)
try {
    $dockerStatus = docker-compose ps 2>$null
    if ($dockerStatus -notmatch "Up") {
        Write-Host "❌ Docker Compose is not running. Run: docker-compose up -d" -ForegroundColor Red
        Write-Host "Note: Docker may not be installed or available" -ForegroundColor Yellow
    } else {
        Write-Host "✅ Docker Compose is running" -ForegroundColor Green
    }
} catch {
    Write-Host "⚠️ Docker not available - skipping Docker checks" -ForegroundColor Yellow
}

# Check backend health
Write-Host "Checking backend health..." -ForegroundColor Cyan
try {
    $healthResponse = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 5
    if ($healthResponse.StatusCode -eq 200) {
        $healthData = $healthResponse.Content | ConvertFrom-Json
        if ($healthData.status -eq "ok") {
            Write-Host "✅ Backend is healthy" -ForegroundColor Green
        } else {
            Write-Host "❌ Backend health check failed" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "❌ Backend health check failed (HTTP $($healthResponse.StatusCode))" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Backend is not responding" -ForegroundColor Red
    Write-Host "Make sure backend is running on http://localhost:8000" -ForegroundColor Yellow
    exit 1
}

# Check frontend (try both ports)
Write-Host "Checking frontend..." -ForegroundColor Cyan
$frontendWorking = $false

# Try port 3000 (Docker)
try {
    $frontendResponse = Invoke-WebRequest -Uri "http://localhost:3000" -UseBasicParsing -TimeoutSec 5
    if ($frontendResponse.StatusCode -eq 200) {
        Write-Host "✅ Frontend is accessible on port 3000" -ForegroundColor Green
        $frontendWorking = $true
    }
} catch {
    # Try port 4173 (local dev)
    try {
        $frontendResponse = Invoke-WebRequest -Uri "http://localhost:4173" -UseBasicParsing -TimeoutSec 5
        if ($frontendResponse.StatusCode -eq 200) {
            Write-Host "✅ Frontend is accessible on port 4173" -ForegroundColor Green
            $frontendWorking = $true
        }
    } catch {
        Write-Host "❌ Frontend is not responding on ports 3000 or 4173" -ForegroundColor Red
        Write-Host "Make sure frontend is running" -ForegroundColor Yellow
        exit 1
    }
}

# Check spec file
if (Test-Path "backend/spec/master_eda_spec_v1.2.2.json") {
    Write-Host "✅ Spec file exists" -ForegroundColor Green
} else {
    Write-Host "❌ Spec file not found" -ForegroundColor Red
    exit 1
}

# Check artifacts directory
if (Test-Path "backend/artifacts") {
    Write-Host "✅ Artifacts directory exists" -ForegroundColor Green
} else {
    Write-Host "❌ Artifacts directory missing" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅✅✅ Foundation validation PASSED ✅✅✅" -ForegroundColor Green
Write-Host ""
Write-Host "Next step: Implement Phase 1 (Goal & KPIs Definition)" -ForegroundColor Cyan
