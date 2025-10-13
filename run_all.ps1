Param(
    [switch]$ForceAuth,
    [string]$Start,
    [string]$End
)

Write-Host "[WHOOP] Starting end-to-end ingestion..." -ForegroundColor Cyan

# 1. Ensure docker compose up for postgres
if (Get-Command docker -ErrorAction SilentlyContinue) {
    Write-Host "[WHOOP] Bringing up Postgres via docker compose..." -ForegroundColor Cyan
    docker compose up -d postgres | Out-Null
} else {
    Write-Host "[WHOOP] Docker not found in PATH; skipping Postgres startup." -ForegroundColor Yellow
}

# 2. Python venv
if (-not (Test-Path .venv)) {
    Write-Host "[WHOOP] Creating virtual environment..." -ForegroundColor Cyan
    python -m venv .venv
}

# Activate venv for current session
$venvActivate = Join-Path (Resolve-Path .venv).Path 'Scripts/Activate.ps1'
. $venvActivate

# 3. Install requirements (if hash changed or missing packages)
Write-Host "[WHOOP] Installing/Updating dependencies..." -ForegroundColor Cyan
pip install -q -r requirements.txt

# 4. OAuth if needed
if ($ForceAuth -or -not (Test-Path .token_store.json)) {
    Write-Host "[WHOOP] Performing OAuth flow..." -ForegroundColor Cyan
    python whoop_ingest.py --auth-only
}

# 5. Run full ingestion (all default resources)
$argList = @()
if ($Start) { $argList += @('--start', $Start) }
if ($End) { $argList += @('--end', $End) }

Write-Host "[WHOOP] Running ingestion..." -ForegroundColor Cyan
python whoop_ingest.py @argList

Write-Host "[WHOOP] Done." -ForegroundColor Green
