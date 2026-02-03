param(
  [switch]$KeepHistory,   # if set, DO NOT purge deals at start
  [switch]$NoOpen         # if set, don't open the HTML automatically
)

$ErrorActionPreference = "Stop"

$backend = $PSScriptRoot
$composeFile = Join-Path $backend "..\docker-compose.yml"
$python = Join-Path $backend ".venv\Scripts\python.exe"
$container = "amazon_deals_postgres"

Write-Host "Backend: $backend"
Write-Host "Compose:  $composeFile"
Write-Host "Python:   $python"
Write-Host "Container:$container"

if (!(Test-Path $python)) {
  throw "Venv python not found at: $python (did you create .venv?)"
}

# Purge deals by default so DB reflects current snapshot only
if ($KeepHistory) {
  $env:PURGE_DEALS_ON_START = "0"
  Write-Host "PURGE_DEALS_ON_START=0 (keeping previous deals)"
} else {
  $env:PURGE_DEALS_ON_START = "1"
  Write-Host "PURGE_DEALS_ON_START=1 (purging previous deals)"
}

# Start docker services
Write-Host "`n[1/4] Starting docker services..."
docker compose -f $composeFile up -d | Out-Host

# Wait for Postgres to accept connections
Write-Host "`n[2/4] Waiting for Postgres to be ready..."
$maxAttempts = 60
for ($i=1; $i -le $maxAttempts; $i++) {
  try {
    docker exec -i $container psql -U deals -d deals -c "select 1;" *> $null
    Write-Host "Postgres is ready."
    break
  } catch {
    if ($i -eq $maxAttempts) { throw "Postgres not ready after $maxAttempts attempts." }
    Start-Sleep -Seconds 1
  }
}

# Run ingestion
Write-Host "`n[3/4] Running ingestion..."
& $python -m app.ingestion

# Generate HTML
Write-Host "`n[4/4] Generating top_deals.html..."
& $python (Join-Path $backend "make_top_deals_html.py")

# DB quick summary (useful sanity check)
Write-Host "`nDB Summary:"
docker exec -it $container psql -U deals -d deals -c "
select
  now() as now_utc,
  count(*) as deals_total,
  max(ingested_at) as last_ingested_at,
  max(published_at) as last_published_at
from deals;
" | Out-Host

Write-Host "Active by category:"
docker exec -it $container psql -U deals -d deals -c "
select category_slug, count(*) as n
from deals
where is_active=true
group by category_slug
order by n desc;
" | Out-Host

# Open HTML
$html = Join-Path $backend "top_deals.html"
if (!$NoOpen) {
  Write-Host "`nOpening: $html"
  Start-Process $html
} else {
  Write-Host "`nHTML generated at: $html"
}
