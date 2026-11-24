# QuickETA - local Continuous Deployment step (P10)
# Pulls the image that the CD workflow published to GHCR and performs a ROLLING update:
# api replicas are replaced one at a time, so nginx keeps serving traffic (zero downtime).
#
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1 [-Image ghcr.io/preettto/quicketa:latest]
param([string]$Image = "ghcr.io/preettto/quicketa:latest")

function Step($n, $msg) { Write-Host "`n[$n] $msg" -ForegroundColor Cyan }
function Fail($msg) { Write-Host "DEPLOY FAILED: $msg" -ForegroundColor Red; exit 1 }

function Wait-Healthy($container, $timeoutSec = 90) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $state = docker inspect -f "{{.State.Health.Status}}" $container 2>$null
        if ($state -eq "healthy") { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

Step "1/5" "Pull $Image"
docker pull $Image
if ($LASTEXITCODE -ne 0) { Fail "could not pull image (is the GHCR package public?)" }

Step "2/5" "Tag as quicketa-app:latest (the name docker-compose uses)"
docker tag $Image quicketa-app:latest

Step "3/5" "Rolling update of API replicas (one at a time)"
foreach ($svc in "api1", "api2", "api3") {
    Write-Host "  -> replacing $svc"
    docker compose up -d --no-build --no-deps --force-recreate $svc | Out-Null
    if (-not (Wait-Healthy "qe-$svc")) { Fail "$svc did not become healthy" }
    Write-Host "     $svc healthy" -ForegroundColor Green
}

Step "4/5" "Update Kafka consumers"
docker compose up -d --no-build --no-deps --force-recreate store-consumer stats-consumer | Out-Null
docker compose up -d --no-build nginx | Out-Null

Step "5/5" "Smoke test through the load balancer"
$health = Invoke-RestMethod "http://localhost:8080/health"
if ($health.status -ne "ok") { Fail "health is '$($health.status)'" }
$order = @{ partner_id = "INDORES13DEL02"; restaurant = @{ lat = 22.745049; lon = 75.892471 };
            delivery = @{ lat = 22.765049; lon = 75.912471 }; traffic = "Medium";
            vehicle_type = "motorcycle"; order_type = "Snack" } | ConvertTo-Json
$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/orders" -ContentType "application/json" -Body $order
Write-Host "  ETA $($r.eta_minutes) min, served_by=$($r.served_by), storage=$($r.storage.path)" -ForegroundColor Green
Write-Host "`nDEPLOY SUCCEEDED: $Image" -ForegroundColor Green
