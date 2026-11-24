# QuickETA - API demo (Phase 5: REST + GraphQL)
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\demo_api.ps1 [-BaseUrl http://localhost:8000]
param([string]$BaseUrl = "http://localhost:8000")

function Show($title, $obj) {
    Write-Host "`n=== $title ===" -ForegroundColor Cyan
    $obj | ConvertTo-Json -Depth 6
}

# 1. Health
Show "GET /health" (Invoke-RestMethod "$BaseUrl/health")

# 2. Place an order (REST)
$order = @{
    partner_id          = "INDORES13DEL02"
    restaurant          = @{ lat = 22.745049; lon = 75.892471 }
    delivery            = @{ lat = 22.765049; lon = 75.912471 }
    traffic             = "Jam"
    vehicle_type        = "motorcycle"
    order_type          = "Meal"
    multiple_deliveries = 1
    city_type           = "Urban"
} | ConvertTo-Json -Depth 4
$placed = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/orders" -ContentType "application/json" -Body $order
Show "POST /api/orders  (traffic = Jam)" $placed

# 3. Same order, low traffic -> ETA should drop
$low = $order -replace '"Jam"', '"Low"'
Show "POST /api/orders  (traffic = Low)" (Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/orders" -ContentType "application/json" -Body $low)

# 4. Fetch it back (targeted query on the shard key)
Show "GET /api/orders/$($placed.order_id)" (Invoke-RestMethod "$BaseUrl/api/orders/$($placed.order_id)")

# 5. Validation error -> 400
Write-Host "`n=== POST /api/orders  (invalid traffic -> expect 400) ===" -ForegroundColor Cyan
try { Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/orders" -ContentType "application/json" -Body ($order -replace '"Jam"', '"Crazy"') }
catch { Write-Host $_.ErrorDetails.Message -ForegroundColor Yellow }

# 6. GraphQL query: client picks exactly the fields it needs
$gq = @{ query = '{ orders(cityCode: "INDO", source: "live", limit: 3) { orderId etaMinutes traffic servedBy weather { condition tempC source } } cityStats(cityCode: "INDO") { cityCode orders avgActualMinutes avgPredictedMinutes } }' } | ConvertTo-Json
Show "POST /graphql  (query)" (Invoke-RestMethod -Method Post -Uri "$BaseUrl/graphql" -ContentType "application/json" -Body $gq)

# 7. GraphQL mutation
$gm = @{ query = 'mutation { placeOrder(input: { partnerId: "BANGRES19DEL01", restaurant: {lat: 12.914264, lon: 77.6784}, delivery: {lat: 12.924264, lon: 77.6884}, traffic: "High", vehicleType: "scooter", orderType: "Snack" }) { orderId etaMinutes etaTime servedBy } }' } | ConvertTo-Json
Show "POST /graphql  (mutation placeOrder)" (Invoke-RestMethod -Method Post -Uri "$BaseUrl/graphql" -ContentType "application/json" -Body $gm)
