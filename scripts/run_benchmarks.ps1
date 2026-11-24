# QuickETA - Phase 9: benchmarks + failure demonstrations
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\run_benchmarks.ps1 -Test <name>
#   names: cache | lb | failover | chaos | kafka | mongo | ablation | all
param([ValidateSet("cache", "lb", "failover", "chaos", "kafka", "mongo", "ablation", "all")][string]$Test = "all")

function Title($t) { Write-Host "`n#############  $t  #############`n" -ForegroundColor Cyan }
function Note($t)  { Write-Host ">> $t" -ForegroundColor Yellow }
function Bench     { docker compose run --rm --no-deps pipeline python scripts/bench.py @args }
function RsStatus($node) {
    docker exec $node mongosh --quiet --eval "rs.status().members.map(m => m.name + ' -> ' + m.stateStr).join('\n')"
}
function Lag { docker exec qe-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-store 2>$null }

if ($Test -in "cache", "all") {
    Title "P7  Redis cache: cold vs warm"
    Bench cache --n 20
}

if ($Test -in "lb", "all") {
    Title "P8  Load balancing: 1 replica vs 3 replicas behind nginx"
    Bench lb --target single --seconds 20 --threads 16
    Bench lb --target lb --seconds 20 --threads 16
}

if ($Test -in "failover", "all") {
    Title "P8  Failover: kill a replica under steady load"
    $job = Start-Job -ScriptBlock { Set-Location $using:PWD; docker compose run --rm --no-deps pipeline python scripts/bench.py load --seconds 30 --threads 8 2>$null }
    Start-Sleep -Seconds 12
    Note "$(Get-Date -Format HH:mm:ss)  docker stop qe-api2"
    docker stop qe-api2 | Out-Null
    Start-Sleep -Seconds 10
    Note "$(Get-Date -Format HH:mm:ss)  docker start qe-api2"
    docker start qe-api2 | Out-Null
    Receive-Job -Job $job -Wait -AutoRemoveJob
}

if ($Test -in "chaos", "all") {
    Title "P9  Weather API outage: retry -> circuit breaker -> fallback -> recovery"
    Bench chaos
}

if ($Test -in "kafka", "all") {
    Title "P3  Kafka durability: consumer down, events buffered, then caught up"
    Note "stopping the order-store consumer"
    docker compose stop store-consumer | Out-Null
    Bench send --n 50 --out /srv/data/output/ids_kafka.txt
    Start-Sleep -Seconds 2
    Bench check --ids /srv/data/output/ids_kafka.txt
    Note "consumer-group lag while the consumer is down:"
    Lag
    Note "starting the consumer again"
    docker compose start store-consumer | Out-Null
    Start-Sleep -Seconds 10
    Bench check --ids /srv/data/output/ids_kafka.txt
    Note "lag after catch-up (should be 0):"
    Lag
}

if ($Test -in "mongo", "all") {
    Title "P5  MongoDB replica failover: kill the PRIMARY of shard 1"
    Note "before:"; RsStatus qe-shard1a
    Note "docker stop qe-shard1a  (the current PRIMARY)"
    docker stop qe-shard1a | Out-Null
    $t0 = Get-Date
    do {
        Start-Sleep -Seconds 1
        $p = docker exec qe-shard1b mongosh --quiet --eval "db.hello().primary" 2>$null
    } until (($p -and $p -notmatch "shard1a") -or ((Get-Date) - $t0).TotalSeconds -gt 40)
    Note ("new PRIMARY elected: {0} after {1:N0} s" -f $p, ((Get-Date) - $t0).TotalSeconds)
    RsStatus qe-shard1b
    Note "writes still work - placing 30 orders while shard1a is down"
    Bench send --n 30 --out /srv/data/output/ids_mongo.txt
    Start-Sleep -Seconds 5
    Bench check --ids /srv/data/output/ids_mongo.txt
    Note "docker start qe-shard1a  (it rejoins, catches up, and - having priority 2 - becomes PRIMARY again)"
    docker start qe-shard1a | Out-Null
    Start-Sleep -Seconds 20
    RsStatus qe-shard1b
}

if ($Test -in "ablation", "all") {
    Title "ML  Ablation: does real API weather improve the model?"
    docker compose run --rm --no-deps pipeline python scripts/ablation.py
}
