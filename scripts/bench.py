"""QuickETA benchmark & failure-demo tool (Phase 9).

Runs INSIDE the docker network (via the pipeline container), so it reaches services by name:
    nginx:8080 (load balancer)   api1:8000 (single replica, bypassing the LB)

Usage (normally called by scripts/run_benchmarks.ps1):
    python scripts/bench.py cache
    python scripts/bench.py lb --target single|lb --seconds 20 --threads 16
    python scripts/bench.py load --seconds 30          # steady load, per-second timeline (failover demo)
    python scripts/bench.py chaos
    python scripts/bench.py send --n 50 --out FILE     # place N orders, save their ids
    python scripts/bench.py check --ids FILE           # how many of those orders are in MongoDB?
Results are also saved as JSON in /srv/data/output/ for the report.
"""
import argparse
import json
import os
import random
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import requests

LB = "http://nginx:8080"
SINGLE = "http://api1:8000"
OUT = "/srv/data/output"

RESTAURANTS = [  # (partner_id, restaurant lat, lon) - real coordinates from the dataset
    ("INDORES13DEL02", 22.745049, 75.892471),
    ("BANGRES19DEL01", 12.914264, 77.678400),
    ("AGRRES010DEL01", 27.161661, 78.042990),
    ("HYDRES15DEL02", 17.431668, 78.408321),
]


def make_order(rng: random.Random, fixed: bool = False) -> dict:
    pid, lat, lon = RESTAURANTS[0] if fixed else rng.choice(RESTAURANTS)
    d = 0.02 if fixed else rng.uniform(0.005, 0.09)
    return {"partner_id": pid, "restaurant": {"lat": lat, "lon": lon},
            "delivery": {"lat": round(lat + d, 6), "lon": round(lon + d, 6)},
            "traffic": "High" if fixed else rng.choice(["Low", "Medium", "High", "Jam"]),
            "vehicle_type": "motorcycle" if fixed else rng.choice(["motorcycle", "scooter"]),
            "order_type": "Meal" if fixed else rng.choice(["Snack", "Meal", "Drinks", "Buffet"]),
            "multiple_deliveries": 0 if fixed else rng.randint(0, 2)}


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    return round(s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))], 1)


def save(name, data):
    os.makedirs(OUT, exist_ok=True)
    with open(f"{OUT}/bench_{name}.json", "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"\n(saved {OUT}/bench_{name}.json)")


def table(rows, headers):
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    line = "+".join("-" * (w + 2) for w in widths)
    print("+" + line + "+")
    print("| " + " | ".join(str(h).ljust(w) for h, w in zip(headers, widths)) + " |")
    print("+" + line + "+")
    for r in rows:
        print("| " + " | ".join(str(c).ljust(w) for c, w in zip(r, widths)) + " |")
    print("+" + line + "+")


# ----------------------------------------------------------------- P7: cache
def cmd_cache(args):
    s = requests.Session()
    order = make_order(random.Random(1), fixed=True)
    cold, warm = [], []
    print(f"COLD: flush Redis before each request ({args.n} requests)")
    for _ in range(args.n):
        s.delete(f"{LB}/admin/cache")
        t = time.perf_counter()
        r = s.post(f"{LB}/api/orders", json=order).json()
        cold.append(((time.perf_counter() - t) * 1000, r["latency_ms"], r["cache"]))
    print(f"WARM: identical request repeated ({args.n} requests)")
    for _ in range(args.n):
        t = time.perf_counter()
        r = s.post(f"{LB}/api/orders", json=order).json()
        warm.append(((time.perf_counter() - t) * 1000, r["latency_ms"], r["cache"]))
    rows = []
    for name, data in (("cold (all MISS)", cold), ("warm (all HIT)", warm)):
        client, server = [d[0] for d in data], [d[1] for d in data]
        rows.append([name, len(data), pct(server, 50), pct(server, 95), round(statistics.mean(server), 1),
                     pct(client, 50), dict(Counter(str(d[2]) for d in data).most_common(1))])
    print()
    table(rows, ["scenario", "n", "server p50 ms", "server p95 ms", "server mean ms", "client p50 ms", "cache status"])
    speedup = round(rows[0][4] / max(rows[1][4], 0.01), 1)
    print(f"\nRedis cache speed-up (mean server latency): {speedup}x")
    save("cache", {"rows": rows, "speedup_x": speedup})


# ----------------------------------------------------------------- load generator
def run_load(base, seconds, threads, timeline=False):
    stop = time.time() + seconds
    lock = threading.Lock()
    results = []   # (t_offset, ok, latency_ms, served_by)
    t0 = time.time()

    def worker(i):
        rng, s = random.Random(i), requests.Session()
        while time.time() < stop:
            t = time.perf_counter()
            try:
                r = s.post(f"{base}/api/orders", json=make_order(rng), timeout=10)
                ok, by = r.status_code == 201, (r.json().get("served_by") if r.status_code == 201 else f"HTTP {r.status_code}")
            except Exception as e:
                ok, by = False, type(e).__name__
            with lock:
                results.append((time.time() - t0, ok, (time.perf_counter() - t) * 1000, by))

    with ThreadPoolExecutor(threads) as ex:
        list(ex.map(worker, range(threads)))
    return results


def summarise(results, seconds):
    lat = [r[2] for r in results if r[1]]
    return {"requests": len(results), "ok": sum(r[1] for r in results), "failed": sum(not r[1] for r in results),
            "throughput_rps": round(sum(r[1] for r in results) / seconds, 1),
            "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95),
            "served_by": dict(Counter(r[3] for r in results if r[1]))}


# ----------------------------------------------------------------- P8: load balancing
def cmd_lb(args):
    base = SINGLE if args.target == "single" else LB
    label = "1 replica (api1 direct)" if args.target == "single" else "3 replicas via nginx"
    requests.post(f"{base}/api/orders", json=make_order(random.Random(0)))   # warm the weather cache
    print(f"{label}: {args.threads} concurrent clients for {args.seconds}s ...")
    s = summarise(run_load(base, args.seconds, args.threads), args.seconds)
    table([[label, s["requests"], s["failed"], s["throughput_rps"], s["p50_ms"], s["p95_ms"], s["served_by"]]],
          ["setup", "requests", "failed", "req/s", "p50 ms", "p95 ms", "served_by"])
    save(f"lb_{args.target}", s)


def cmd_load(args):
    print(f"steady load on nginx: {args.threads} clients for {args.seconds}s (per-second timeline)")
    res = run_load(LB, args.seconds, args.threads)
    rows = []
    for sec in range(args.seconds):
        chunk = [r for r in res if sec <= r[0] < sec + 1]
        rows.append([f"{sec:>2}s", sum(r[1] for r in chunk), sum(not r[1] for r in chunk),
                     " ".join(f"{k}:{v}" for k, v in sorted(Counter(r[3] for r in chunk if r[1]).items()))])
    table(rows, ["t", "ok", "failed", "served_by"])
    s = summarise(res, args.seconds)
    print(f"\nTOTAL: {s['ok']} ok, {s['failed']} failed  ->  availability {100 * s['ok'] / max(1, s['requests']):.2f}%")
    save("failover", {"timeline": rows, "summary": s})


# ----------------------------------------------------------------- P9: circuit breaker
def cmd_chaos(args):
    s = requests.Session()
    s.delete(f"{SINGLE}/admin/cache")
    order = make_order(random.Random(2), fixed=True)

    def shot(phase):
        t = time.perf_counter()
        r = s.post(f"{SINGLE}/api/orders", json=order).json()
        ms = round((time.perf_counter() - t) * 1000)
        st = s.get(f"{SINGLE}/admin/chaos").json()
        return [phase, r["weather"]["source"], r["cache"]["weather"], r["eta_minutes"], ms,
                st["weather_circuit"]["state"], st["weather_circuit"]["consecutive_failures"]]

    rows = [shot("normal")]
    s.post(f"{SINGLE}/admin/chaos", json={"weather_down": True})
    for i in range(4):
        rows.append(shot(f"outage #{i + 1}"))
    s.post(f"{SINGLE}/admin/chaos", json={"weather_down": False})
    s.delete(f"{SINGLE}/admin/cache")             # force real API calls, so the breaker is exercised
    rows.append(shot("API back, breaker still OPEN"))
    print("waiting 31 s for the breaker cool-down (OPEN -> HALF_OPEN) ...")
    time.sleep(31)
    s.delete(f"{SINGLE}/admin/cache")
    rows.append(shot("after cool-down: probe call"))
    rows.append(shot("recovered (cached)"))
    table(rows, ["phase", "weather", "cache", "ETA min", "latency ms", "breaker", "failures"])
    print("\nRead it as: retries make outage calls slow -> after 3 failures the breaker OPENS -> calls fail FAST "
          "(no network) -> ETA still returned from the fallback -> after 30 s one probe succeeds -> CLOSED.")
    save("chaos", {"rows": rows})


# ----------------------------------------------------------------- P3 / P5: durability checks
def cmd_send(args):
    rng, s, ids, paths = random.Random(7), requests.Session(), [], Counter()
    for _ in range(args.n):
        r = s.post(f"{LB}/api/orders", json=make_order(rng)).json()
        ids.append(r["order_id"])
        paths[r["storage"]["path"]] += 1
    with open(args.out, "w") as f:
        f.write("\n".join(ids))
    print(f"placed {len(ids)} orders  storage paths: {dict(paths)}  -> ids saved to {args.out}")


def cmd_check(args):
    s = requests.Session()
    ids = open(args.ids).read().split()
    found = sum(s.get(f"{LB}/api/orders/{i}").status_code == 200 for i in ids)
    print(f"orders in MongoDB: {found}/{len(ids)}" + ("   (all stored)" if found == len(ids) else "   (rest still in Kafka)"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("cache")
    p.add_argument("--n", type=int, default=20)
    p = sub.add_parser("lb")
    p.add_argument("--target", choices=["single", "lb"], required=True)
    p.add_argument("--seconds", type=int, default=20)
    p.add_argument("--threads", type=int, default=16)
    p = sub.add_parser("load")
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--threads", type=int, default=8)
    sub.add_parser("chaos")
    p = sub.add_parser("send")
    p.add_argument("--n", type=int, default=50)
    p.add_argument("--out", default="/srv/data/output/ids.txt")
    p = sub.add_parser("check")
    p.add_argument("--ids", default="/srv/data/output/ids.txt")
    a = ap.parse_args()
    {"cache": cmd_cache, "lb": cmd_lb, "load": cmd_load, "chaos": cmd_chaos,
     "send": cmd_send, "check": cmd_check}[a.cmd](a)
