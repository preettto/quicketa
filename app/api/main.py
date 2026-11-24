"""QuickETA API service (P1 REST + P2 GraphQL).

Run (inside the container):  gunicorn -w 2 --threads 4 -b 0.0.0.0:8000 "app.api.main:create_app()"
"""
import os

from flask import Flask, jsonify, request

from app import cache
from app.api.graphql_schema import schema
from app.ml.predictor import load_model
from app.services import live_stats, weather
from app.services import orders as svc
from app.services.storage import mongo_db, pg_pool


def create_app() -> Flask:
    app = Flask(__name__)
    load_model()   # load once at start-up, not on the first request

    @app.post("/api/orders")
    def place_order():
        try:
            return jsonify(svc.place_order(request.get_json(force=True))), 201
        except svc.ValidationError as e:
            return jsonify({"error": str(e)}), 400

    @app.get("/api/orders/<order_id>")
    def get_order(order_id):
        doc = svc.get_order(order_id)
        return (jsonify(doc), 200) if doc else (jsonify({"error": "not found"}), 404)

    @app.get("/api/model")
    def model_info():
        return jsonify(load_model()[1])

    @app.get("/health")
    def health():
        checks = {}
        try:
            mongo_db().command("ping")
            checks["mongodb"] = "up"
        except Exception as e:
            checks["mongodb"] = f"down: {type(e).__name__}"
        try:
            conn = pg_pool().getconn()
            pg_pool().putconn(conn)
            checks["postgres"] = "up"
        except Exception as e:
            checks["postgres"] = f"down: {type(e).__name__}"
        try:
            cache.client().ping()
            checks["redis"] = "up"
        except Exception as e:
            checks["redis"] = f"down: {type(e).__name__} (cache bypassed)"
        checks["model"] = load_model()[1]["model_version"]
        checks["weather_circuit"] = weather.breaker.snapshot()
        ok = checks["mongodb"] == "up" and checks["postgres"] == "up"
        return jsonify({"status": "ok" if ok else "degraded", "served_by": svc.INSTANCE,
                        "checks": checks}), (200 if ok else 503)

    @app.get("/api/stats/live")
    def stats_live():
        minutes = int(request.args.get("minutes", live_stats.WINDOW_MIN))
        return jsonify(live_stats.window(minutes))

    # ---------- admin: fault injection + cache control (for demos / benchmarks) ----------
    @app.route("/admin/chaos", methods=["GET", "POST"])
    def chaos():
        if request.method == "POST":
            cache.set_flag(weather.CHAOS_KEY, bool((request.get_json(force=True) or {}).get("weather_down")))
        return jsonify({"weather_down": cache.get_flag(weather.CHAOS_KEY),
                        "weather_circuit": weather.breaker.snapshot(), "served_by": svc.INSTANCE})

    @app.delete("/admin/cache")
    def flush_cache():
        removed = {p: cache.delete_pattern(f"{p}:*") for p in ("weather", "partner", "eta")}
        return jsonify({"removed": removed})

    @app.route("/graphql", methods=["POST"])
    def graphql():
        payload = request.get_json(force=True)
        result = schema.execute(payload.get("query"), variable_values=payload.get("variables"))
        body = {"data": result.data}
        if result.errors:
            body["errors"] = [str(e) for e in result.errors]
        return jsonify(body), (400 if result.errors and not result.data else 200)

    @app.get("/")
    def index():
        return jsonify({"service": "QuickETA", "served_by": os.getenv("INSTANCE_NAME", "local"),
                        "endpoints": ["POST /api/orders", "GET /api/orders/<id>", "GET /api/stats/live",
                                      "GET /api/model", "GET /health", "POST /graphql",
                                      "GET|POST /admin/chaos", "DELETE /admin/cache"]})

    return app
