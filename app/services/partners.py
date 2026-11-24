"""Partner-profile lookup: Redis cache (1 h) in front of the PostgreSQL partner service."""
from app import cache
from app.services.storage import pg_pool


def _from_db(partner_id: str):
    pool = pg_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT age, rating FROM delivery_partners WHERE partner_id = %s", (partner_id,))
            row = cur.fetchone()
    finally:
        pool.putconn(conn)
    if not row:
        return None
    return {"age": float(row[0]) if row[0] is not None else None,
            "rating": float(row[1]) if row[1] is not None else None}


def get_partner(partner_id: str):
    """Returns (profile_or_None, cache_status)."""
    key = f"partner:{partner_id}"
    hit = cache.get_json(key)
    if hit:
        return hit, "hit"
    p = _from_db(partner_id)
    if p:
        cache.set_json(key, p, ttl_s=3600)
    return p, "miss"
