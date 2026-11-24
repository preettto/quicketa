"""Partner-profile lookup from the PostgreSQL partner service (source of truth for age / rating)."""
from app.services.storage import pg_pool


def get_partner(partner_id: str):
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
