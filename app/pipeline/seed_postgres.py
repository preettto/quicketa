"""Seed the PostgreSQL 'partner service' DB from the raw CSV.

In a real company the delivery-partner and restaurant master data live in another team's
operational database. We simulate that system once, so the pipeline can EXTRACT from a
genuine third source (CSV + REST API + relational DB).

Run:  docker compose run --rm pipeline python -m app.pipeline.seed_postgres
"""
import pandas as pd

from app import config


def read_raw(path: str = None) -> pd.DataFrame:
    raw = pd.read_csv(path or config.RAW_CSV, dtype=str)
    raw = raw.apply(lambda s: s.str.strip())
    return raw.replace({"NaN": None})


def city_code_of(partner_id: pd.Series) -> pd.Series:
    """'INDORES13DEL02' -> 'INDO' (the city is encoded before 'RES')."""
    return partner_id.str.split("RES").str[0]


def build_tables(raw: pd.DataFrame):
    """Pure function: raw CSV rows -> (partners_df, restaurants_df)."""
    df = raw.copy()
    df["city_code"] = city_code_of(df["Delivery_person_ID"])
    df["age"] = pd.to_numeric(df["Delivery_person_Age"], errors="coerce")
    rating = pd.to_numeric(df["Delivery_person_Ratings"], errors="coerce")
    df["rating"] = rating.where(rating <= 5)          # 6.0 on a 1-5 scale is invalid

    partners = (df.groupby("Delivery_person_ID")
                  .agg(city_code=("city_code", "first"),
                       age=("age", "median"),
                       rating=("rating", "median"),
                       vehicle_type=("Type_of_vehicle",
                                     lambda s: s.mode().iat[0] if not s.mode().empty else None))
                  .reset_index()
                  .rename(columns={"Delivery_person_ID": "partner_id"}))
    partners["rating"] = partners["rating"].round(1)

    lat = pd.to_numeric(df["Restaurant_latitude"]).abs().round(6)
    lon = pd.to_numeric(df["Restaurant_longitude"]).abs().round(6)
    rest = pd.DataFrame({"city_code": df["city_code"], "lat": lat, "lon": lon})
    rest = rest[(rest.lat > 0) & (rest.lon > 0)].drop_duplicates().sort_values(["city_code", "lat", "lon"])
    rest["restaurant_id"] = rest["city_code"] + "-R" + (rest.groupby("city_code").cumcount() + 1).astype(str).str.zfill(3)
    return partners, rest[["restaurant_id", "city_code", "lat", "lon"]]


def main():
    import psycopg2
    import psycopg2.extras

    partners, restaurants = build_tables(read_raw())
    none = lambda v: None if pd.isna(v) else v  # noqa: E731
    with psycopg2.connect(config.PG_DSN) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO delivery_partners (partner_id, city_code, age, rating, vehicle_type)
            VALUES %s ON CONFLICT (partner_id) DO UPDATE
            SET city_code=EXCLUDED.city_code, age=EXCLUDED.age, rating=EXCLUDED.rating,
                vehicle_type=EXCLUDED.vehicle_type, updated_at=NOW()""",
            [(r.partner_id, r.city_code, none(r.age), none(r.rating), r.vehicle_type)
             for r in partners.itertuples()])
        psycopg2.extras.execute_values(cur, """
            INSERT INTO restaurants (restaurant_id, city_code, lat, lon) VALUES %s
            ON CONFLICT (restaurant_id) DO UPDATE SET lat=EXCLUDED.lat, lon=EXCLUDED.lon""",
            [tuple(r) for r in restaurants.itertuples(index=False)])
    print(f"seeded delivery_partners={len(partners)}  restaurants={len(restaurants)}")


if __name__ == "__main__":
    main()
