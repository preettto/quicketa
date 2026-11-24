"""light 't' stage (EtLT) - validate, clean, repair, conform, join.  NO heavy feature logic here.

Rules derived from the data-quality profiling done in Phase 2:
  R1  strip whitespace; the literal string 'NaN' -> real null
  R2  target '(min) 24' -> 24 (int)
  R3  negative restaurant coordinates -> repaired with abs()        (sign error)
  R4  restaurant coordinates == 0     -> REJECT  'zero_restaurant_coords'
  R5  partner rating > 5              -> REJECT  'invalid_rating'
  R6  'conditions Sunny' -> 'Sunny'
  R7  missing order time -> imputed as pickup time - median(pickup gap)
  R8  partner age/rating taken from the partner DB (source of truth), restaurant_id joined from DB
  R9  hourly weather joined by (city_code, order hour)
"""
import pandas as pd

from app.pipeline.seed_postgres import city_code_of

CANONICAL = ["order_id", "partner_id", "city_code", "restaurant_id",
             "rest_lat", "rest_lon", "del_lat", "del_lon", "order_ts",
             "traffic", "weather_condition", "vehicle_type", "vehicle_condition", "order_type",
             "multiple_deliveries", "festival", "city_type", "partner_age", "partner_rating",
             "actual_minutes"]


def light_transform(raw: pd.DataFrame, partners: pd.DataFrame, restaurants: pd.DataFrame):
    stats = {"input_rows": int(len(raw))}
    df = raw.copy()                                                  # R1 already applied by read_raw

    df["actual_minutes"] = df["Time_taken(min)"].str.replace("(min)", "", regex=False).str.strip().astype(int)  # R2

    for src, dst in [("Restaurant_latitude", "rest_lat"), ("Restaurant_longitude", "rest_lon"),
                     ("Delivery_location_latitude", "del_lat"), ("Delivery_location_longitude", "del_lon")]:
        df[dst] = pd.to_numeric(df[src], errors="coerce")
    neg = (df["rest_lat"] < 0) | (df["rest_lon"] < 0)
    stats["repaired_negative_coords"] = int(neg.sum())               # R3
    df["rest_lat"], df["rest_lon"] = df["rest_lat"].abs(), df["rest_lon"].abs()

    reason = pd.Series(None, index=df.index, dtype=object)
    reason[(df["rest_lat"] == 0) | (df["rest_lon"] == 0)] = "zero_restaurant_coords"          # R4
    raw_rating = pd.to_numeric(df["Delivery_person_Ratings"], errors="coerce")
    reason[reason.isna() & (raw_rating > 5)] = "invalid_rating"                                  # R5

    rejected = df[reason.notna()].assign(reject_reason=reason[reason.notna()])
    df = df[reason.isna()].copy()
    stats["rejected_by_reason"] = {k: int(v) for k, v in rejected["reject_reason"].value_counts().items()}
    stats["rejected_rows"] = int(len(rejected))

    df["order_id"] = df["ID"]
    df["partner_id"] = df["Delivery_person_ID"]
    df["city_code"] = city_code_of(df["partner_id"])
    df["weather_condition"] = df["Weatherconditions"].str.replace("conditions", "", regex=False).str.strip()  # R6
    df["weather_condition"] = df["weather_condition"].replace({"NaN": None, "": None})

    picked = pd.to_datetime(df["Order_Date"] + " " + df["Time_Order_picked"], format="%d-%m-%Y %H:%M:%S")
    ordered = pd.to_datetime(df["Order_Date"] + " " + df["Time_Orderd"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
    gap = (picked - ordered).dt.total_seconds().div(60) % 1440       # handles orders crossing midnight
    median_gap = float(gap.median())
    missing = ordered.isna()
    ordered[missing] = picked[missing] - pd.Timedelta(minutes=median_gap)                        # R7
    df["order_ts"] = ordered
    stats["imputed_order_time"] = int(missing.sum())
    stats["median_pickup_gap_min"] = median_gap

    df = df.rename(columns={"Road_traffic_density": "traffic", "Type_of_vehicle": "vehicle_type",
                            "Vehicle_condition": "vehicle_condition", "Type_of_order": "order_type",
                            "multiple_deliveries": "multiple_deliveries", "Festival": "festival",
                            "City": "city_type"})
    df["vehicle_condition"] = pd.to_numeric(df["vehicle_condition"], errors="coerce")
    df["multiple_deliveries"] = pd.to_numeric(df["multiple_deliveries"], errors="coerce")

    p = partners.rename(columns={"age": "partner_age", "rating": "partner_rating"})               # R8
    df = df.merge(p[["partner_id", "partner_age", "partner_rating"]], on="partner_id", how="left")
    r = restaurants.assign(rest_lat=restaurants["lat"].round(6), rest_lon=restaurants["lon"].round(6))
    df["rest_lat"], df["rest_lon"] = df["rest_lat"].round(6), df["rest_lon"].round(6)
    df = df.merge(r[["city_code", "rest_lat", "rest_lon", "restaurant_id"]],
                  on=["city_code", "rest_lat", "rest_lon"], how="left")

    stats["clean_rows"] = int(len(df))
    out = df[CANONICAL]
    stats["nulls_after_clean"] = {k: int(v) for k, v in out.isna().sum().items() if v}
    return out, rejected, stats


def city_centroids(clean: pd.DataFrame) -> pd.DataFrame:
    """One representative coordinate per city, used for the weather API calls."""
    return (clean.groupby("city_code")[["rest_lat", "rest_lon"]].median()
                 .rename(columns={"rest_lat": "lat", "rest_lon": "lon"}).reset_index())


def join_weather(clean: pd.DataFrame, weather: pd.DataFrame):
    """R9: attach hourly weather by (city_code, hour of order)."""
    df = clean.copy()
    df["ts_hour"] = pd.to_datetime(df["order_ts"]).dt.floor("h")
    df = df.merge(weather, on=["city_code", "ts_hour"], how="left").drop(columns="ts_hour")
    df["weather_source"] = df["temp_c"].notna().map({True: "api", False: "missing"})
    return df, {k: int(v) for k, v in df["weather_source"].value_counts().items()}
