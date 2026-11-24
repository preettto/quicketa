import pandas as pd

from app.pipeline.seed_postgres import build_tables
from app.pipeline.transform import join_weather, light_transform


def raw_rows():
    base = {"Delivery_person_Age": "30", "Delivery_location_latitude": "22.76",
            "Delivery_location_longitude": "75.91", "Order_Date": "19-03-2022",
            "Time_Order_picked": "11:45:00", "Weatherconditions": "conditions Sunny",
            "Road_traffic_density": "High", "Vehicle_condition": "2", "Type_of_order": "Snack",
            "Type_of_vehicle": "motorcycle", "multiple_deliveries": "0", "Festival": "No",
            "City": "Urban", "Time_taken(min)": "(min) 24", "Delivery_person_Ratings": "4.8",
            "Time_Orderd": "11:30:00", "Restaurant_latitude": "22.745", "Restaurant_longitude": "75.892"}
    rows = [
        {**base, "ID": "a1", "Delivery_person_ID": "INDORES13DEL02"},                                  # clean
        {**base, "ID": "a2", "Delivery_person_ID": "INDORES13DEL03", "Restaurant_latitude": "-22.745"},  # repair
        {**base, "ID": "a3", "Delivery_person_ID": "INDORES13DEL04", "Restaurant_latitude": "0.0",
         "Restaurant_longitude": "0.0"},                                                               # reject
        {**base, "ID": "a4", "Delivery_person_ID": "INDORES13DEL05", "Delivery_person_Ratings": "6"},   # reject
        {**base, "ID": "a5", "Delivery_person_ID": "INDORES13DEL06", "Time_Orderd": None},               # impute
    ]
    return pd.DataFrame(rows)


def test_light_transform_rules_and_reconciliation():
    raw = raw_rows()
    partners, restaurants = build_tables(raw)
    clean, rejected, stats = light_transform(raw, partners[["partner_id", "age", "rating"]], restaurants)

    assert stats["input_rows"] == stats["clean_rows"] + stats["rejected_rows"] == 5
    assert stats["rejected_by_reason"] == {"zero_restaurant_coords": 1, "invalid_rating": 1}
    assert stats["repaired_negative_coords"] == 1
    assert stats["imputed_order_time"] == 1
    assert set(clean["order_id"]) == {"a1", "a2", "a5"}
    assert (clean["rest_lat"] > 0).all()
    assert (clean["city_code"] == "INDO").all()
    assert (clean["weather_condition"] == "Sunny").all()
    assert clean["actual_minutes"].tolist() == [24, 24, 24]
    assert clean["restaurant_id"].notna().all()


def test_weather_join_marks_missing():
    raw = raw_rows()
    partners, restaurants = build_tables(raw)
    clean, _, _ = light_transform(raw, partners[["partner_id", "age", "rating"]], restaurants)
    weather = pd.DataFrame({"city_code": ["INDO"], "ts_hour": [pd.Timestamp("2022-03-19 11:00")],
                            "temp_c": [30.0], "precip_mm": [0.0], "wind_kmh": [5.0]})
    joined, summary = join_weather(clean, weather)
    assert summary == {"api": 3}
    assert joined["temp_c"].eq(30.0).all()
