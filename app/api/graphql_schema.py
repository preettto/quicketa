"""GraphQL schema (P2). Same data as REST, but the CLIENT chooses the fields and filters.

Example query:
  { orders(cityCode: "INDO", minEta: 30, limit: 5) { orderId etaMinutes traffic weather { condition tempC } } }
"""
import graphene

from app.services import orders as svc
from app.services.storage import mongo_db


class Weather(graphene.ObjectType):
    condition = graphene.String()
    temp_c = graphene.Float()
    precip_mm = graphene.Float()
    wind_kmh = graphene.Float()
    source = graphene.String()


class Order(graphene.ObjectType):
    order_id = graphene.String()
    source = graphene.String()
    city_code = graphene.String()
    partner_id = graphene.String()
    order_ts = graphene.String()
    traffic = graphene.String()
    vehicle_type = graphene.String()
    order_type = graphene.String()
    eta_minutes = graphene.Float()
    actual_minutes = graphene.Int()
    served_by = graphene.String()
    weather = graphene.Field(Weather)


class CityStats(graphene.ObjectType):
    city_code = graphene.String()
    orders = graphene.Int()
    avg_actual_minutes = graphene.Float()
    avg_predicted_minutes = graphene.Float()


def _to_order(d: dict) -> Order:
    w = d.get("weather") or {}
    return Order(order_id=d["order_id"], source=d.get("source"), city_code=d.get("city_code"),
                 partner_id=d.get("partner_id"), order_ts=str(d.get("order_ts")),
                 traffic=d.get("traffic"), vehicle_type=d.get("vehicle_type"),
                 order_type=d.get("order_type"), eta_minutes=d.get("predicted_minutes"),
                 actual_minutes=d.get("actual_minutes"), served_by=d.get("served_by"),
                 weather=Weather(**{k: w.get(k) for k in ("condition", "temp_c", "precip_mm", "wind_kmh", "source")}))


class Query(graphene.ObjectType):
    order = graphene.Field(Order, order_id=graphene.String(required=True))
    orders = graphene.List(Order, city_code=graphene.String(), source=graphene.String(),
                           traffic=graphene.String(), min_eta=graphene.Float(), limit=graphene.Int(default_value=10))
    city_stats = graphene.List(CityStats, city_code=graphene.String())

    def resolve_order(self, info, order_id):
        d = svc.get_order(order_id)
        return _to_order(d) if d else None

    def resolve_orders(self, info, city_code=None, source=None, traffic=None, min_eta=None, limit=10):
        q = {}
        if city_code:
            q["city_code"] = city_code
        if source:
            q["source"] = source
        if traffic:
            q["traffic"] = traffic
        if min_eta is not None:
            q["predicted_minutes"] = {"$gte": min_eta}
        cur = mongo_db()["orders"].find(q, {"_id": 0}).sort("order_ts", -1).limit(min(limit, 100))
        return [_to_order(d) for d in cur]

    def resolve_city_stats(self, info, city_code=None):
        match = {"city_code": city_code} if city_code else {}
        pipeline = [{"$match": match},
                    {"$group": {"_id": "$city_code", "orders": {"$sum": 1},
                                "avg_actual": {"$avg": "$actual_minutes"},
                                "avg_pred": {"$avg": "$predicted_minutes"}}},
                    {"$sort": {"_id": 1}}]
        return [CityStats(city_code=r["_id"], orders=r["orders"],
                          avg_actual_minutes=round(r["avg_actual"], 2) if r["avg_actual"] else None,
                          avg_predicted_minutes=round(r["avg_pred"], 2) if r["avg_pred"] else None)
                for r in mongo_db()["orders"].aggregate(pipeline)]


class LocationInput(graphene.InputObjectType):
    lat = graphene.Float(required=True)
    lon = graphene.Float(required=True)


class OrderInput(graphene.InputObjectType):
    partner_id = graphene.String(required=True)
    restaurant = graphene.Field(LocationInput, required=True)
    delivery = graphene.Field(LocationInput, required=True)
    traffic = graphene.String(required=True)
    vehicle_type = graphene.String(required=True)
    order_type = graphene.String(required=True)
    vehicle_condition = graphene.Int()
    multiple_deliveries = graphene.Int()
    festival = graphene.String()
    city_type = graphene.String()


class PlaceOrder(graphene.Mutation):
    class Arguments:
        input = OrderInput(required=True)

    order_id = graphene.String()
    eta_minutes = graphene.Float()
    eta_time = graphene.String()
    served_by = graphene.String()

    def mutate(self, info, input):
        body = {k: v for k, v in dict(input).items() if v is not None}
        body["restaurant"], body["delivery"] = dict(body["restaurant"]), dict(body["delivery"])
        r = svc.place_order(body)
        return PlaceOrder(order_id=r["order_id"], eta_minutes=r["eta_minutes"],
                          eta_time=r["eta_time"], served_by=r["served_by"])


class Mutation(graphene.ObjectType):
    place_order = PlaceOrder.Field()


schema = graphene.Schema(query=Query, mutation=Mutation)
