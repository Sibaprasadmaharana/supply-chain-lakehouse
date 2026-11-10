#!/usr/bin/env python
"""
Source-system simulator for the Supply Chain Analytics Lakehouse.

Behaves like an Order Management System (OMS) writing to Postgres:
new customers and orders arrive, orders ship and get delivered, customers
move cities (drives SCD Type 2), seller ratings drift and product prices change.

It also injects the messy realities a production pipeline must survive:
  * late-arriving records  (updated_at back-dated by a day ~3% of shipments)
  * bad records            (negative freight cost, delivered before shipped)
  * no-op updates          (a customer "moves" to the same city)
  * an operational incident (one carrier has a recurring mid-month disruption)

Business time is SIMULATED: every step is one calendar day, so months of
history can be built in seconds and every run is reproducible (seeded RNG).

Usage:
    python generator/generate.py seed  --days 60
    python generator/generate.py batch --days 1
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "source_db" / "schema.sql"

PG_DSN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "supply"),
    user=os.getenv("PG_USER", "de_user"),
    password=os.getenv("PG_PASSWORD", "de_pass"),
)

CITIES = [
    ("Hyderabad", "TG"), ("Bengaluru", "KA"), ("Mumbai", "MH"), ("Pune", "MH"),
    ("Chennai", "TN"), ("Delhi", "DL"), ("Kolkata", "WB"), ("Ahmedabad", "GJ"),
    ("Jaipur", "RJ"), ("Lucknow", "UP"), ("Kochi", "KL"), ("Indore", "MP"),
    ("Bhubaneswar", "OD"), ("Visakhapatnam", "AP"), ("Chandigarh", "CH"),
]
FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Ishaan", "Rohan", "Arjun", "Kabir", "Rahul",
    "Ananya", "Diya", "Priya", "Sneha", "Kavya", "Meera", "Pooja", "Nisha",
    "Sai", "Kiran", "Ravi", "Lakshmi", "Suresh", "Deepa", "Amit", "Neha",
]
LAST_NAMES = [
    "Sharma", "Reddy", "Iyer", "Nair", "Patel", "Gupta", "Rao", "Das",
    "Mehta", "Singh", "Mishra", "Joshi", "Kulkarni", "Menon", "Bose", "Naidu",
]
SELLER_PREFIX = ["Sri", "Om", "Galaxy", "Urban", "Prime", "Royal", "Green", "Star", "Metro", "Lotus"]
SELLER_SUFFIX = ["Traders", "Retail", "Enterprises", "Mart", "Distributors", "Store", "Wholesale"]
SEGMENTS = [("consumer", 0.70), ("small_business", 0.22), ("enterprise", 0.08)]
CATEGORIES = {
    # category: (min_price, max_price, min_kg, max_kg)
    "electronics": (999, 45000, 0.2, 4.0),
    "home_kitchen": (199, 9000, 0.3, 12.0),
    "fashion": (299, 6000, 0.1, 1.5),
    "books": (99, 1500, 0.2, 1.2),
    "sports": (299, 15000, 0.3, 15.0),
    "beauty": (99, 3000, 0.05, 0.8),
    "furniture": (1999, 60000, 5.0, 40.0),
    "grocery": (49, 1200, 0.2, 10.0),
}
# name, service_level, transit_days, base_rate, rate_per_kg, delay_probability, selection_weight
CARRIERS = [
    ("SwiftAir Express", "express", 1, 120.0, 38.0, 0.04, 0.15),
    ("MetroLink Couriers", "standard", 2, 60.0, 18.0, 0.10, 0.30),
    ("RoadRunner Logistics", "standard", 3, 50.0, 14.0, 0.14, 0.25),
    ("Coastal Freight", "economy", 4, 35.0, 9.0, 0.18, 0.20),
    ("Northstar Cargo", "economy", 5, 30.0, 7.5, 0.25, 0.10),
]
INCIDENT_CARRIER = "Coastal Freight"  # recurring disruption on days 10-17 of each month


def connect():
    return psycopg2.connect(**PG_DSN)


def weighted(rng: random.Random, pairs):
    values, weights = zip(*pairs)
    return rng.choices(values, weights=weights, k=1)[0]


def at(day: date, rng: random.Random, start_hour: int = 8, end_hour: int = 22) -> datetime:
    """Random timestamp on `day` between start_hour and end_hour."""
    return datetime.combine(day, time(0)) + timedelta(
        seconds=rng.randint(start_hour * 3600, end_hour * 3600 - 1)
    )


def fake_customer(rng: random.Random, created_at: datetime):
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    city, state = rng.choice(CITIES)
    email = f"{first}.{last}{rng.randint(1, 9999)}@example.com".lower()
    return (f"{first} {last}", email, city, state, weighted(rng, SEGMENTS), created_at, created_at)


# --------------------------------------------------------------------------- seed
def seed(conn, start: date, days: int, orders_per_day: int, base_seed: int) -> None:
    rng = random.Random(base_seed)
    origin = datetime.combine(start - timedelta(days=1), time(0))
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text())
        cur.execute(
            "truncate shop._sim_plan, shop.shipments, shop.order_items, shop.orders, "
            "shop.customers, shop.products, shop.sellers, shop.carriers, shop._sim_clock "
            "restart identity cascade"
        )
        execute_values(
            cur,
            "insert into shop.carriers (carrier_name, service_level, created_at, updated_at) values %s",
            [(c[0], c[1], origin, origin) for c in CARRIERS],
        )
        sellers = []
        for i in range(40):
            city, state = rng.choice(CITIES)
            created = origin - timedelta(days=rng.randint(30, 700))
            sellers.append((
                f"{rng.choice(SELLER_PREFIX)} {rng.choice(SELLER_SUFFIX)} {i + 1:02d}",
                city, state, round(rng.uniform(3.2, 4.9), 1), created, created,
            ))
        execute_values(
            cur,
            "insert into shop.sellers (seller_name, city, state, rating, created_at, updated_at) values %s",
            sellers,
        )
        products = []
        for i in range(150):
            category = rng.choice(list(CATEGORIES))
            pmin, pmax, wmin, wmax = CATEGORIES[category]
            created = origin - timedelta(days=rng.randint(30, 500))
            products.append((
                f"{category.replace('_', ' ').title()} Item {i + 1:03d}", category,
                round(rng.uniform(pmin, pmax), 2), round(rng.uniform(wmin, wmax), 2), created, created,
            ))
        execute_values(
            cur,
            "insert into shop.products (product_name, category, unit_price, weight_kg, created_at, updated_at) "
            "values %s",
            products,
        )
        execute_values(
            cur,
            "insert into shop.customers (customer_name, email, city, state, segment, created_at, updated_at) "
            "values %s",
            [fake_customer(rng, origin - timedelta(days=rng.randint(1, 900))) for _ in range(400)],
        )
        cur.execute("insert into shop._sim_clock (sim_date) values (%s)", (start - timedelta(days=1),))
    conn.commit()
    print(f"[generator] master data seeded; simulating {days} days from {start}")
    for i in range(days):
        simulate_day(conn, start + timedelta(days=i), orders_per_day, base_seed)


# --------------------------------------------------------------------------- one simulated day
def simulate_day(conn, day: date, orders_per_day: int, base_seed: int) -> None:
    rng = random.Random(f"{base_seed}:{day.isoformat()}")  # reproducible per day
    day_start = datetime.combine(day, time(0))
    day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
    stats = dict(new_customers=0, orders=0, cancelled=0, shipped=0, delivered=0, scd_changes=0)

    with conn.cursor() as cur:
        # 1) New customers sign up
        new_customers = [fake_customer(rng, at(day, rng)) for _ in range(rng.randint(2, 6))]
        execute_values(
            cur,
            "insert into shop.customers (customer_name, email, city, state, segment, created_at, updated_at) "
            "values %s",
            new_customers,
        )
        stats["new_customers"] = len(new_customers)

        cur.execute("select customer_id from shop.customers where created_at < %s", (day_start,))
        customer_ids = [r[0] for r in cur.fetchall()]
        cur.execute("select product_id, unit_price from shop.products")
        products = cur.fetchall()
        cur.execute("select seller_id from shop.sellers")
        seller_ids = [r[0] for r in cur.fetchall()]
        cur.execute("select carrier_name, carrier_id from shop.carriers")
        carrier_ids = dict(cur.fetchall())

        # 2) New orders (weekends are busier)
        factor = 1.25 if day.weekday() >= 5 else 1.0
        n_orders = max(1, int(rng.gauss(orders_per_day * factor, orders_per_day * 0.15)))
        for _ in range(n_orders):
            order_ts = at(day, rng, 6, 23)
            promised = (order_ts + timedelta(days=rng.randint(4, 7))).date()
            cur.execute(
                "insert into shop.orders (customer_id, order_status, order_ts, promised_delivery_date, "
                "created_at, updated_at) values (%s, 'placed', %s, %s, %s, %s) returning order_id",
                (rng.choice(customer_ids), order_ts, promised, order_ts, order_ts),
            )
            order_id = cur.fetchone()[0]
            items = []
            for _ in range(weighted(rng, [(1, 0.55), (2, 0.25), (3, 0.12), (4, 0.08)])):
                product_id, price = rng.choice(products)
                qty = weighted(rng, [(1, 0.80), (2, 0.15), (3, 0.05)])
                items.append((order_id, product_id, rng.choice(seller_ids), qty, price, order_ts, order_ts))
            execute_values(
                cur,
                "insert into shop.order_items (order_id, product_id, seller_id, quantity, unit_price, "
                "created_at, updated_at) values %s",
                items,
            )
        stats["orders"] = n_orders

        # 3) A few open orders get cancelled
        cur.execute(
            "select order_id from shop.orders where order_status = 'placed' and order_ts < %s", (day_start,)
        )
        for (order_id,) in cur.fetchall():
            if rng.random() < 0.015:
                cur.execute(
                    "update shop.orders set order_status = 'cancelled', updated_at = %s where order_id = %s",
                    (at(day, rng), order_id),
                )
                stats["cancelled"] += 1

        # 4) Open orders ship
        cur.execute(
            """
            select o.order_id, o.order_ts, coalesce(sum(i.quantity * p.weight_kg), 0) as weight_kg
            from shop.orders o
            join shop.order_items i on i.order_id = o.order_id
            join shop.products p on p.product_id = i.product_id
            where o.order_status = 'placed' and o.order_ts < %s
            group by o.order_id, o.order_ts
            """,
            (day_start,),
        )
        for order_id, order_ts, weight_kg in cur.fetchall():
            waiting_days = (day - order_ts.date()).days
            if waiting_days < 3 and rng.random() > 0.7:
                continue
            name, _level, transit, base, per_kg, delay_p, _w = weighted(rng, [(c, c[6]) for c in CARRIERS])
            if name == INCIDENT_CARRIER and 10 <= day.day <= 17:
                delay_p = 0.60
            shipped_ts = at(day, rng, 7, 20)
            freight = round((base + per_kg * float(weight_kg)) * rng.uniform(0.95, 1.08), 2)
            delay = rng.randint(1, 4) if rng.random() < delay_p else 0
            planned = shipped_ts + timedelta(days=transit + delay, hours=rng.randint(-6, 6))

            roll = rng.random()  # inject bad data
            if roll < 0.006:
                freight = -freight
            elif roll < 0.012:
                planned = shipped_ts - timedelta(hours=rng.randint(2, 30))

            # late-arriving record: OMS writes it with yesterday's timestamp
            record_ts = shipped_ts - timedelta(days=1) if rng.random() < 0.03 else shipped_ts
            cur.execute(
                "insert into shop.shipments (order_id, carrier_id, shipped_ts, delivered_ts, freight_cost, "
                "shipment_status, created_at, updated_at) values (%s, %s, %s, null, %s, 'in_transit', %s, %s) "
                "returning shipment_id",
                (order_id, carrier_ids[name], shipped_ts, freight, record_ts, record_ts),
            )
            shipment_id = cur.fetchone()[0]
            cur.execute("insert into shop._sim_plan values (%s, %s)", (shipment_id, planned))
            cur.execute(
                "update shop.orders set order_status = 'shipped', updated_at = %s where order_id = %s",
                (shipped_ts, order_id),
            )
            stats["shipped"] += 1

        # 5) In-transit shipments get delivered
        cur.execute(
            """
            select s.shipment_id, s.order_id, p.planned_delivery_ts
            from shop.shipments s
            join shop._sim_plan p on p.shipment_id = s.shipment_id
            where s.shipment_status = 'in_transit' and p.planned_delivery_ts <= %s
            """,
            (day_end,),
        )
        for shipment_id, order_id, planned in cur.fetchall():
            event_ts = max(planned, day_start)
            cur.execute(
                "update shop.shipments set delivered_ts = %s, shipment_status = 'delivered', updated_at = %s "
                "where shipment_id = %s",
                (planned, event_ts, shipment_id),
            )
            cur.execute(
                "update shop.orders set order_status = 'delivered', updated_at = %s where order_id = %s",
                (event_ts, order_id),
            )
            stats["delivered"] += 1

        # 6) Slowly changing attributes
        cur.execute("select customer_id, segment from shop.customers where created_at < %s", (day_start,))
        for customer_id, segment in cur.fetchall():
            r = rng.random()
            if r < 0.008:  # moves city (may be the same city -> a no-op change)
                city, state = rng.choice(CITIES)
                cur.execute(
                    "update shop.customers set city = %s, state = %s, updated_at = %s where customer_id = %s",
                    (city, state, at(day, rng), customer_id),
                )
                stats["scd_changes"] += 1
            elif r < 0.010:  # changes segment
                new_segment = rng.choice([s for s, _ in SEGMENTS if s != segment])
                cur.execute(
                    "update shop.customers set segment = %s, updated_at = %s where customer_id = %s",
                    (new_segment, at(day, rng), customer_id),
                )
                stats["scd_changes"] += 1
            elif r < 0.013:  # changes e-mail (Type 1 attribute)
                cur.execute(
                    "update shop.customers set email = replace(email, '@example.com', '@mail.example.com'), "
                    "updated_at = %s where customer_id = %s",
                    (at(day, rng), customer_id),
                )

        cur.execute("select seller_id, rating from shop.sellers")
        for seller_id, rating in cur.fetchall():
            if rng.random() < 0.03:
                new_rating = min(5.0, max(1.0, round(float(rating) + rng.choice([-0.2, -0.1, 0.1, 0.2]), 1)))
                cur.execute(
                    "update shop.sellers set rating = %s, updated_at = %s where seller_id = %s",
                    (new_rating, at(day, rng), seller_id),
                )

        for product_id, _price in products:
            if rng.random() < 0.01:
                cur.execute(
                    "update shop.products set unit_price = round(unit_price * %s, 2), updated_at = %s "
                    "where product_id = %s",
                    (round(rng.uniform(0.90, 1.12), 4), at(day, rng), product_id),
                )

        cur.execute("update shop._sim_clock set sim_date = %s", (day,))
    conn.commit()
    print(
        f"[generator] {day}: {stats['orders']} orders, {stats['shipped']} shipped, "
        f"{stats['delivered']} delivered, {stats['cancelled']} cancelled, "
        f"{stats['new_customers']} new customers, {stats['scd_changes']} customer changes"
    )


# --------------------------------------------------------------------------- batch
def batch(conn, days: int, orders_per_day: int, base_seed: int) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute("select max(sim_date) from shop._sim_clock")
            current = cur.fetchone()[0]
    except psycopg2.errors.UndefinedTable:
        sys.exit("Source database is empty. Run: python generator/generate.py seed")
    if current is None:
        sys.exit("Source database is empty. Run: python generator/generate.py seed")
    for i in range(1, days + 1):
        simulate_day(conn, current + timedelta(days=i), orders_per_day, base_seed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate an order management system in Postgres.")
    parser.add_argument("--orders-per-day", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    sub = parser.add_subparsers(dest="command", required=True)
    p_seed = sub.add_parser("seed", help="(re)create source data with N days of history")
    p_seed.add_argument("--days", type=int, default=60)
    p_seed.add_argument("--start", type=date.fromisoformat, default=None,
                        help="first simulated day (YYYY-MM-DD); default = today - days - 14")
    p_batch = sub.add_parser("batch", help="advance the simulation by N days")
    p_batch.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    conn = connect()
    try:
        if args.command == "seed":
            start = args.start or (date.today() - timedelta(days=args.days + 14))
            seed(conn, start, args.days, args.orders_per_day, args.seed)
        else:
            batch(conn, args.days, args.orders_per_day, args.seed)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
