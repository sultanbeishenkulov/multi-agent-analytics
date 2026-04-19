"""
Seed the analytics database with realistic synthetic data.

Generates ~500K+ rows across all tables with realistic distributions:
- 8 regions, 10K customers, 200 products, 50 campaigns
- ~450K orders over 2 years with seasonal patterns
- ~25K support tickets with realistic resolution times
"""

import os
import random
from datetime import date, datetime, timedelta
from decimal import Decimal

from faker import Faker
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://analyst:analyst_pass@localhost:5432/analytics",
)

fake = Faker()
Faker.seed(42)
random.seed(42)

engine = create_engine(DATABASE_URL)

# ── Configuration ────────────────────────────────────────────

REGIONS = [
    ("North America", "US", "America/New_York"),
    ("Western Europe", "DE", "Europe/Berlin"),
    ("UK & Ireland", "GB", "Europe/London"),
    ("APAC", "JP", "Asia/Tokyo"),
    ("Latin America", "BR", "America/Sao_Paulo"),
    ("Nordics", "SE", "Europe/Stockholm"),
    ("ANZ", "AU", "Australia/Sydney"),
    ("Middle East", "AE", "Asia/Dubai"),
]

PRODUCT_CATALOG = {
    "Software": {
        "Analytics": [("DataViz Pro", 299), ("InsightEngine", 499), ("MetricsDash", 149), ("QueryBuilder", 199)],
        "Security": [("VaultGuard", 399), ("NetShield", 249), ("AuthFlow", 179), ("ThreatScan", 599)],
        "Productivity": [("TaskForge", 89), ("DocuSync", 129), ("CalendarAI", 69), ("NoteStream", 49)],
    },
    "Hardware": {
        "Networking": [("CloudRouter X1", 899), ("MeshPoint Pro", 349), ("FiberSwitch 48", 1299)],
        "Storage": [("RackStore 8TB", 599), ("FlashVault 2TB", 449), ("BackupBox", 279)],
        "Peripherals": [("ErgoBoard K7", 159), ("PrecisionMouse", 89), ("UltraMonitor 32", 749)],
    },
    "Services": {
        "Consulting": [("Architecture Review", 2500), ("Migration Planning", 3500), ("Security Audit", 4000)],
        "Training": [("Admin Bootcamp", 999), ("Developer Workshop", 1499), ("Exec Briefing", 799)],
        "Support Plans": [("Basic Support", 199), ("Premium Support", 499), ("Enterprise SLA", 1299)],
    },
}

CAMPAIGN_TEMPLATES = [
    ("Spring Launch {year}", "Email", 15000),
    ("Summer Sale {year}", "Social", 25000),
    ("Back to Business {year}", "Search", 30000),
    ("Black Friday {year}", "Display", 50000),
    ("Year-End Push {year}", "Affiliate", 20000),
    ("Product Hunt Launch {year}", "Social", 10000),
    ("Webinar Series {year}", "Email", 8000),
    ("Partner Co-Market {year}", "Affiliate", 12000),
    ("Retargeting Q{q} {year}", "Display", 18000),
    ("SEO Content Push {year}", "Search", 9000),
]

NUM_CUSTOMERS = 10_000
NUM_ORDERS = 450_000
NUM_TICKETS = 25_000
ORDER_START = date(2024, 1, 1)
ORDER_END = date(2025, 12, 31)

SEGMENTS = ["Enterprise", "SMB", "Consumer"]
SEGMENT_WEIGHTS = [0.15, 0.35, 0.50]


def seed_regions(conn):
    print("Seeding regions...")
    for name, country, tz in REGIONS:
        conn.execute(
            text("INSERT INTO regions (name, country, timezone) VALUES (:n, :c, :t) ON CONFLICT DO NOTHING"),
            {"n": name, "c": country, "t": tz},
        )
    conn.commit()


def seed_products(conn):
    print("Seeding products...")
    for category, subcats in PRODUCT_CATALOG.items():
        for subcat, items in subcats.items():
            for name, price in items:
                cost = round(price * random.uniform(0.3, 0.6), 2)
                conn.execute(
                    text(
                        "INSERT INTO products (name, category, subcategory, unit_price, cost_price) "
                        "VALUES (:n, :c, :s, :p, :cp) ON CONFLICT DO NOTHING"
                    ),
                    {"n": name, "c": category, "s": subcat, "p": price, "cp": cost},
                )
    conn.commit()


def seed_customers(conn):
    print(f"Seeding {NUM_CUSTOMERS} customers...")
    batch = []
    for i in range(NUM_CUSTOMERS):
        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        region_id = random.randint(1, len(REGIONS))
        signup = fake.date_between(start_date="-3y", end_date="today")
        batch.append({
            "email": f"user{i}@{fake.domain_name()}",
            "full_name": fake.name(),
            "segment": segment,
            "region_id": region_id,
            "signup_date": signup,
        })
        if len(batch) >= 1000:
            conn.execute(
                text(
                    "INSERT INTO customers (email, full_name, segment, region_id, signup_date) "
                    "VALUES (:email, :full_name, :segment, :region_id, :signup_date)"
                ),
                batch,
            )
            batch = []
    if batch:
        conn.execute(
            text(
                "INSERT INTO customers (email, full_name, segment, region_id, signup_date) "
                "VALUES (:email, :full_name, :segment, :region_id, :signup_date)"
            ),
            batch,
        )
    conn.commit()


def seed_campaigns(conn):
    print("Seeding campaigns...")
    for year in [2024, 2025]:
        for q in range(1, 5):
            for tmpl_name, channel, budget in CAMPAIGN_TEMPLATES:
                name = tmpl_name.format(year=year, q=q)
                start = date(year, (q - 1) * 3 + 1, 1)
                end = start + timedelta(days=random.randint(14, 60))
                region_id = random.choice([None, random.randint(1, len(REGIONS))])
                conn.execute(
                    text(
                        "INSERT INTO campaigns (name, channel, start_date, end_date, budget, region_id) "
                        "VALUES (:n, :ch, :s, :e, :b, :r)"
                    ),
                    {"n": name, "ch": channel, "s": start, "e": end, "b": budget, "r": region_id},
                )
    conn.commit()


def seed_orders(conn):
    print(f"Seeding {NUM_ORDERS} orders (this takes a moment)...")
    # Get product count
    result = conn.execute(text("SELECT COUNT(*) FROM products"))
    num_products = result.scalar()
    result = conn.execute(text("SELECT COUNT(*) FROM campaigns"))
    num_campaigns = result.scalar()

    total_days = (ORDER_END - ORDER_START).days
    batch = []

    for i in range(NUM_ORDERS):
        # Seasonal pattern: more orders in Q4, fewer in Q1
        day_offset = random.randint(0, total_days)
        order_date = ORDER_START + timedelta(days=day_offset)
        month = order_date.month

        # Seasonal weight: boost Q4
        if month in (11, 12):
            if random.random() > 0.6:
                day_offset = random.randint(
                    (date(order_date.year, 10, 1) - ORDER_START).days,
                    (date(order_date.year, 12, 31) - ORDER_START).days,
                )
                order_date = ORDER_START + timedelta(days=min(day_offset, total_days))

        customer_id = random.randint(1, NUM_CUSTOMERS)
        product_id = random.randint(1, num_products)
        campaign_id = random.choice([None, None, random.randint(1, num_campaigns)])

        # Get product price (we'll use a lookup in batches for speed)
        quantity = random.choices([1, 2, 3, 5, 10], weights=[50, 25, 15, 7, 3], k=1)[0]
        unit_price = round(random.uniform(49, 4000), 2)
        discount = random.choices(
            [0, 5, 10, 15, 20, 25],
            weights=[40, 20, 15, 10, 10, 5],
            k=1,
        )[0]
        status = random.choices(
            ["completed", "returned", "cancelled"],
            weights=[85, 10, 5],
            k=1,
        )[0]

        batch.append({
            "customer_id": customer_id,
            "product_id": product_id,
            "campaign_id": campaign_id,
            "order_date": order_date,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount,
            "status": status,
        })

        if len(batch) >= 5000:
            conn.execute(
                text(
                    "INSERT INTO orders (customer_id, product_id, campaign_id, order_date, "
                    "quantity, unit_price, discount_pct, status) "
                    "VALUES (:customer_id, :product_id, :campaign_id, :order_date, "
                    ":quantity, :unit_price, :discount_pct, :status)"
                ),
                batch,
            )
            conn.commit()
            batch = []
            if (i + 1) % 50000 == 0:
                print(f"  ...{i + 1}/{NUM_ORDERS} orders")

    if batch:
        conn.execute(
            text(
                "INSERT INTO orders (customer_id, product_id, campaign_id, order_date, "
                "quantity, unit_price, discount_pct, status) "
                "VALUES (:customer_id, :product_id, :campaign_id, :order_date, "
                ":quantity, :unit_price, :discount_pct, :status)"
            ),
            batch,
        )
    conn.commit()

    # Update customer lifetime values
    print("  Updating customer lifetime values...")
    conn.execute(text(
        "UPDATE customers c SET lifetime_value = sub.ltv "
        "FROM (SELECT customer_id, COALESCE(SUM(revenue), 0) AS ltv "
        "      FROM orders WHERE status = 'completed' GROUP BY customer_id) sub "
        "WHERE c.customer_id = sub.customer_id"
    ))
    conn.commit()


def seed_tickets(conn):
    print(f"Seeding {NUM_TICKETS} support tickets...")
    priorities = ["Low", "Medium", "High", "Critical"]
    priority_weights = [30, 40, 20, 10]
    categories = ["Billing", "Technical", "Shipping", "Returns", "General"]
    category_weights = [20, 30, 25, 15, 10]

    result = conn.execute(text("SELECT COUNT(*) FROM products"))
    num_products = result.scalar()

    batch = []
    for i in range(NUM_TICKETS):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        product_id = random.choice([None, random.randint(1, num_products)])
        created = fake.date_time_between(start_date="-2y", end_date="now")
        priority = random.choices(priorities, weights=priority_weights, k=1)[0]
        category = random.choices(categories, weights=category_weights, k=1)[0]

        # Resolution time depends on priority
        resolution_hours = {
            "Critical": random.uniform(1, 8),
            "High": random.uniform(4, 48),
            "Medium": random.uniform(24, 120),
            "Low": random.uniform(48, 240),
        }[priority]

        resolved = created + timedelta(hours=resolution_hours) if random.random() > 0.05 else None
        satisfaction = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30], k=1)[0] if resolved else None

        batch.append({
            "customer_id": customer_id,
            "product_id": product_id,
            "created_at": created,
            "resolved_at": resolved,
            "priority": priority,
            "category": category,
            "satisfaction": satisfaction,
        })

        if len(batch) >= 2000:
            conn.execute(
                text(
                    "INSERT INTO support_tickets (customer_id, product_id, created_at, resolved_at, "
                    "priority, category, satisfaction) "
                    "VALUES (:customer_id, :product_id, :created_at, :resolved_at, "
                    ":priority, :category, :satisfaction)"
                ),
                batch,
            )
            conn.commit()
            batch = []

    if batch:
        conn.execute(
            text(
                "INSERT INTO support_tickets (customer_id, product_id, created_at, resolved_at, "
                "priority, category, satisfaction) "
                "VALUES (:customer_id, :product_id, :created_at, :resolved_at, "
                ":priority, :category, :satisfaction)"
            ),
            batch,
        )
    conn.commit()


def print_stats(conn):
    print("\n── Database Stats ─────────────────────────")
    for table in ["regions", "customers", "products", "campaigns", "orders", "support_tickets"]:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        print(f"  {table:20s} {result.scalar():>10,} rows")
    print("───────────────────────────────────────────\n")


def main():
    print("Connecting to database...")
    with engine.connect() as conn:
        seed_regions(conn)
        seed_products(conn)
        seed_customers(conn)
        seed_campaigns(conn)
        seed_orders(conn)
        seed_tickets(conn)
        print_stats(conn)
    print("Done! Database is ready.")


if __name__ == "__main__":
    main()
