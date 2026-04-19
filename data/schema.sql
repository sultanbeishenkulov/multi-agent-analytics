-- ============================================================
-- Multi-Agent Analytics System — E-Commerce Star Schema
-- ============================================================
-- Star schema with 6 tables: 2 fact + 4 dimension
-- Designed for rich analytical queries: revenue trends,
-- customer cohorts, product performance, regional analysis.
-- ============================================================

-- ── Dimension: Regions ──────────────────────────────────────
CREATE TABLE regions (
    region_id   SERIAL PRIMARY KEY,
    name        VARCHAR(50)  NOT NULL UNIQUE,
    country     VARCHAR(50)  NOT NULL,
    timezone    VARCHAR(50)  NOT NULL
);

-- ── Dimension: Customers ────────────────────────────────────
CREATE TABLE customers (
    customer_id   SERIAL PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    full_name     VARCHAR(100) NOT NULL,
    segment       VARCHAR(20)  NOT NULL CHECK (segment IN ('Enterprise','SMB','Consumer')),
    region_id     INT          NOT NULL REFERENCES regions(region_id),
    signup_date   DATE         NOT NULL,
    lifetime_value NUMERIC(12,2) DEFAULT 0
);

CREATE INDEX idx_customers_segment ON customers(segment);
CREATE INDEX idx_customers_region  ON customers(region_id);
CREATE INDEX idx_customers_signup  ON customers(signup_date);

-- ── Dimension: Products ─────────────────────────────────────
CREATE TABLE products (
    product_id  SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    category    VARCHAR(50)  NOT NULL,
    subcategory VARCHAR(50)  NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
    cost_price  NUMERIC(10,2) NOT NULL CHECK (cost_price > 0),
    is_active   BOOLEAN       DEFAULT TRUE
);

CREATE INDEX idx_products_category ON products(category);

-- ── Dimension: Marketing Campaigns ──────────────────────────
CREATE TABLE campaigns (
    campaign_id SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    channel     VARCHAR(30)  NOT NULL CHECK (channel IN ('Email','Social','Search','Display','Affiliate')),
    start_date  DATE         NOT NULL,
    end_date    DATE,
    budget      NUMERIC(12,2) NOT NULL CHECK (budget > 0),
    region_id   INT          REFERENCES regions(region_id)
);

-- ── Fact: Orders ────────────────────────────────────────────
CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT          NOT NULL REFERENCES customers(customer_id),
    product_id   INT          NOT NULL REFERENCES products(product_id),
    campaign_id  INT          REFERENCES campaigns(campaign_id),
    order_date   DATE         NOT NULL,
    quantity     INT          NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10,2) NOT NULL,
    discount_pct NUMERIC(5,2) DEFAULT 0 CHECK (discount_pct >= 0 AND discount_pct <= 100),
    revenue      NUMERIC(12,2) GENERATED ALWAYS AS (
                    quantity * unit_price * (1 - discount_pct / 100)
                 ) STORED,
    status       VARCHAR(20)  NOT NULL DEFAULT 'completed'
                    CHECK (status IN ('completed','returned','cancelled'))
);

CREATE INDEX idx_orders_date       ON orders(order_date);
CREATE INDEX idx_orders_customer   ON orders(customer_id);
CREATE INDEX idx_orders_product    ON orders(product_id);
CREATE INDEX idx_orders_campaign   ON orders(campaign_id);
CREATE INDEX idx_orders_status     ON orders(status);

-- ── Fact: Support Tickets ───────────────────────────────────
CREATE TABLE support_tickets (
    ticket_id    SERIAL PRIMARY KEY,
    customer_id  INT          NOT NULL REFERENCES customers(customer_id),
    product_id   INT          REFERENCES products(product_id),
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMP,
    priority     VARCHAR(10)  NOT NULL CHECK (priority IN ('Low','Medium','High','Critical')),
    category     VARCHAR(30)  NOT NULL CHECK (category IN ('Billing','Technical','Shipping','Returns','General')),
    satisfaction INT          CHECK (satisfaction BETWEEN 1 AND 5)
);

CREATE INDEX idx_tickets_customer  ON support_tickets(customer_id);
CREATE INDEX idx_tickets_created   ON support_tickets(created_at);
CREATE INDEX idx_tickets_priority  ON support_tickets(priority);

-- ── Utility Views ───────────────────────────────────────────

-- Monthly revenue summary
CREATE VIEW monthly_revenue AS
SELECT
    DATE_TRUNC('month', o.order_date)::DATE AS month,
    r.name AS region,
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.revenue) AS total_revenue,
    AVG(o.revenue) AS avg_order_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN regions r   ON c.region_id = r.region_id
JOIN products p  ON o.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY 1, 2, 3;

-- Customer cohort view
CREATE VIEW customer_cohorts AS
SELECT
    c.customer_id,
    c.segment,
    DATE_TRUNC('month', c.signup_date)::DATE AS cohort_month,
    r.name AS region,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.revenue), 0) AS total_spent
FROM customers c
JOIN regions r ON c.region_id = r.region_id
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'completed'
GROUP BY c.customer_id, c.segment, cohort_month, r.name;
