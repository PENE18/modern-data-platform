-- ============================================================
-- DREMIO ETL: nessie.bronze → nessie.silver → nessie.gold
-- Iceberg sur Nessie : Time Travel, MERGE, Schema Evolution,
-- Compaction, Branches
-- ============================================================
--
-- SCHÉMAS BRONZE RÉELS (Spark):
--
-- nessie.bronze.customers
--   customer_id, email, first_name, last_name, city, state,
--   registration_date, customer_segment, ingested_at
--
-- nessie.bronze.orders
--   order_id, customer_id, order_date, product_id, quantity,
--   status, payment_method, total_amount, ingested_at, order_source
--
-- nessie.bronze.products
--   product_id, product_name, category, price, stock_quantity, ingested_at
--
-- ============================================================


-- ============================================================
-- SECTION 0 : Exploration rapide du bronze
-- ============================================================

SELECT * FROM nessie.bronze.orders     LIMIT 10;
SELECT * FROM nessie.bronze.customers  LIMIT 10;
SELECT * FROM nessie.bronze.products   LIMIT 10;

-- Fraîcheur des données
SELECT
    'bronze.orders'    AS table_name,
    COUNT(*)           AS row_count,
    MAX(ingested_at)   AS last_ingested
FROM nessie.bronze.orders
UNION ALL
SELECT 'bronze.customers', COUNT(*), MAX(ingested_at) FROM nessie.bronze.customers
UNION ALL
SELECT 'bronze.products',  COUNT(*), MAX(ingested_at) FROM nessie.bronze.products;

-- Valeurs distinctes utiles pour comprendre les données
SELECT DISTINCT status           FROM nessie.bronze.orders;
SELECT DISTINCT payment_method   FROM nessie.bronze.orders;
SELECT DISTINCT order_source     FROM nessie.bronze.orders;
SELECT DISTINCT customer_segment FROM nessie.bronze.customers;
SELECT DISTINCT category         FROM nessie.bronze.products;


-- ============================================================
-- SECTION 1 : BRANCHES NESSIE
-- ============================================================

CREATE BRANCH IF NOT EXISTS silver_dev        IN nessie;
CREATE BRANCH IF NOT EXISTS gold_experiment   IN nessie;

SHOW BRANCHES IN nessie;

-- Toutes les écritures silver se font sur silver_dev
USE BRANCH silver_dev IN nessie;


-- ============================================================
-- SECTION 2 : SILVER — Création des tables
-- ============================================================

-- ---------------------------------------------------------
-- Silver: orders_enriched_V2
-- Jointure orders + customers + products + colonnes calculées
-- Partition par (order_year, order_month)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.silver.orders_enriched_V2 (

    -- Clés
    order_id                    VARCHAR,
    customer_id                 VARCHAR,
    product_id                  VARCHAR,

    -- Temps
    order_date                  DATE,
    order_year                  INTEGER,
    order_month                 INTEGER,
    order_quarter               INTEGER,
    day_of_week                 INTEGER,
    is_weekend                  BOOLEAN,

    -- Commande brute
    quantity                    INTEGER,
    status                      VARCHAR,
    payment_method              VARCHAR,
    order_source                VARCHAR,
    channel_category            VARCHAR,   -- Online / In-Store / Partner

    -- Finance calculée
    unit_price                  DOUBLE,
    subtotal                    DOUBLE,
    discount_amount             DOUBLE,
    tax_amount                  DOUBLE,
    shipping_cost               DOUBLE,
    total_amount                DOUBLE,
    profit_margin_pct           DOUBLE,

    -- Client dénormalisé
    customer_name               VARCHAR,
    customer_email              VARCHAR,
    customer_segment            VARCHAR,
    customer_city               VARCHAR,
    customer_state              VARCHAR,
    customer_lifetime_orders    BIGINT,

    -- Produit dénormalisé
    product_name                VARCHAR,
    product_category            VARCHAR,

    -- Tiers & flags
    order_value_tier            VARCHAR,
    customer_value_tier         VARCHAR,
    is_first_order              BOOLEAN,
    is_repeat_customer          BOOLEAN,

    -- Qualité ETL
    data_quality_flag           VARCHAR,
    processed_at                TIMESTAMP
)
PARTITION BY (order_year, order_month)
TBLPROPERTIES (
    'write.format.default'               = 'parquet',
    'write.parquet.compression-codec'    = 'snappy',
    'history.expire.max-snapshot-age-ms' = '604800000'
);


-- ---------------------------------------------------------
-- Silver: customer_profiles (SCD Type 1)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.silver.customer_profiles (
    customer_id             VARCHAR,
    customer_name           VARCHAR,
    customer_email          VARCHAR,
    customer_segment        VARCHAR,
    customer_city           VARCHAR,
    customer_state          VARCHAR,
    registration_date       DATE,
    first_order_date        DATE,
    last_order_date         DATE,
    total_orders            INTEGER,
    lifetime_value          DOUBLE,
    avg_order_value         DOUBLE,
    customer_tier           VARCHAR,
    churn_risk_score        DOUBLE,
    is_active               BOOLEAN,
    updated_at              TIMESTAMP
)
PARTITION BY (customer_segment)
TBLPROPERTIES (
    'write.format.default'       = 'parquet',
    'write.metadata.delete-mode' = 'merge-on-read'
);


-- ============================================================
-- SECTION 3 : SILVER — Chargement initial
-- ============================================================

-- ---------------------------------------------------------
-- 3A. orders_enriched_V2
-- ---------------------------------------------------------
INSERT INTO nessie.silver.orders_enriched_V2
WITH customer_history AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        MIN(order_date) OVER (PARTITION BY customer_id) AS first_order_date,
        COUNT(order_id) OVER (PARTITION BY customer_id) AS lifetime_orders
    FROM nessie.bronze.orders
    WHERE status != 'CANCELLED'
      AND order_id IS NOT NULL
)
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,

    o.order_date,
    CAST(YEAR(o.order_date)      AS INTEGER)                AS order_year,
    CAST(MONTH(o.order_date)     AS INTEGER)                AS order_month,
    CAST(QUARTER(o.order_date)   AS INTEGER)                AS order_quarter,
    CAST(DAYOFWEEK(o.order_date) AS INTEGER)                AS day_of_week,
    CASE WHEN DAYOFWEEK(o.order_date) IN (1,7)
         THEN TRUE ELSE FALSE END                           AS is_weekend,

    o.quantity,
    UPPER(TRIM(o.status))                                   AS status,
    TRIM(o.payment_method)                                  AS payment_method,
    TRIM(o.order_source)                                    AS order_source,
    CASE
        WHEN LOWER(TRIM(o.order_source)) IN ('web','mobile','app') THEN 'Online'
        WHEN LOWER(TRIM(o.order_source)) IN ('store','pos')        THEN 'In-Store'
        ELSE 'Partner'
    END                                                     AS channel_category,

    p.price                                                 AS unit_price,
    o.quantity * p.price                                    AS subtotal,
    GREATEST((o.quantity * p.price) - o.total_amount, 0)   AS discount_amount,
    o.total_amount * 0.08                                   AS tax_amount,
    CAST(CASE
        WHEN o.total_amount < 50  THEN 10.0
        WHEN o.total_amount < 100 THEN 5.0
        ELSE 0.0
    END AS DOUBLE)                                          AS shipping_cost,
    o.total_amount,
    CASE
        WHEN o.total_amount > 0
        THEN (o.total_amount - o.quantity * p.price * 0.6)
             / o.total_amount * 100
        ELSE NULL
    END                                                     AS profit_margin_pct,

    TRIM(c.first_name) || ' ' || TRIM(c.last_name)         AS customer_name,
    LOWER(TRIM(c.email))                                    AS customer_email,
    c.customer_segment,
    c.city                                                  AS customer_city,
    c.state                                                 AS customer_state,
    COALESCE(ch.lifetime_orders, 1)                        AS customer_lifetime_orders,

    p.product_name,
    p.category                                              AS product_category,

    CASE
        WHEN o.total_amount > 1000 THEN 'High Value'
        WHEN o.total_amount > 500  THEN 'Medium Value'
        WHEN o.total_amount > 100  THEN 'Low Value'
        ELSE 'Minimal Value'
    END                                                     AS order_value_tier,

    CASE
        WHEN COALESCE(ch.lifetime_orders, 1) >= 10 THEN 'VIP'
        WHEN COALESCE(ch.lifetime_orders, 1) >= 5  THEN 'Loyal'
        WHEN COALESCE(ch.lifetime_orders, 1) >= 2  THEN 'Regular'
        ELSE 'New'
    END                                                     AS customer_value_tier,

    CASE WHEN o.order_date = ch.first_order_date
         THEN TRUE ELSE FALSE END                           AS is_first_order,
    CASE WHEN COALESCE(ch.lifetime_orders, 1) > 1
         THEN TRUE ELSE FALSE END                           AS is_repeat_customer,

    CASE
        WHEN o.total_amount  <= 0    THEN 'INVALID_AMOUNT'
        WHEN o.quantity      <= 0    THEN 'INVALID_QUANTITY'
        WHEN p.product_id   IS NULL  THEN 'MISSING_PRODUCT'
        WHEN c.customer_id  IS NULL  THEN 'MISSING_CUSTOMER'
        ELSE 'VALID'
    END                                                     AS data_quality_flag,

    CURRENT_TIMESTAMP                                       AS processed_at

FROM nessie.bronze.orders      o
LEFT JOIN nessie.bronze.customers  c  ON o.customer_id = c.customer_id
LEFT JOIN nessie.bronze.products   p  ON o.product_id  = p.product_id
LEFT JOIN customer_history         ch ON o.order_id    = ch.order_id
                                      AND o.customer_id = ch.customer_id
WHERE o.status     != 'CANCELLED'
  AND o.total_amount > 0
  AND o.order_id   IS NOT NULL;


-- ---------------------------------------------------------
-- 3B. customer_profiles
-- ---------------------------------------------------------
INSERT INTO nessie.silver.customer_profiles
SELECT
    c.customer_id,
    TRIM(c.first_name) || ' ' || TRIM(c.last_name)     AS customer_name,
    LOWER(TRIM(c.email))                                AS customer_email,
    c.customer_segment,
    c.city                                              AS customer_city,
    c.state                                             AS customer_state,
    c.registration_date,
    MIN(o.order_date)                                   AS first_order_date,
    MAX(o.order_date)                                   AS last_order_date,
    CAST(COUNT(DISTINCT o.order_id) AS INTEGER)         AS total_orders,
    SUM(o.total_amount)                                 AS lifetime_value,
    AVG(o.total_amount)                                 AS avg_order_value,
    CASE
        WHEN SUM(o.total_amount) > 5000 THEN 'VIP'
        WHEN SUM(o.total_amount) > 2000 THEN 'High Value'
        WHEN SUM(o.total_amount) > 500  THEN 'Medium Value'
        ELSE 'Low Value'
    END                                                 AS customer_tier,
    CAST(CASE
        WHEN DATEDIFF(MAX(o.order_date), CURRENT_DATE) > 180 THEN 0.8
        WHEN DATEDIFF(MAX(o.order_date), CURRENT_DATE) > 90  THEN 0.5
        WHEN DATEDIFF(MAX(o.order_date), CURRENT_DATE) > 30  THEN 0.2
        ELSE 0.1
    END AS DOUBLE)                                      AS churn_risk_score,
    TRUE                                                AS is_active,
    CURRENT_TIMESTAMP                                   AS updated_at
FROM nessie.bronze.customers c
LEFT JOIN nessie.bronze.orders o
       ON c.customer_id  = o.customer_id
      AND o.status       != 'CANCELLED'
      AND o.total_amount  > 0
WHERE c.customer_id IS NOT NULL
  AND c.email       IS NOT NULL
GROUP BY
    c.customer_id, c.first_name, c.last_name,
    c.email, c.customer_segment,
    c.city, c.state, c.registration_date;

SELECT * from nessie.silver.customer_profiles;
SELECT * from nessie.silver.orders_enriched_V2;
-- ============================================================
-- SECTION 4 : MERGE (CDC / Upsert incrémental)
-- ============================================================

-- ---------------------------------------------------------
-- 4A. MERGE orders_enriched_V2
-- Traite uniquement les lignes bronze arrivées après le
-- dernier processed_at en silver
-- ---------------------------------------------------------
MERGE INTO nessie.silver.orders_enriched_V2 AS target
USING (

    WITH last_ts AS (
        SELECT COALESCE(MAX(processed_at), TIMESTAMP '1970-01-01 00:00:00') AS max_ts
        FROM nessie.silver.orders_enriched_V2
    )

    SELECT
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,

        EXTRACT(YEAR    FROM o.order_date) AS order_year,
        EXTRACT(MONTH   FROM o.order_date) AS order_month,
        EXTRACT(QUARTER FROM o.order_date) AS order_quarter,
        EXTRACT(DOW     FROM o.order_date) AS day_of_week,

        CASE WHEN EXTRACT(DOW FROM o.order_date) IN (0,6)
             THEN TRUE ELSE FALSE END AS is_weekend,

        o.quantity,
        UPPER(TRIM(o.status)) AS status,
        TRIM(o.payment_method) AS payment_method,
        TRIM(o.order_source) AS order_source,

        CASE
            WHEN LOWER(TRIM(o.order_source)) IN ('web','mobile','app') THEN 'Online'
            WHEN LOWER(TRIM(o.order_source)) IN ('store','pos') THEN 'In-Store'
            ELSE 'Partner'
        END AS channel_category,

        p.price AS unit_price,
        o.quantity * p.price AS subtotal,
        GREATEST((o.quantity * p.price) - o.total_amount, 0) AS discount_amount,
        o.total_amount * 0.08 AS tax_amount,

        CASE
            WHEN o.total_amount < 50 THEN 10.0
            WHEN o.total_amount < 100 THEN 5.0
            ELSE 0.0
        END AS shipping_cost,

        o.total_amount,

        CASE
            WHEN o.total_amount > 0
            THEN (o.total_amount - o.quantity * p.price * 0.6)
                 / o.total_amount * 100
            ELSE NULL
        END AS profit_margin_pct,

        TRIM(c.first_name) || ' ' || TRIM(c.last_name) AS customer_name,
        LOWER(TRIM(c.email)) AS customer_email,
        c.customer_segment,
        c.city AS customer_city,
        c.state AS customer_state,
        1 AS customer_lifetime_orders,

        p.product_name,
        p.category AS product_category,

        CASE
            WHEN o.total_amount > 1000 THEN 'High Value'
            WHEN o.total_amount > 500 THEN 'Medium Value'
            WHEN o.total_amount > 100 THEN 'Low Value'
            ELSE 'Minimal Value'
        END AS order_value_tier,

        'New' AS customer_value_tier,
        FALSE AS is_first_order,
        FALSE AS is_repeat_customer,

        CASE
            WHEN o.total_amount <= 0 THEN 'INVALID_AMOUNT'
            WHEN o.quantity <= 0 THEN 'INVALID_QUANTITY'
            WHEN p.product_id IS NULL THEN 'MISSING_PRODUCT'
            WHEN c.customer_id IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END AS data_quality_flag,

        CURRENT_TIMESTAMP AS processed_at

    FROM nessie.bronze.orders o
    LEFT JOIN nessie.bronze.customers c ON o.customer_id = c.customer_id
    LEFT JOIN nessie.bronze.products p ON o.product_id = p.product_id
    CROSS JOIN last_ts

    WHERE o.ingested_at > last_ts.max_ts
      AND o.status <> 'CANCELLED'
      AND o.total_amount > 0
      AND o.order_id IS NOT NULL

) AS source

ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    status           = source.status,
    total_amount     = source.total_amount,
    discount_amount  = source.discount_amount,
    channel_category = source.channel_category,
    processed_at     = source.processed_at

WHEN NOT MATCHED THEN INSERT *;

-- ---------------------------------------------------------
-- 4B. MERGE customer_profiles (SCD Type 1)
-- ---------------------------------------------------------
MERGE INTO nessie.silver.customer_profiles AS target
USING (
    SELECT
        c.customer_id,
        TRIM(c.first_name) || ' ' || TRIM(c.last_name) AS customer_name,
        LOWER(TRIM(c.email))                            AS customer_email,
        c.customer_segment,
        c.city                                          AS customer_city,
        c.state                                         AS customer_state,
        c.registration_date,
        TRUE                                            AS is_active,
        CURRENT_TIMESTAMP                               AS updated_at
    FROM nessie.bronze.customers c
    WHERE c.ingested_at > (
        SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01 00:00:00')
        FROM nessie.silver.customer_profiles
    )
    AND c.customer_id IS NOT NULL
) AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    customer_name    = source.customer_name,
    customer_email   = source.customer_email,
    customer_segment = source.customer_segment,
    customer_city    = source.customer_city,
    customer_state   = source.customer_state,
    is_active        = source.is_active,
    updated_at       = source.updated_at

WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, customer_email,
    customer_segment, customer_city, customer_state,
    registration_date, is_active, updated_at
) VALUES (
    source.customer_id, source.customer_name, source.customer_email,
    source.customer_segment, source.customer_city, source.customer_state,
    source.registration_date, source.is_active, source.updated_at
);


-- ============================================================
-- SECTION 5 : SCHEMA EVOLUTION Iceberg
-- ============================================================

-- Ajouter des colonnes (les fichiers Parquet existants ne sont PAS réécrits,
-- les anciennes lignes retournent NULL pour ces colonnes)
ALTER TABLE nessie.silver.orders_enriched_V2
ADD COLUMNS (delivery_date DATE);

ALTER TABLE nessie.silver.orders_enriched_V2
ADD COLUMNS (nps_score INTEGER);


-- Upcast sûr (INTEGER → BIGINT)
ALTER TABLE nessie.silver.orders_enriched_V2
ADD COLUMNS (
    customer_lifetime_orders_big BIGINT,  -- upcast
    profit_margin DOUBLE                -- rename de profit_margin_pct
);

-- Supprimer une colonne expérimentale
-- (soft-delete : données toujours accessibles via Time Travel)
ALTER TABLE nessie.silver.orders_enriched_V2
DROP COLUMN nps_score;

-- Vérifier le schéma résultant
DESCRIBE TABLE nessie.silver.orders_enriched_V2;


-- ============================================================
-- SECTION 6 : TIME TRAVEL Iceberg
-- ============================================================

-- Lister tous les snapshots
SELECT * FROM TABLE(table_snapshot('nessie.silver.orders_enriched_V2'))
ORDER BY committed_at DESC;

-- Historique des opérations (INSERT, MERGE, ALTER, OPTIMIZE...)
SELECT * FROM TABLE(table_history('nessie.silver.orders_enriched_V2'))
ORDER BY made_current_at DESC;

-- Requêter avant un MERGE (timestamp)
SELECT COUNT(*) AS rows_before_merge
FROM nessie.silver.orders_enriched_V2
AT TIMESTAMP '2026-02-24 16:27:08.255';

-- Requêter à un snapshot précis (ID issu de table_snapshot)
SELECT COUNT(*) AS rows_at_snapshot
FROM nessie.silver.orders_enriched_V2
AT SNAPSHOT '6487833200514876382';

-- Comparer deux états (détecter une régression)
SELECT 'avant_merge' AS version,
       COUNT(*)       AS total_orders,
       SUM(total_amount) AS total_revenue
FROM nessie.silver.orders_enriched_V2
AT TIMESTAMP '2026-02-24 16:27:08.255'
UNION ALL
SELECT 'apres_merge',
       COUNT(*),
       SUM(total_amount)
FROM nessie.silver.orders_enriched_V2
AT TIMESTAMP '2026-02-24 16:23:12.206';

-- Rollback vers un snapshot antérieur si nécessaire
ROLLBACK TABLE nessie.silver.orders_enriched_V2
TO SNAPSHOT '6487833200514876382';


-- ============================================================
-- SECTION 7 : Merge silver_dev → main + Tag
-- ============================================================

MERGE BRANCH silver_dev INTO main IN nessie;

CREATE TAG v1_0_silver AT BRANCH main IN nessie;

DROP BRANCH silver_dev IN nessie;

SHOW TAGS     IN nessie;
SHOW BRANCHES IN nessie;


-- ============================================================
-- SECTION 8 : GOLD LAYER
-- ============================================================

USE BRANCH gold_experiment IN nessie;

-- ---------------------------------------------------------
-- Gold: daily_sales_summary_V2
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.gold.daily_sales_summary_V2 (
    sale_date               DATE,
    sale_year               INTEGER,
    sale_month              INTEGER,
    total_orders            INTEGER,
    total_items_sold        INTEGER,
    unique_customers        INTEGER,
    new_customers           INTEGER,
    repeat_customers        INTEGER,
    total_revenue           DOUBLE,
    avg_order_value         DOUBLE,
    max_order_value         DOUBLE,
    completion_rate         DOUBLE,
    avg_profit_margin       DOUBLE,
    total_discounts         DOUBLE,
    weekend_order_pct       DOUBLE,
    -- Canal (issu de order_source)
    online_revenue          DOUBLE,
    instore_revenue         DOUBLE,
    partner_revenue         DOUBLE,
    -- Paiement
    credit_card_orders      INTEGER,
    paypal_orders           INTEGER,
    other_payment_orders    INTEGER,
    -- Segment client
    premium_revenue         DOUBLE,
    standard_revenue        DOUBLE,
    basic_revenue           DOUBLE,
    calculated_at           TIMESTAMP
)
PARTITION BY (sale_year, sale_month)
TBLPROPERTIES ('write.format.default' = 'parquet');


INSERT INTO nessie.gold.daily_sales_summary_V2
SELECT
    order_date AS sale_date,
    CAST(EXTRACT(YEAR FROM order_date) AS INT)  AS sale_year,
    CAST(EXTRACT(MONTH FROM order_date) AS INT) AS sale_month,
    CAST(COUNT(DISTINCT order_id) AS INT)       AS total_orders,
    CAST(SUM(quantity) AS INT)                  AS total_items_sold,
    CAST(COUNT(DISTINCT customer_id) AS INT)    AS unique_customers,
    CAST(SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END) AS INT) AS new_customers,
    CAST(SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) AS INT) AS repeat_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    MAX(total_amount) AS max_order_value,
    CAST(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS completion_rate,
    AVG(profit_margin) AS avg_profit_margin,
    SUM(discount_amount) AS total_discounts,
    CAST(SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DOUBLE) AS weekend_order_pct,
    SUM(CASE WHEN channel_category = 'Online'   THEN total_amount ELSE 0 END) AS online_revenue,
    SUM(CASE WHEN channel_category = 'In-Store' THEN total_amount ELSE 0 END) AS instore_revenue,
    SUM(CASE WHEN channel_category = 'Partner'  THEN total_amount ELSE 0 END) AS partner_revenue,
    CAST(SUM(CASE WHEN payment_method = 'Credit Card' THEN 1 ELSE 0 END) AS INT) AS credit_card_orders,
    CAST(SUM(CASE WHEN payment_method = 'PayPal' THEN 1 ELSE 0 END) AS INT) AS paypal_orders,
    CAST(SUM(CASE WHEN payment_method NOT IN ('Credit Card','PayPal') THEN 1 ELSE 0 END) AS INT) AS other_payment_orders,
    SUM(CASE WHEN customer_segment = 'Premium'  THEN total_amount ELSE 0 END) AS premium_revenue,
    SUM(CASE WHEN customer_segment = 'Standard' THEN total_amount ELSE 0 END) AS standard_revenue,
    SUM(CASE WHEN customer_segment = 'Basic'    THEN total_amount ELSE 0 END) AS basic_revenue,
    CURRENT_TIMESTAMP AS calculated_at
FROM nessie.silver.orders_enriched_V2
WHERE data_quality_flag = 'VALID'
GROUP BY order_date;
SELECT * from nessie.gold.daily_sales_summary_V2;
-- ---------------------------------------------------------
-- Gold: customer_360 (RFM + churn)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.gold.customer_360 (
    customer_id                 VARCHAR,
    customer_name               VARCHAR,
    customer_email              VARCHAR,
    customer_segment            VARCHAR,
    customer_tier               VARCHAR,
    customer_city               VARCHAR,
    customer_state              VARCHAR,
    registration_date           DATE,
    first_order_date            DATE,
    last_order_date             DATE,
    days_since_last_order       INTEGER,
    total_orders                INTEGER,
    lifetime_value              DOUBLE,
    avg_order_value             DOUBLE,
    favorite_category           VARCHAR,
    favorite_payment_method     VARCHAR,
    favorite_channel            VARCHAR,
    recency_score               INTEGER,
    frequency_score             INTEGER,
    monetary_score              INTEGER,
    rfm_score                   DOUBLE,
    churn_risk_score            DOUBLE,
    lifetime_value_rank         INTEGER,
    segment_rank                INTEGER,
    last_updated                TIMESTAMP
)
PARTITION BY (customer_segment)
TBLPROPERTIES ('write.format.default' = 'parquet');


INSERT INTO nessie.gold.customer_360
WITH agg AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        customer_segment,
        customer_city,
        customer_state,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        DATEDIFF(MAX(order_date), CURRENT_DATE) AS days_since_last_order
    FROM nessie.silver.orders_enriched_V2
    WHERE data_quality_flag = 'VALID'
    GROUP BY customer_id, customer_name, customer_email, customer_segment, customer_city, customer_state
),
favorite_category AS (
    SELECT customer_id, product_category AS favorite_category
    FROM (
        SELECT
            customer_id,
            product_category,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
        FROM nessie.silver.orders_enriched_V2
        WHERE data_quality_flag = 'VALID'
        GROUP BY customer_id, product_category
    ) t
    WHERE rn = 1
),
favorite_payment AS (
    SELECT customer_id, payment_method AS favorite_payment_method
    FROM (
        SELECT
            customer_id,
            payment_method,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
        FROM nessie.silver.orders_enriched_V2
        WHERE data_quality_flag = 'VALID'
        GROUP BY customer_id, payment_method
    ) t
    WHERE rn = 1
),
favorite_channel AS (
    SELECT customer_id, order_source AS favorite_channel
    FROM (
        SELECT
            customer_id,
            order_source,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
        FROM nessie.silver.orders_enriched_V2
        WHERE data_quality_flag = 'VALID'
        GROUP BY customer_id, order_source
    ) t
    WHERE rn = 1
),
scored AS (
    SELECT
        a.*,
        fcat.favorite_category,
        fpay.favorite_payment_method,
        fchan.favorite_channel,
        CAST(NTILE(5) OVER (ORDER BY days_since_last_order ASC) AS INT) AS recency_score,
        CAST(NTILE(5) OVER (ORDER BY total_orders DESC) AS INT) AS frequency_score,
        CAST(NTILE(5) OVER (ORDER BY lifetime_value DESC) AS INT) AS monetary_score,
        CAST(RANK() OVER (ORDER BY lifetime_value DESC) AS INT) AS lifetime_value_rank,
        CAST(RANK() OVER (PARTITION BY customer_segment ORDER BY lifetime_value DESC) AS INT) AS segment_rank,
        CASE
            WHEN lifetime_value > 5000 THEN 'VIP'
            WHEN lifetime_value > 2000 THEN 'High Value'
            WHEN lifetime_value > 500  THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_tier,
        CAST(
            CASE
                WHEN DATEDIFF(last_order_date, CURRENT_DATE) > 180 THEN 0.8
                WHEN DATEDIFF(last_order_date, CURRENT_DATE) > 90  THEN 0.5
                WHEN DATEDIFF(last_order_date, CURRENT_DATE) > 30  THEN 0.2
                ELSE 0.1
            END AS DOUBLE
        ) AS churn_risk_score
    FROM agg a
    LEFT JOIN favorite_category fcat ON a.customer_id = fcat.customer_id
    LEFT JOIN favorite_payment   fpay ON a.customer_id = fpay.customer_id
    LEFT JOIN favorite_channel   fchan ON a.customer_id = fchan.customer_id
),
reg AS (
    SELECT customer_id, registration_date
    FROM nessie.bronze.customers
)
SELECT
    s.customer_id,
    s.customer_name,
    s.customer_email,
    s.customer_segment,
    s.customer_tier,
    s.customer_city,
    s.customer_state,
    r.registration_date,
    s.first_order_date,
    s.last_order_date,
    s.days_since_last_order,
    CAST(s.total_orders AS INT) AS total_orders,
    s.lifetime_value,
    s.avg_order_value,
    s.favorite_category,
    s.favorite_payment_method,
    s.favorite_channel,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    CAST((s.recency_score + s.frequency_score + s.monetary_score)/3.0 AS DOUBLE) AS rfm_score,
    s.churn_risk_score,
    s.lifetime_value_rank,
    s.segment_rank,
    CURRENT_TIMESTAMP AS last_updated
FROM scored s
LEFT JOIN reg r ON s.customer_id = r.customer_id;
select * from nessie.gold.customer_360;

-- ---------------------------------------------------------
-- Gold: product_performance_V2 (BCG quadrant)
-- Note: pas de is_available dans bronze.products,
--       on filtre sur stock_quantity > 0
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.gold.product_performance_V2 (
    product_id              VARCHAR,
    product_name            VARCHAR,
    product_category        VARCHAR,
    total_orders            INTEGER,
    units_sold              INTEGER,
    total_revenue           DOUBLE,
    avg_selling_price       DOUBLE,
    unique_customers        INTEGER,
    revenue_last_30d        DOUBLE,
    revenue_prev_30d        DOUBLE,
    revenue_growth_pct      DOUBLE,
    performance_quadrant    VARCHAR,
    revenue_rank            INTEGER,
    category_rank           INTEGER,
    current_stock           INTEGER,
    days_of_stock_left      INTEGER,
    reorder_recommended     BOOLEAN,
    last_calculated         TIMESTAMP
)
PARTITION BY (product_category)
TBLPROPERTIES ('write.format.default' = 'parquet');


INSERT INTO nessie.gold.product_performance_V2
WITH metrics AS (
    SELECT
        product_id,
        product_name,
        product_category,
        CAST(COUNT(DISTINCT order_id) AS INT)    AS total_orders,
        CAST(SUM(quantity) AS INT)               AS units_sold,
        SUM(total_amount)                        AS total_revenue,
        AVG(unit_price)                           AS avg_selling_price,
        CAST(COUNT(DISTINCT customer_id) AS INT) AS unique_customers
    FROM nessie.silver.orders_enriched_V2
    WHERE data_quality_flag = 'VALID'
    GROUP BY product_id, product_name, product_category
),
trends AS (
    SELECT
        product_id,
        SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE, 30)
                 THEN total_amount ELSE 0 END)  AS revenue_last_30d,
        SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE, 60)
                              AND DATE_SUB(CURRENT_DATE, 31)
                 THEN total_amount ELSE 0 END)  AS revenue_prev_30d,
        SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE, 30)
                 THEN quantity ELSE 0 END)      AS units_last_30d
    FROM nessie.silver.orders_enriched_V2
    WHERE data_quality_flag = 'VALID'
    GROUP BY product_id
),
stock AS (
    SELECT product_id, stock_quantity AS current_stock
    FROM nessie.bronze.products
    WHERE stock_quantity IS NOT NULL
)
SELECT
    m.product_id,
    m.product_name,
    m.product_category,
    m.total_orders,
    m.units_sold,
    m.total_revenue,
    m.avg_selling_price,
    m.unique_customers,
    t.revenue_last_30d,
    t.revenue_prev_30d,
    CASE
        WHEN COALESCE(t.revenue_prev_30d, 0) = 0 THEN NULL
        ELSE (t.revenue_last_30d - t.revenue_prev_30d) / t.revenue_prev_30d * 100
    END AS revenue_growth_pct,
    CASE
        WHEN t.revenue_last_30d > AVG(t.revenue_last_30d) OVER ()
         AND t.revenue_last_30d > COALESCE(t.revenue_prev_30d,0) * 1.1 THEN 'Star'
        WHEN t.revenue_last_30d > AVG(t.revenue_last_30d) OVER () THEN 'Cash Cow'
        WHEN t.revenue_last_30d > COALESCE(t.revenue_prev_30d,0) * 1.1 THEN 'Question Mark'
        ELSE 'Dog'
    END AS performance_quadrant,
    CAST(RANK() OVER (ORDER BY m.total_revenue DESC) AS INT) AS revenue_rank,
    CAST(RANK() OVER (PARTITION BY m.product_category ORDER BY m.total_revenue DESC) AS INT) AS category_rank,
    s.current_stock,
    CASE
        WHEN COALESCE(t.units_last_30d, 0) = 0 THEN NULL
        ELSE CAST(s.current_stock / (t.units_last_30d / 30.0) AS INT)
    END AS days_of_stock_left,
    CASE
        WHEN COALESCE(t.units_last_30d, 0) > 0
         AND s.current_stock < t.units_last_30d / 30.0 * 7
        THEN TRUE ELSE FALSE
    END AS reorder_recommended,
    CURRENT_TIMESTAMP AS last_calculated
FROM metrics m
LEFT JOIN trends t ON m.product_id = t.product_id
LEFT JOIN stock s  ON m.product_id = s.product_id;


SELECT * from nessie.gold.product_performance_V2;
-- ============================================================
-- SECTION 9 : Merge gold_experiment → main + Tag
-- ============================================================

MERGE BRANCH gold_experiment INTO main IN nessie;

CREATE TAG v1_0_gold AT BRANCH main IN nessie;

DROP BRANCH gold_experiment IN nessie;


-- ============================================================
-- SECTION 10 : COMPACTION & MAINTENANCE
-- ============================================================

-- Compacter les petits fichiers Parquet générés par Spark
OPTIMIZE TABLE nessie.silver.orders_enriched_V2
    REWRITE DATA USING BIN_PACK;

-- Re-trier pour optimiser le partition pruning analytique
CREATE TABLE nessie.gold.orders_enriched_sorted AS
SELECT *
FROM nessie.silver.orders_enriched_V2
ORDER BY order_year, order_month, customer_id;





-- ============================================================
-- SECTION 11 : MONITORING & VALIDATION
-- ============================================================

-- Row counts sur toutes les couches
SELECT 'bronze.orders' AS layer, COUNT(*) AS row_count FROM nessie.bronze.orders
UNION ALL SELECT 'bronze.customers', COUNT(*) AS row_count FROM nessie.bronze.customers
UNION ALL SELECT 'bronze.products', COUNT(*) AS row_count FROM nessie.bronze.products
UNION ALL SELECT 'silver.orders_enriched_V2', COUNT(*) AS row_count FROM nessie.silver.orders_enriched_V2
UNION ALL SELECT 'silver.customer_profiles', COUNT(*) AS row_count FROM nessie.silver.customer_profiles
UNION ALL SELECT 'gold.daily_sales_summary_V2', COUNT(*) AS row_count FROM nessie.gold.daily_sales_summary_V2
UNION ALL SELECT 'gold.customer_360', COUNT(*) AS row_count FROM nessie.gold.customer_360
UNION ALL SELECT 'gold.product_performance_V2', COUNT(*) AS row_count FROM nessie.gold.product_performance_V2;

-- Qualité des données silver
SELECT
    data_quality_flag,
    COUNT(*)                                                AS records,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)    AS pct
FROM nessie.silver.orders_enriched_V2
GROUP BY data_quality_flag
ORDER BY records DESC;

-- Snapshots silver (Time Travel audit)
SELECT * FROM TABLE(table_snapshot('nessie.silver.orders_enriched_V2'))
ORDER BY committed_at DESC LIMIT 20;

-- Taille des fichiers Parquet silver
SELECT
    file_path,
    ROUND(file_size_in_bytes / 1024.0 / 1024.0, 2) AS size_mb,
    record_count
FROM TABLE(table_files('nessie.silver.orders_enriched_V2'))
ORDER BY file_size_in_bytes DESC LIMIT 20;

-- Branches et tags actifs
SHOW BRANCHES IN nessie;
SHOW TAGS     IN nessie;

-- KPIs rapides
SELECT 'Revenue MTD' AS metric,
       CAST(ROUND(SUM(total_amount), 2) AS VARCHAR)
FROM nessie.silver.orders_enriched_V2
WHERE order_year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND order_month = EXTRACT(MONTH FROM CURRENT_DATE)
  AND data_quality_flag = 'VALID'

UNION ALL

SELECT 'Clients à risque churn (>0.5)',
       CAST(COUNT(*) AS VARCHAR)
FROM nessie.gold.customer_360
WHERE churn_risk_score > 0.5

UNION ALL

SELECT 'Produits à réapprovisionner',
       CAST(COUNT(*) AS VARCHAR)
FROM nessie.gold.product_performance_V2
WHERE reorder_recommended = TRUE;