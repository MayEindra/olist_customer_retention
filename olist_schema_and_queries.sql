
-- Orders (links to customers, items, payments, reviews)
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

-- Order line items (links to orders, products, sellers)
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price REAL,
    freight_value REAL
);

-- Order reviews (links to orders via order_id)
CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);

-- Order payments (links to orders via order_id)
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL
);

-- Customers (links to orders via customer_id, to geolocation via zip)
CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

-- Products (links to order_items via product_id)
CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL
);

-- Sellers (links to order_items via seller_id, to geolocation via zip)
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id TEXT,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

-- Geolocation (links from customers and sellers via zip_code_prefix)
CREATE TABLE IF NOT EXISTS olist_geolocation_dataset (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
);

-- Category names in English (optional; join to products on product_category_name)
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name TEXT,
    product_category_name_english TEXT
);

-- =============================================================================
-- JOIN KEY REFERENCE (for Python/pandas replication)
-- =============================================================================
-- order_id          -> orders, order_items, order_reviews, order_payments
-- customer_id       -> orders -> customers
-- product_id        -> order_items -> products
-- seller_id         -> order_items -> sellers
-- zip_code_prefix   -> customers (customer_zip_code_prefix), sellers (seller_zip_code_prefix)
--                    -> geolocation (geolocation_zip_code_prefix)
-- product_category_name -> products -> product_category_name_translation

-- =============================================================================
-- ANALYTICAL QUERIES (run after loading CSVs into the tables above)
-- =============================================================================


-- Orders with review score (one row per order)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    r.review_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date
FROM olist_orders_dataset o
LEFT JOIN olist_order_reviews_dataset r ON o.order_id = r.order_id;

-- Full analytical view: orders + items + products + sellers + customers
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS total_item_value,
    p.product_category_name,
    p.product_weight_g,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
FROM olist_orders_dataset o
JOIN olist_order_items_dataset i ON o.order_id = i.order_id
LEFT JOIN olist_products_dataset p ON i.product_id = p.product_id
LEFT JOIN olist_sellers_dataset s ON i.seller_id = s.seller_id
LEFT JOIN olist_customers_dataset c ON o.customer_id = c.customer_id;

-- Create customer satisfaction analysis dataset
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Target variable
    r.review_score,
    r.review_creation_date,
    
    -- Delivery performance metrics (computed in SQL)
    JULIANDAY(o.order_delivered_customer_date) - JULIANDAY(o.order_estimated_delivery_date) AS delivery_delay_days,
    JULIANDAY(o.order_delivered_customer_date) - JULIANDAY(o.order_purchase_timestamp) AS actual_delivery_days,
    
    -- Order-level aggregations
    COUNT(DISTINCT i.order_item_id) AS items_in_order,
    SUM(i.price) AS total_price,
    SUM(i.freight_value) AS total_freight,
    SUM(i.price + i.freight_value) AS total_order_value,
    AVG(p.product_weight_g) AS avg_product_weight,
    
    -- Product characteristics
    p.product_category_name,
    
    -- Geography
    c.customer_state,
    s.seller_state,
    c.customer_city,
    s.seller_city,
    CASE WHEN c.customer_state = s.seller_state THEN 1 ELSE 0 END AS same_state_delivery
    
FROM olist_orders_dataset o
JOIN olist_order_reviews_dataset r ON o.order_id = r.order_id
JOIN olist_order_items_dataset i ON o.order_id = i.order_id
LEFT JOIN olist_products_dataset p ON i.product_id = p.product_id
LEFT JOIN olist_customers_dataset c ON o.customer_id = c.customer_id
LEFT JOIN olist_sellers_dataset s ON i.seller_id = s.seller_id

WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL

GROUP BY o.order_id, r.review_score, c.customer_state, s.seller_state, 
         c.customer_city, s.seller_city, p.product_category_name;
