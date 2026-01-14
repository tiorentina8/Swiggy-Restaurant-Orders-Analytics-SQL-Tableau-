	SELECT COUNT(*) 
FROM swiggy_data;

-- Data validation and cleaning
-- Null check

SELECT
    SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS null_state,
    SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
    SUM(CASE WHEN restaurant_name IS NULL THEN 1 ELSE 0 END) AS null_restaurant,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN dish_name IS NULL THEN 1 ELSE 0 END) AS null_dish,
    SUM(CASE WHEN price_inr IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS null_rating,
    SUM(CASE WHEN rating_count IS NULL THEN 1 ELSE 0 END) AS null_rating_count
FROM swiggy_data;

SELECT *
FROM swiggy_data
WHERE
    state = '' 
    OR city = '' 
    OR restaurant_name = '' 
    OR location = '' 
    OR category = '' 
    OR dish_name = '';

	SELECT COUNT(*) 
FROM swiggy_data;


SELECT
    state,
    city,
    order_date,
    restaurant_name,
    location,
    category,
    dish_name,
    price_inr,
    rating,
    rating_count,
    COUNT(*) AS cnt
FROM swiggy_data
GROUP BY
    state,
    city,
    order_date,
    restaurant_name,
    location,
    category,
    dish_name,
    price_inr,
    rating,
    rating_count
HAVING COUNT(*) > 1;


DELETE FROM swiggy_data
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY state,
                             city,
                             order_date,
                             restaurant_name,
                             location,
                             category,
                             dish_name,
                             price_inr,
                             rating,
                             rating_count
                ORDER BY ctid
            ) AS rn
        FROM swiggy_data
    ) t
    WHERE rn > 1
);

-- dim_date

CREATE TABLE dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   date,
    year        int,
    month       int,
    month_name  varchar(20),
    quarter     int,
    day         int,
    week        int
);

SELECT * FROM dim_date;

-- dim_location
CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    state       varchar(100),
    city        varchar(100),
    location    varchar(200)
);

-- dim_restaurant
CREATE TABLE dim_restaurant (
    restaurant_id SERIAL PRIMARY KEY,
    restaurant_name varchar(200)
);

-- dim_category
CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    category    varchar(200)
);

-- dim_dish
CREATE TABLE dim_dish (
    dish_id   SERIAL PRIMARY KEY,
    dish_name varchar(200)
);

CREATE TABLE fact_swiggy_orders (
    order_id       SERIAL PRIMARY KEY,
    date_id        int,
    price_inr      numeric(10,2),
    rating         numeric(4,2),
    rating_count   int,
    location_id    int,
    restaurant_id  int,
    category_id    int,
    dish_id        int,
    FOREIGN KEY (date_id)       REFERENCES dim_date(date_id),
    FOREIGN KEY (location_id)   REFERENCES dim_location(location_id),
    FOREIGN KEY (restaurant_id) REFERENCES dim_restaurant(restaurant_id),
    FOREIGN KEY (category_id)   REFERENCES dim_category(category_id),
    FOREIGN KEY (dish_id)       REFERENCES dim_dish(dish_id)
);

INSERT INTO dim_date (
    full_date,
    year,
    month,
    month_name,
    quarter,
    day,
    week
)
SELECT DISTINCT
    order_date,
    EXTRACT(YEAR  FROM order_date)::int,
    EXTRACT(MONTH FROM order_date)::int,
    TO_CHAR(order_date, 'Month'),
    EXTRACT(QUARTER FROM order_date)::int,
    EXTRACT(DAY   FROM order_date)::int,
    EXTRACT(WEEK  FROM order_date)::int
FROM swiggy_data
WHERE order_date IS NOT NULL;

INSERT INTO dim_date (
    full_date,
    year,
    month,
    month_name,
    quarter,
    day,
    week
)
SELECT DISTINCT
    order_date,
    EXTRACT(YEAR  FROM order_date)::int,
    EXTRACT(MONTH FROM order_date)::int,
    TO_CHAR(order_date, 'Month'),
    EXTRACT(QUARTER FROM order_date)::int,
    EXTRACT(DAY   FROM order_date)::int,
    EXTRACT(WEEK  FROM order_date)::int
FROM swiggy_data
WHERE order_date IS NOT NULL;

SELECT * FROM dim_date

-- dim location
-- Option 2: Drop the old table, then recreate (this deletes existing data!)

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    state       varchar(100),
    city        varchar(100),
    location    varchar(200)
);

-- dim_restaurant
INSERT INTO dim_restaurant (restaurant_name)
SELECT DISTINCT
    restaurant_name
FROM swiggy_data;

-- dim_category
INSERT INTO dim_category (category)
SELECT DISTINCT
    category
FROM swiggy_data;

-- dim_dish
INSERT INTO dim_dish (dish_name)
SELECT DISTINCT
    dish_name
FROM swiggy_data;

INSERT INTO fact_swiggy_orders (
    date_id,
    price_inr,
    rating,
    rating_count,
    location_id,
    restaurant_id,
    category_id,
    dish_id
)
SELECT
    dd.date_id,
    s.price_inr,
    s.rating,
    s.rating_count,
    dl.location_id,
    dr.restaurant_id,
    dc.category_id,
    dsh.dish_id
FROM swiggy_data s
JOIN dim_date dd  ON dd.full_date      = s.order_date
JOIN dim_location dl  ON dl.state          = s.state
                       AND dl.city           = s.city
                       AND dl.location       = s.location
JOIN dim_restaurant dr  ON dr.restaurant_name = s.restaurant_name
JOIN dim_category   dc  ON dc.category        = s.category
JOIN dim_dish       dsh ON dsh.dish_name      = s.dish_name;

SELECT * FROM fact_swiggy_orders;

SELECT *
FROM fact_swiggy_orders f
JOIN dim_date       d  ON f.date_id       = d.date_id
JOIN dim_location   l  ON f.location_id   = l.location_id
JOIN dim_restaurant r  ON f.restaurant_id = r.restaurant_id
JOIN dim_category   c  ON f.category_id   = c.category_id
JOIN dim_dish       d1 ON f.dish_id       = d1.dish_id;

-- Total orders
SELECT COUNT(*) AS total_orders
FROM fact_swiggy_orders;

-- Total revenue
SELECT
    ROUND(SUM(price_inr) / 1000000.0, 2) AS total_revenue_inr_million
FROM fact_swiggy_orders;

-- Average dish price
SELECT
    ROUND(AVG(price_inr), 2) AS avg_dish_price_inr
FROM fact_swiggy_orders;

-- Average rating
SELECT
AVG(Rating) AS Avg_rating
FROM fact_swiggy_orders;

-- Monthly order trends

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_date d
    ON f.date_id = d.date_id
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    COUNT(*) DESC;

-- Total revenue

SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(price_INR) AS total_revenue
FROM fact_swiggy_orders f
JOIN dim_date d
    ON f.date_id = d.date_id
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    SUM(price_INR) DESC;

-- Quarterly trend

SELECT
    d.quarter,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_date d
    ON f.date_id = d.date_id
GROUP BY
    d.quarter
ORDER BY
    COUNT(*) DESC;

-- Yearly trend

SELECT
d.year,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_date d
    ON f.date_id = d.date_id
GROUP BY
    d.year
ORDER BY
    COUNT(*) DESC;


-- ORDERS BY DAY OF WEEK (MON - SUN)

SELECT
    TO_CHAR(d.full_date, 'Dy') AS day_name,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_date d
    ON f.date_id = d.date_id
GROUP BY
    TO_CHAR(d.full_date, 'Dy'),
    EXTRACT(DOW FROM d.full_date)
ORDER BY
    EXTRACT(DOW FROM d.full_date);

-- Top 10 cities by order volume
SELECT
    l.city,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_location l
    ON l.location_id = f.location_id
GROUP BY
    l.city
ORDER BY
    COUNT(*) DESC
LIMIT 10;

-- Top 10 cities by order revenue
SELECT
    l.city,
    SUM(f.price_INR) AS total_revenue
FROM fact_swiggy_orders f
JOIN dim_location l
    ON l.location_id = f.location_id
GROUP BY
    l.city
ORDER BY
  SUM(f.price_INR) DESC
LIMIT 10;

-- Revenue contribution by state

SELECT
    l.state,
    SUM(f.price_INR) AS total_revenue
FROM fact_swiggy_orders f
JOIN dim_location l
    ON l.location_id = f.location_id
GROUP BY
    l.state
ORDER BY
  SUM(f.price_INR) DESC
LIMIT 10;


-- Top categories by order volume
SELECT
    c.category,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders f
JOIN dim_category c
    ON f.category_id = c.category_id
GROUP BY
    c.category
ORDER BY
    total_orders DESC;

-- Most Ordered Dishes
SELECT
    d.dish_name,
    COUNT(*) AS order_count
FROM fact_swiggy_orders AS f
JOIN dim_dish AS d
    ON f.dish_id = d.dish_id
GROUP BY
    d.dish_name
ORDER BY
    order_count DESC;


-- Total Orders by Price Range
-- Example fixing the CASE with CONVERT
-- Total Orders by Price Range (PostgreSQL)
SELECT
    CASE
        WHEN CAST(price_inr AS float8) < 100 THEN 'Under 100'
        WHEN CAST(price_inr AS float8) BETWEEN 100 AND 199 THEN '100 - 199'
        WHEN CAST(price_inr AS float8) BETWEEN 200 AND 299 THEN '200 - 299'
        WHEN CAST(price_inr AS float8) BETWEEN 300 AND 499 THEN '300 - 499'
        ELSE '500+'
    END AS price_range,
    COUNT(*) AS total_orders
FROM fact_swiggy_orders
GROUP BY
    CASE
        WHEN CAST(price_inr AS float8) < 100 THEN 'Under 100'
        WHEN CAST(price_inr AS float8) BETWEEN 100 AND 199 THEN '100 - 199'
        WHEN CAST(price_inr AS float8) BETWEEN 200 AND 299 THEN '200 - 299'
        WHEN CAST(price_inr AS float8) BETWEEN 300 AND 499 THEN '300 - 499'
        ELSE '500+'
    END
ORDER BY total_orders DESC;

-- Rating Count Distribution (1–5)
SELECT
    rating,
    COUNT(*) AS rating_count
FROM fact_swiggy_orders
GROUP BY rating
ORDER BY COUNT(*) DESC
LIMIT 10;

SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(price_inr) / 1000000.0, 2) AS total_revenue_inr_million,
    ROUND(AVG(price_inr), 2) AS avg_dish_price_inr
FROM fact_swiggy_orders;