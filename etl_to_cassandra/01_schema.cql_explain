CREATE KEYSPACE IF NOT EXISTS reviews
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE reviews;

CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id        TEXT,
    star_rating       INT,
    review_id         TEXT,
    customer_id       TEXT,
    product_title     TEXT,
    review_headline   TEXT,
    review_body       TEXT,
    review_date       DATE,
    helpful_votes     INT,
    total_votes       INT,
    verified_purchase INT,
    PRIMARY KEY ((product_id), star_rating, review_id)
) WITH CLUSTERING ORDER BY (star_rating DESC, review_id ASC);

CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id       TEXT,
    review_date       DATE,
    review_id         TEXT,
    product_id        TEXT,
    product_title     TEXT,
    star_rating       INT,
    review_headline   TEXT,
    review_body       TEXT,
    helpful_votes     INT,
    total_votes       INT,
    verified_purchase INT,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

CREATE TABLE IF NOT EXISTS top_products_by_period (
    year_month    TEXT,
    review_count  INT,
    product_id    TEXT,
    product_title TEXT,
    PRIMARY KEY ((year_month), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC, product_id ASC);

CREATE TABLE IF NOT EXISTS top_customers_by_period (
    year_month   TEXT,
    review_count INT,
    customer_id  TEXT,
    PRIMARY KEY ((year_month), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

CREATE TABLE IF NOT EXISTS top_haters_by_period (
    year_month     TEXT,
    low_star_count INT,
    customer_id    TEXT,
    PRIMARY KEY ((year_month), low_star_count, customer_id)
) WITH CLUSTERING ORDER BY (low_star_count DESC, customer_id ASC);

CREATE TABLE IF NOT EXISTS top_backers_by_period (
    year_month      TEXT,
    high_star_count INT,
    customer_id     TEXT,
    PRIMARY KEY ((year_month), high_star_count, customer_id)
) WITH CLUSTERING ORDER BY (high_star_count DESC, customer_id ASC);