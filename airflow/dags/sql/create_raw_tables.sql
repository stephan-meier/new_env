DROP TABLE IF EXISTS raw.order_details CASCADE;
DROP TABLE IF EXISTS raw.orders CASCADE;
DROP TABLE IF EXISTS raw.customers CASCADE;
DROP TABLE IF EXISTS raw.employees CASCADE;
DROP TABLE IF EXISTS raw.products CASCADE;

CREATE TABLE raw.customers (
    id BIGINT NOT NULL PRIMARY KEY,
    last_name VARCHAR(50),
    first_name VARCHAR(50),
    email VARCHAR(50),
    company VARCHAR(50),
    phone VARCHAR(25),
    address1 VARCHAR(150),
    address2 VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(15),
    country VARCHAR(50),
    change_date TIMESTAMP
);

CREATE TABLE raw.employees (
    id BIGINT NOT NULL PRIMARY KEY,
    last_name VARCHAR(50),
    first_name VARCHAR(50),
    email VARCHAR(50),
    avatar VARCHAR(250),
    job_title VARCHAR(50),
    department VARCHAR(50),
    manager_id BIGINT,
    phone VARCHAR(25),
    address1 VARCHAR(150),
    address2 VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(15),
    country VARCHAR(50),
    change_date TIMESTAMP
);

CREATE TABLE raw.orders (
    id BIGINT NOT NULL PRIMARY KEY,
    employee_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    shipped_date TIMESTAMP,
    ship_name VARCHAR(50),
    ship_address1 VARCHAR(150),
    ship_address2 VARCHAR(150),
    ship_city VARCHAR(50),
    ship_state VARCHAR(50),
    ship_postal_code VARCHAR(50),
    ship_country VARCHAR(50),
    shipping_fee NUMERIC(19,4) DEFAULT 0,
    payment_type VARCHAR(50),
    paid_date TIMESTAMP,
    order_status VARCHAR(25),
    change_date TIMESTAMP
);

CREATE TABLE raw.order_details (
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    unit_price NUMERIC(19,4) DEFAULT 0,
    discount DOUBLE PRECISION NOT NULL DEFAULT 0,
    order_detail_status VARCHAR(25),
    date_allocated TIMESTAMP,
    change_date TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE raw.products (
    id BIGINT NOT NULL PRIMARY KEY,
    product_code VARCHAR(25),
    product_name VARCHAR(50),
    description VARCHAR(250),
    standard_cost NUMERIC(19,4) DEFAULT 0,
    list_price NUMERIC(19,4) NOT NULL DEFAULT 0,
    target_level BIGINT,
    reorder_level BIGINT,
    minimum_reorder_quantity BIGINT,
    quantity_per_unit VARCHAR(50),
    discontinued BIGINT NOT NULL DEFAULT 0,
    category VARCHAR(50),
    change_date TIMESTAMP
);
