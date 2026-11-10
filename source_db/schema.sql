-- Source OLTP schema for the simulated Order Management System (OMS).
-- Idempotent: safe to run on every seed.
create schema if not exists shop;

create table if not exists shop.carriers (
    carrier_id     serial primary key,
    carrier_name   varchar(100) not null unique,
    service_level  varchar(20)  not null,
    created_at     timestamp    not null,
    updated_at     timestamp    not null
);

create table if not exists shop.sellers (
    seller_id    serial primary key,
    seller_name  varchar(120) not null,
    city         varchar(60)  not null,
    state        varchar(2)   not null,
    rating       numeric(2, 1) not null,
    created_at   timestamp    not null,
    updated_at   timestamp    not null
);

create table if not exists shop.products (
    product_id    serial primary key,
    product_name  varchar(150)  not null,
    category      varchar(40)   not null,
    unit_price    numeric(10, 2) not null,
    weight_kg     numeric(7, 2)  not null,
    created_at    timestamp     not null,
    updated_at    timestamp     not null
);

create table if not exists shop.customers (
    customer_id    serial primary key,
    customer_name  varchar(100) not null,
    email          varchar(150) not null,
    city           varchar(60)  not null,
    state          varchar(2)   not null,
    segment        varchar(30)  not null,
    created_at     timestamp    not null,
    updated_at     timestamp    not null
);

create table if not exists shop.orders (
    order_id                serial primary key,
    customer_id             integer     not null references shop.customers (customer_id),
    order_status            varchar(20) not null
        check (order_status in ('placed', 'shipped', 'delivered', 'cancelled')),
    order_ts                timestamp   not null,
    promised_delivery_date  date        not null,
    created_at              timestamp   not null,
    updated_at              timestamp   not null
);

create table if not exists shop.order_items (
    order_item_id  serial primary key,
    order_id       integer       not null references shop.orders (order_id),
    product_id     integer       not null references shop.products (product_id),
    seller_id      integer       not null references shop.sellers (seller_id),
    quantity       integer       not null check (quantity > 0),
    unit_price     numeric(10, 2) not null,
    created_at     timestamp     not null,
    updated_at     timestamp     not null
);

-- NOTE: freight_cost / delivered_ts have no check constraints on purpose.
-- The "legacy" source lets bad data through, so the pipeline must catch it.
create table if not exists shop.shipments (
    shipment_id      serial primary key,
    order_id         integer       not null references shop.orders (order_id),
    carrier_id       integer       not null references shop.carriers (carrier_id),
    shipped_ts       timestamp     not null,
    delivered_ts     timestamp,
    freight_cost     numeric(10, 2) not null,
    shipment_status  varchar(20)   not null,
    created_at       timestamp     not null,
    updated_at       timestamp     not null
);

-- Watermark-friendly indexes for incremental extraction
create index if not exists ix_carriers_updated_at    on shop.carriers (updated_at);
create index if not exists ix_sellers_updated_at     on shop.sellers (updated_at);
create index if not exists ix_products_updated_at    on shop.products (updated_at);
create index if not exists ix_customers_updated_at   on shop.customers (updated_at);
create index if not exists ix_orders_updated_at      on shop.orders (updated_at);
create index if not exists ix_order_items_updated_at on shop.order_items (updated_at);
create index if not exists ix_shipments_updated_at   on shop.shipments (updated_at);

-- Simulator-only tables (prefixed with _ and never ingested)
create table if not exists shop._sim_clock (sim_date date not null);
create table if not exists shop._sim_plan (
    shipment_id          integer primary key references shop.shipments (shipment_id),
    planned_delivery_ts  timestamp not null
);
