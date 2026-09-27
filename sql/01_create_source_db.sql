/* =====================================================================
   Source OLTP database for the retail platform (SQL Server 2016-2022,
   Developer edition is fine). Run in SSMS / Azure Data Studio / sqlcmd.

   Mirroring requirements this script satisfies:
     * every table has a PRIMARY KEY
     * only mirroring-supported data types (no sql_variant, xml, etc.)
   ===================================================================== */
IF DB_ID('RetailOLTP') IS NULL
    CREATE DATABASE RetailOLTP;
GO
USE RetailOLTP;
GO

IF OBJECT_ID('dbo.order_items') IS NOT NULL DROP TABLE dbo.order_items;
IF OBJECT_ID('dbo.orders')      IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID('dbo.customers')   IS NOT NULL DROP TABLE dbo.customers;
IF OBJECT_ID('dbo.products')    IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID('dbo.stores')      IS NOT NULL DROP TABLE dbo.stores;
GO

CREATE TABLE dbo.stores (
    store_id      VARCHAR(10)   NOT NULL PRIMARY KEY,
    store_name    NVARCHAR(100) NOT NULL,
    city          NVARCHAR(60)  NOT NULL,
    state         NVARCHAR(60)  NULL,
    country       NVARCHAR(60)  NOT NULL,
    currency      CHAR(3)       NOT NULL,
    store_format  VARCHAR(20)   NOT NULL,
    opened_date   DATE          NOT NULL
);

CREATE TABLE dbo.products (
    product_id     VARCHAR(10)   NOT NULL PRIMARY KEY,
    product_name   NVARCHAR(150) NOT NULL,
    category       NVARCHAR(50)  NOT NULL,
    brand          NVARCHAR(60)  NOT NULL,
    unit_price_inr DECIMAL(12,2) NOT NULL,
    unit_cost_inr  DECIMAL(12,2) NOT NULL,
    is_active      BIT           NOT NULL DEFAULT 1,
    updated_at     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.customers (
    customer_id      INT           NOT NULL PRIMARY KEY,
    first_name       NVARCHAR(60)  NOT NULL,
    last_name        NVARCHAR(60)  NOT NULL,
    email            NVARCHAR(150) NOT NULL,
    phone            VARCHAR(20)   NULL,
    city             NVARCHAR(60)  NOT NULL,
    state            NVARCHAR(60)  NULL,
    country          NVARCHAR(60)  NOT NULL,
    loyalty_tier     VARCHAR(10)   NOT NULL,
    marketing_opt_in BIT           NOT NULL,
    signup_date      DATE          NOT NULL,
    updated_at       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.orders (
    order_id       INT           NOT NULL PRIMARY KEY,
    customer_id    INT           NOT NULL REFERENCES dbo.customers(customer_id),
    order_ts       DATETIME2(0)  NOT NULL,
    channel        VARCHAR(20)   NOT NULL,
    currency       CHAR(3)       NOT NULL,
    order_status   VARCHAR(20)   NOT NULL,
    payment_method VARCHAR(20)   NOT NULL,
    ship_city      NVARCHAR(60)  NULL,
    ship_country   NVARCHAR(60)  NULL,
    updated_at     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.order_items (
    order_id     INT           NOT NULL REFERENCES dbo.orders(order_id),
    line_no      INT           NOT NULL,
    product_id   VARCHAR(10)   NOT NULL REFERENCES dbo.products(product_id),
    quantity     INT           NOT NULL,
    unit_price   DECIMAL(12,2) NOT NULL,   -- in orders.currency
    discount_pct DECIMAL(5,2)  NOT NULL DEFAULT 0,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, line_no)
);
GO

CREATE INDEX ix_orders_customer ON dbo.orders(customer_id, order_ts);
GO
