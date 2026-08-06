-- 1. Sales Table (In-store transactions)
CREATE TABLE IF NOT EXISTS sales (
    sale_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    sale_date DATE NOT NULL,
    payment_method TEXT NOT NULL,
    line_amount NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (sale_id, sale_date)
);

-- 2. Orders Table (E-commerce headers)
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT NOT NULL,
    customer_id TEXT,
    order_ts TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    channel TEXT NOT NULL,
    business_date DATE NOT NULL,
    PRIMARY KEY (order_id, business_date)
);

-- 3. Order Items Table (E-commerce line items)
CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_amount NUMERIC(10, 2) NOT NULL
);

-- 4. Shipments Table (Supplier portal)
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT NOT NULL,
    supplier_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    ship_date DATE NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (shipment_id, ship_date)
);

-- 5. Inventory Table (Warehouse daily snapshots)
CREATE TABLE IF NOT EXISTS inventory (
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL,
    snapshot_date DATE NOT NULL,
    PRIMARY KEY (product_id, warehouse_id, snapshot_date)
);