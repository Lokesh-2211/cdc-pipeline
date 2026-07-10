CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO orders (customer_name, product, amount, status) VALUES
    ('Alice Smith', 'Widget A', 29.99, 'pending'),
    ('Bob Jones', 'Widget B', 49.99, 'shipped'),
    ('Carla Diaz', 'Widget C', 19.99, 'pending');

-- Debezium needs REPLICA IDENTITY FULL to capture full before/after row images,
-- otherwise deletes and updates only show primary key values.
ALTER TABLE orders REPLICA IDENTITY FULL;
