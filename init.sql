-- Initialize PostgreSQL for Arctic Mirror
-- This script sets up the database, tables, and replication configuration

-- Create the users table
CREATE TABLE IF NOT EXISTS public.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO public.users (username, email) VALUES
    ('admin', 'admin@example.com'),
    ('user1', 'user1@example.com'),
    ('user2', 'user2@example.com')
ON CONFLICT (username) DO NOTHING;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for users table
DROP TRIGGER IF EXISTS update_users_updated_at ON public.users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON public.users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Create replication user (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
        CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'secret';
    END IF;
END
$$;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE mydb TO replicator;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO replicator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;

-- Create publication for all tables
DROP PUBLICATION IF EXISTS pub_all;
CREATE PUBLICATION pub_all FOR ALL TABLES;

-- Create additional test tables
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES public.users(id),
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data for orders
INSERT INTO public.orders (user_id, total_amount, status) VALUES
    (1, 99.99, 'completed'),
    (2, 149.99, 'pending'),
    (3, 299.99, 'shipped')
ON CONFLICT (id) DO NOTHING;

-- Insert sample data for products
INSERT INTO public.products (name, description, price) VALUES
    ('Product A', 'Description for Product A', 29.99),
    ('Product B', 'Description for Product B', 49.99),
    ('Product C', 'Description for Product C', 79.99)
ON CONFLICT (id) DO NOTHING;

-- Grant permissions on new tables
GRANT ALL PRIVILEGES ON public.orders TO replicator;
GRANT ALL PRIVILEGES ON public.products TO replicator;

-- Refresh publication
ALTER PUBLICATION pub_all ADD TABLE public.orders, public.products;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Arctic Mirror initialization completed successfully';
    RAISE NOTICE 'Database: %', current_database();
    RAISE NOTICE 'User: %', current_user;
    RAISE NOTICE 'Tables created: users, orders, products';
    RAISE NOTICE 'Publication: pub_all';
    RAISE NOTICE 'Replication user: replicator';
END
$$;