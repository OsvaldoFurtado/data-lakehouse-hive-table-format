CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;



CREATE TABLE customers (
  customer_id INT primary KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  phone VARCHAR(50),
  email VARCHAR(50),
  street VARCHAR(100),
  city VARCHAR(100),
  state VARCHAR(10),
  zip_code VARCHAR(20),
  created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER setting_updated_at_customers
BEFORE UPDATE ON customers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TABLE stores (
  store_id INT primary KEY,
  store_name VARCHAR(100),
  phone VARCHAR(50),
  email VARCHAR(50),
  street VARCHAR(100),
  city VARCHAR(100),
  state VARCHAR(10),
  zip_code VARCHAR(20),
  created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TRIGGER setting_updated_at_stores
BEFORE UPDATE ON stores
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TABLE staffs (
  staff_id INT primary KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(50),
  phone VARCHAR(50),
  active INT,
  store_id INT REFERENCES stores(store_id),
  manager_id INT REFERENCES staffs(staff_id),
  created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER setting_updated_at_staffs
BEFORE UPDATE ON staffs
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();



CREATE TABLE orders (
  order_id INT primary KEY,
  customer_id INT REFERENCES customers(customer_id),
  order_status INT,
  order_date DATE,
  required_date DATE,
  shipped_date DATE,
  store_id INT REFERENCES stores(store_id),
  staff_id INT REFERENCES staffs(staff_id),
  created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER setting_updated_at_orders
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


CREATE TABLE order_items (
  order_id INT,
  item_id INT,
  product_id INT,
  quantity FLOAT,
  list_price FLOAT,
  discount FLOAT,
  created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(order_id,item_id)
);

CREATE TRIGGER setting_updated_at_order_items
BEFORE UPDATE ON order_items
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();


  
-- Carregar dados da tabela 'customers'
\COPY customers(customer_id,first_name,last_name,phone,email,street,city,state,zip_code) FROM '/docker-entrypoint-initdb.d/customers.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NULL');


-- Carregar dados da tabela 'stores'
\COPY stores(store_id,store_name,phone,email,street,city,state,zip_code) FROM '/docker-entrypoint-initdb.d/stores.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NULL');


-- Carregar dados da tabela 'staffs'
\COPY staffs(staff_id,first_name,last_name,email,phone,active,store_id,manager_id) FROM '/docker-entrypoint-initdb.d/staffs.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NULL');


-- Carregar dados da tabela 'orders'
\COPY orders(order_id,customer_id,order_status,order_date,required_date,shipped_date,store_id,staff_id) FROM '/docker-entrypoint-initdb.d/orders.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NULL');


-- Carregar dados da tabela 'order_items'
\COPY order_items(order_id,item_id,product_id,quantity,list_price,discount) FROM '/docker-entrypoint-initdb.d/order_items.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL 'NULL');