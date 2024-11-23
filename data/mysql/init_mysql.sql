SET GLOBAL local_infile=1;

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS brands;
DROP TABLE IF EXISTS categories;

CREATE TABLE IF NOT EXISTS brands (
    brand_id int primary key,
    brand_name varchar(100),
    created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS categories (
    category_id int primary key,
    category_name varchar(100),
    created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id int primary key,
    product_name varchar(100),
    brand_id int,
    category_id int,
    model_year int,
    list_price float,
    created_at TIMESTAMP DEFAULT '2015-01-01 00:00:00',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    foreign key (brand_id) references brands(brand_id),
    foreign key (category_id) references categories(category_id)
);


CREATE TABLE IF NOT EXISTS stocks (
    store_id int,
    product_id int,
    quantity float,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,    
    primary key(store_id, product_id)
);

LOAD DATA INFILE '/var/lib/mysql-files/categories.csv' INTO TABLE categories FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (category_id,category_name);
LOAD DATA INFILE '/var/lib/mysql-files/brands.csv' INTO TABLE brands FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (brand_id,brand_name);
LOAD DATA INFILE '/var/lib/mysql-files/products.csv' INTO TABLE products FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (product_id,product_name,brand_id,category_id,model_year,list_price);
LOAD DATA INFILE '/var/lib/mysql-files/stocks.csv' INTO TABLE stocks FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS (store_id,product_id,quantity);
