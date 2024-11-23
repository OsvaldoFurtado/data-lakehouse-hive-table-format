#!/bin/sh

/usr/bin/mc config host add myminio http://minio:9000 minioadmin minioadmin

# Creates lakehouse bucket
/usr/bin/mc rm -r --force myminio/lakehouse

# Creates bucket structure - bronze
/usr/bin/mc mb myminio/lakehouse/bronze/categories
/usr/bin/mc mb myminio/lakehouse/bronze/brands
/usr/bin/mc mb myminio/lakehouse/bronze/products
/usr/bin/mc mb myminio/lakehouse/bronze/customers
/usr/bin/mc mb myminio/lakehouse/bronze/stores
/usr/bin/mc mb myminio/lakehouse/bronze/staffs
/usr/bin/mc mb myminio/lakehouse/bronze/orders
/usr/bin/mc mb myminio/lakehouse/bronze/order_items
/usr/bin/mc mb myminio/lakehouse/bronze/stocks

# Creates bucket structure - silver
/usr/bin/mc mb myminio/lakehouse/silver/products
/usr/bin/mc mb myminio/lakehouse/silver/customers
/usr/bin/mc mb myminio/lakehouse/silver/stores
/usr/bin/mc mb myminio/lakehouse/silver/staffs
/usr/bin/mc mb myminio/lakehouse/silver/orders
/usr/bin/mc mb myminio/lakehouse/silver/order_items
/usr/bin/mc mb myminio/lakehouse/silver/stocks

# Sets bucket download policy
/usr/bin/mc policy download myminio/lakehouse

exit 0
