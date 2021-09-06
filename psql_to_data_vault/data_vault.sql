create schema if not exists core;

create table if not exists core.h_orders (
    h_order_rk SERIAL PRIMARY KEY,
    order_id int4 UNIQUE,
    source_system text,
    processed_dttm date
);


create table if not exists core.s_orders (
    h_order_rk int UNIQUE,
	orderdate date NULL,
	orderstatus varchar(1) NULL,
	orderpriority varchar(15) NULL,
	clerk varchar(15) NULL,
    source_system text,
    valid_from_dttm date,
    valid_to_dttm date,
    processed_dttm date
);


create table if not exists core.h_products (
    h_product_rk SERIAL PRIMARY KEY,
    product_id int4 UNIQUE,
    source_system text,
    processed_dttm date
);


create table if not exists core.s_products (
    h_product_rk int UNIQUE,
	productname varchar(55) NULL,
	producttype varchar(25) NULL,
	productsize int4 NULL,
	retailprice numeric(15,2) NULL,
    source_system text,
    valid_from_dttm date,
    valid_to_dttm date,
    processed_dttm date
);

create table if not exists core.h_suppliers (
    h_supplier_rk SERIAL PRIMARY KEY,
    supplierid_id int4 UNIQUE,
    source_system text,
    processed_dttm date
);


create table if not exists core.s_suppliers (
    h_supplier_rk int UNIQUE,
	suppliername varchar(25) NULL,
	address varchar(40) NULL,
	phone varchar(15) NULL,
	balance numeric(15,2) NULL,
	descr varchar(101) NULL,
    source_system text,
    valid_from_dttm date,
    valid_to_dttm date,
    processed_dttm date
);


CREATE TABLE if not exists core.l_orderdetails (
    l_order_details_rk SERIAL PRIMARY KEY,
    h_order_rk int,
    h_product_rk int,
    source_system text,
    processed_dttm date,
    UNIQUE (h_order_rk, h_product_rk)
);


create table if not exists core.l_s_orderdetails (
    l_order_details_rk int UNIQUE,
    unitprice numeric(15,2) NULL,
	quantity numeric(15,2) NULL,
	discount numeric(15,2) NULL,
    source_system text,
    valid_from_dttm date,
    valid_to_dttm date,
    processed_dttm date
);


CREATE TABLE if not exists core.l_productsuppl (
    l_product_suppliers_rk SERIAL PRIMARY KEY,
    h_product_rk int,
    h_supplier_rk int,
    source_system text,
    processed_dttm date,
    UNIQUE (h_product_rk, h_supplier_rk)
);

create table if not exists core.l_s_productsuppl (
    l_product_suppliers_rk int UNIQUE,
    qty int4 NULL,
	supplycost numeric(15,2) NULL,
	descr varchar(199) NULL,
    source_system text,
    valid_from_dttm date,
    valid_to_dttm date,
    processed_dttm date
);