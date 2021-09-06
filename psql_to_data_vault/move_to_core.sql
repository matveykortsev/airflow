
-- Заполняем хабы
insert into core.h_orders (order_id, source_system, processed_dttm )
select orderid, 'Postgres', now() from stage.orders 
on conflict do nothing ;


insert into core.h_products (product_id, source_system, processed_dttm )
select productid, 'Postgres', now() from stage.products 
on conflict do nothing ;


insert into core.h_suppliers (supplierid_id, source_system, processed_dttm )
select supplierid, 'Postgres', now() from stage.suppliers 
on conflict do nothing ;

-- Заполняем сателлиты хабов
insert into core.s_orders (h_order_rk, orderdate, orderstatus, orderpriority, clerk, source_system, valid_from_dttm, valid_to_dttm, processed_dttm)
(select ho.h_order_rk , orderdate, orderstatus, orderpriority, clerk, 'Postgres', now(), now() + interval '1 year', now()
from stage.orders o 
join core.h_orders ho 
on o.orderid = ho.order_id)
on conflict do nothing;


insert into core.s_products (h_product_rk, productname, producttype, productsize, retailprice, source_system, valid_from_dttm, valid_to_dttm, processed_dttm)
(select hp.h_product_rk, productname, producttype, productsize, retailprice, 'Postgres', now(), now() + interval '1 year', now()
from stage.products as p
join core.h_products as hp 
on p.productid = hp.product_id)
on conflict do nothing;


insert into core.s_suppliers (h_supplier_rk, suppliername, address, phone, balance, descr, source_system, valid_from_dttm, valid_to_dttm, processed_dttm)
(select h_supplier_rk, suppliername, address, phone, balance, descr, 'Postgres', now(), now() + interval '1 year', now()
from stage.suppliers as s
join core.h_suppliers as hs 
on s.supplierid = hs.supplierid_id)
on conflict do nothing;


-- заполняем линки
insert into core.l_orderdetails (h_order_rk, h_product_rk, source_system, processed_dttm )
select orderid, productid, 'Postgres', now() from stage.orderdetails 
on conflict do nothing ;


insert into core.l_productsuppl (h_product_rk, h_supplier_rk, source_system, processed_dttm )
select productid, supplierid, 'Postgres', now() from stage.productsuppl 
on conflict do nothing ;


-- заполняем сателлиты линков

insert into core.l_s_orderdetails (l_order_details_rk, unitprice, quantity, discount, source_system, valid_from_dttm, valid_to_dttm, processed_dttm)
(select lo.l_order_details_rk, unitprice, quantity, discount, 'Postgres', now(), now() + interval '1 year', now()
from stage.orderdetails as o 
join core.l_orderdetails lo
on o.orderid = lo.h_order_rk and o.productid = lo.h_product_rk)
on conflict do nothing;


insert into core.l_s_productsuppl (l_product_suppliers_rk, qty, supplycost, descr, source_system, valid_from_dttm, valid_to_dttm, processed_dttm)
(select lp.l_product_suppliers_rk, qty, supplycost, descr, 'Postgres', now(), now() + interval '1 year', now()
from stage.productsuppl p
join core.l_productsuppl lp
on p.productid = lp.h_product_rk and p.supplierid = lp.h_supplier_rk)
on conflict do nothing;
