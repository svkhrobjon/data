create database BikeStore
go 
use BikeStore;
go


create schema Sales;
go

create schema Prod;
go


create table Sales.Customers
(
   customer_id int primary key
  ,first_name varchar(50)
  ,last_name varchar(50)
  ,phone varchar(50)
  ,email varchar(100)
  ,street varchar(100)
  ,city varchar(100)
  ,state varchar(50)
  ,zip_code int
);

create table Sales.Orders
(
   order_id int primary key
  ,customer_id int 
  ,order_status int
  ,order_date datetime
  ,required_date datetime
  ,shipped_date datetime
  ,store_id int
  ,staff_if int
);

create table Sales.Staffs
(
   staff_id int primary key
  ,first_name varchar(50)
  ,last_name varchar(50)
  ,email varchar(100)
  ,phone varchar(50)
  ,active bit
  ,store_id int
  ,manager_id int
);


create table Sales.Stores
(
   store_id int primary key
  ,store_name varchar(100)
  ,phone varchar(50)
  ,email varchar(100)
  ,street varchar(100)
  ,city varchar(100)
  ,state varchar(50)
  ,zip_code int
);


create table Sales.Order_items
(
   order_id int 
  ,item_id int
  ,product_id int
  ,quantity int
  ,list_price decimal(10,2)
  ,discount decimal(4,2)
  ,primary key (order_id, item_id)
);

create table Prod.Categories
(
	category_id int primary key,
	category_name varchar(100)
);

create table Prod.Brands
(
	 brand_id int primary key
	,brand_name varchar(100)
);

create table Prod.Products
(
	 product_id int primary key
	,product_name varchar(100)
	,brand_id int
	,category_id int
	,model_year int
	,list_price decimal(10,2)
);

create table Prod.Stocks
(
	 store_id int
	,product_id int
	,quantity int
	,primary key (store_id, product_id)
);

alter table Sales.Orders
add foreign key (customer_id) references Sales.Customers(customer_id),
	foreign key (store_id) references Sales.Stores(store_id),
	foreign key (staff_if) references Sales.Staffs(staff_id)

alter table Sales.Staffs
add foreign key (store_id) references Sales.Stores(store_id),
	foreign key (manager_id) references Sales.Staffs(staff_id);

alter table Sales.Order_items
add foreign key (order_id) references Sales.Orders(order_id),
	foreign key (product_id) references Prod.Products(product_id);

alter table Prod.Products
add foreign key (brand_id) references Prod.Brands(brand_id),
	foreign key (category_id) references Prod.Categories(category_id);

alter table Prod.Stocks
add foreign key (store_id) references Sales.Stores(store_id),
	foreign key (product_id) references Prod.Products(product_id);


create or alter proc sp_ImportingData
as
begin


	create table #_staging_Customers
	(
		 customer_id int,
		 first_name nvarchar(255),
		 last_name nvarchar(255),
		 phone nvarchar(255),
		 email nvarchar(255),
		 street nvarchar(255),
		 city nvarchar(255),
		 state nvarchar(255),
		 zip_code int
	);

	create table #_staging_Orders
	(
		 order_id int,
		 customer_id int,
		 order_status int,
		 order_date nvarchar(255),
		 required_date nvarchar(255),
		 shipped_date nvarchar(255),
		 store_id int,
		 staff_if int
	);

	create table #_staging_Staffs
	(
		 staff_id int,
		 first_name nvarchar(255),
		 last_name nvarchar(255),
		 email nvarchar(255),
		 phone nvarchar(255),
		 active int,
		 store_id int,
		 manager_id nvarchar(255)
	);

	create table #_staging_Stores
	(
		 store_id int,
		 store_name nvarchar(255),
		 phone nvarchar(255),
		 email nvarchar(255),
		 street nvarchar(255),
		 city nvarchar(255),
		 state nvarchar(255),
		 zip_code int
	);

	create table #_staging_Order_items
	(
		 order_id int,
		 item_id int,
		 product_id int,
		 quantity int,
		 list_price nvarchar(255),
		 discount nvarchar(255)
	);

	create table #_staging_Categories
	(
		 category_id int,
		 category_name nvarchar(255)
	);

	create table #_staging_Brands
	(
		 brand_id int,
		 brand_name nvarchar(255)
	);

	create table #_staging_Products
	(
		 product_id int,
		 product_name nvarchar(255),
		 brand_id int,
		 category_id int,
		 model_year int,
		 list_price nvarchar(255)
	);

	create table #_staging_Stocks
	(
		 store_id int,
		 product_id int,
		 quantity int
	);

	bulk insert #_staging_Customers
	from 'C:\BikeStore\customers.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Orders
	from 'C:\BikeStore\orders.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Staffs
	from 'C:\BikeStore\staffs.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Stores
	from 'C:\BikeStore\stores.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Order_items
	from 'C:\BikeStore\order_items.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Categories
	from 'C:\BikeStore\categories.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Brands
	from 'C:\BikeStore\brands.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Products
	from 'C:\BikeStore\products.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);

	bulk insert #_staging_Stocks
	from 'C:\BikeStore\stocks.csv'
	with (firstrow = 2, fieldterminator = ',', rowterminator = '\n', tablock);


	update #_staging_Customers
	set phone = null
	where phone = 'null'

	update #_staging_Orders
	set shipped_date = null
	where shipped_date = 'null'
	
	update #_staging_Staffs
	set manager_id = null
	where manager_id = 'null'


merge Sales.Stores as target
using #_staging_Stores as source
on target.store_id = source.store_id
when matched then
	update set
		store_name = source.store_name,
		phone = source.phone,
		email = source.email,
		street = source.street,
		city = source.city,
		state = source.state,
		zip_code = source.zip_code
when not matched then
	insert (store_id, store_name, phone, email, street, city, state, zip_code)
	values (source.store_id, source.store_name, source.phone, source.email, source.street, source.city, source.state, source.zip_code);


merge Prod.Categories as target
using #_staging_Categories as source
on target.category_id = source.category_id
when matched then
	update set category_name = source.category_name
when not matched then
	insert (category_id, category_name)
	values (source.category_id, source.category_name);



merge Prod.Brands as target
using #_staging_Brands as source
on target.brand_id = source.brand_id
when matched then
	update set brand_name = source.brand_name
when not matched then
	insert (brand_id, brand_name)
	values (source.brand_id, source.brand_name);



merge Prod.Products as target
using (
	select product_id, product_name, brand_id, category_id, model_year,
		cast(list_price as decimal(10,2)) as list_price
	from #_staging_Products
) as source
on target.product_id = source.product_id
when matched then
	update set
		product_name = source.product_name,
		brand_id = source.brand_id,
		category_id = source.category_id,
		model_year = source.model_year,
		list_price = source.list_price
when not matched then
	insert (product_id, product_name, brand_id, category_id, model_year, list_price)
	values (source.product_id, source.product_name, source.brand_id, source.category_id, source.model_year, source.list_price);


merge Sales.Customers as target
using #_staging_Customers as source
on target.customer_id = source.customer_id
when matched then
	update set
		first_name = source.first_name,
		last_name = source.last_name,
		phone = source.phone,
		email = source.email,
		street = source.street,
		city = source.city,
		state = source.state,
		zip_code = source.zip_code
when not matched then
	insert (customer_id, first_name, last_name, phone, email, street, city, state, zip_code)
	values (source.customer_id, source.first_name, source.last_name, source.phone, source.email, source.street, source.city, source.state, source.zip_code);



merge Sales.Staffs as target
using (
	select staff_id, first_name, last_name, email, phone, active, store_id,
		cast(case when manager_id = 'NULL' then NULL else manager_id end as int) as manager_id
	from #_staging_Staffs
) as source
on target.staff_id = source.staff_id
when matched then
	update set
		first_name = source.first_name,
		last_name = source.last_name,
		email = source.email,
		phone = source.phone,
		active = source.active,
		store_id = source.store_id,
		manager_id = source.manager_id
when not matched then
	insert (staff_id, first_name, last_name, email, phone, active, store_id, manager_id)
	values (source.staff_id, source.first_name, source.last_name, source.email, source.phone, source.active, source.store_id, source.manager_id);


merge Sales.Orders as target
using (
	select order_id, customer_id, order_status,
		cast(case when order_date = 'NULL' then NULL else order_date end as datetime) as order_date,
		cast(case when required_date = 'NULL' then NULL else required_date end as datetime) as required_date,
		cast(case when shipped_date = 'NULL' then NULL else shipped_date end as datetime) as shipped_date,
		store_id, staff_if
	from #_staging_Orders
) as source
on target.order_id = source.order_id
when matched then
	update set
		customer_id = source.customer_id,
		order_status = source.order_status,
		order_date = source.order_date,
		required_date = source.required_date,
		shipped_date = source.shipped_date,
		store_id = source.store_id,
		staff_if = source.staff_if
when not matched then
	insert (order_id, customer_id, order_status, order_date, required_date, shipped_date, store_id, staff_if)
	values (source.order_id, source.customer_id, source.order_status, source.order_date, source.required_date, source.shipped_date, source.store_id, source.staff_if);



merge Sales.Order_items as target
using (
	select order_id, item_id, product_id, quantity,
		cast(list_price as decimal(10,2)) as list_price,
		cast(discount as decimal(4,2)) as discount
	from #_staging_Order_items
) as source
on target.order_id = source.order_id and target.item_id = source.item_id
when matched then
	update set
		product_id = source.product_id,
		quantity = source.quantity,
		list_price = source.list_price,
		discount = source.discount
when not matched then
	insert (order_id, item_id, product_id, quantity, list_price, discount)
	values (source.order_id, source.item_id, source.product_id, source.quantity, source.list_price, source.discount);


merge Prod.Stocks as target
using #_staging_Stocks as source
on target.store_id = source.store_id and target.product_id = source.product_id
when matched then
	update set quantity = source.quantity
when not matched then
	insert (store_id, product_id, quantity)
	values (source.store_id, source.product_id, source.quantity);
end	

exec sp_ImportingData

select object_schema_name(object_id('sp_ImportingData')) as sch,
       object_name(object_id('sp_ImportingData')) as name;

exec sp_helptext 'sp_ImportingData';


--vw_StoreSalesSummary: Revenue, #Orders, AOV per store

create or alter view vw_StoreSalesSummary 
as 
(
select 
	s.store_id, s.store_name,
	cast(sum(quantity * list_price * (1 - discount)) as decimal(18,2)) as Revenue,
	count(o.order_id) as number_of_orders,
	cast( avg( distinct quantity * list_price * (1 - discount)) as decimal(18,2)) AOV

from 
[Sales].[Stores] s join [Sales].[Orders] o 
on s.store_id = o.store_id join [Sales].[Order_items] oi
on o.order_id = oi.order_id
group by s.store_id, s.store_name
)

select * from vw_StoreSalesSummary

-- w_TopSellingProducts: Rank products by total sales

create or alter view vw_TopSellingProducts 
as
(
select 
	p.product_id,
	p.product_name,
	cast(sum(quantity * oi.list_price * (1 - discount)) as decimal(18,2)) as products_total_sales,
	rank() over(order by sum(quantity * oi.list_price * (1 - discount)) desc ) as rank_product

from [Prod].[Products] p join [Sales].[Order_items] oi
on p.product_id =oi.product_id
group by 	p.product_id,p.product_name
)

select * from vw_TopSellingProducts

-- vw_InventoryStatus: Items running low on stock

create or alter view vw_InventoryStatus 
as
(
select 
	st.store_id,
	st.store_name, 
	p.product_id, 
	p.product_name, 
	quantity
from Prod.Products p left join
prod.Stocks s on s.product_id = p.product_id
join sales.Stores st on s.store_id = st.store_id
where quantity < 10
)


--vw_StaffPerformance: Orders and revenue handled per staff

--staff, order_items, order

create or alter view vw_StaffPerformance
as
(
select 
	staff_id, concat(first_name, ' ', last_name) as full_name,
	count(distinct oi.order_id) as number_orders,
	cast( sum(quantity * list_price * (1 - discount) ) as decimal(18,2)) as revenue_per_staff

from Sales.Staffs as s join Sales.Orders as o 
on s.staff_id = o.staff_if join Sales.Order_items oi
on o.order_id = oi.order_id
group by staff_id, concat(first_name, ' ', last_name)
)

select * from vw_StaffPerformance


-- vw_RegionalTrends: Revenue by city or region

create or alter view vw_RegionalTrends
as
(
select 
	city,
	state,
	cast( sum(quantity * list_price * (1 - discount) ) as decimal(18,2)) as regional_revenue
from Sales.Customers c join Sales.Orders o
on c.customer_id = o.customer_id join Sales.Order_items oi
on o.order_id = oi.order_id
group by city, state
)

select * from vw_RegionalTrends


-- vw_SalesByCategory: Sales volume and margin by product category

create or alter view vw_SalesByCategory 
as
(
select 
	category_name,
	sum(oi.quantity) as total_units_sold,
	cast( sum(quantity * oi.list_price * (1 - discount) ) as decimal(18,2)) as revenue_category,
	cast( sum(quantity * oi.list_price * (1 - discount) * 0.4 ) as decimal(18,2)) as estimated_margin

from Sales.Order_items oi join Prod.Products p 
on oi.product_id = p.product_id join Prod.Categories as c
on p.category_id = c.category_id
group by category_name
)

select * from vw_SalesByCategory



create or alter procedure sp_CalculateStoreKPI
    @StoreId int,
    @StartDate date = NULL,
    @EndDate date = NULL
as
begin
    set nocount on;

    ;with OrderRevenue as (
        select
            o.order_id,
            sum(oi.quantity * oi.list_price * (1 - oi.discount)) as order_revenue
        from Sales.Orders o
        join Sales.Order_items oi on oi.order_id = o.order_id
        where o.store_id = @StoreId
          and (@StartDate is null or cast(o.order_date as date) >= @StartDate)
          and (@EndDate   is null or cast(o.order_date as date) <= @EndDate)
        group by o.order_id
    )
    select
        @StoreId as store_id,
        (select store_name from Sales.Stores where store_id = @StoreId) as store_name,
        @StartDate as start_date,
        @EndDate as end_date,

        -- Sales KPIs
        cast(isnull(sum(orv.order_revenue),0) as decimal(18,2)) as total_revenue,
        count(orv.order_id) as number_of_orders,
        cast(isnull(sum(orv.order_revenue),0) / nullif(count(orv.order_id),0) as decimal(18,2)) as aov,

        -- Inventory KPIs
        (select count(*) from Prod.Stocks where store_id = @StoreId and quantity < 10) as low_stock_sku_count,
        (select sum(quantity) from Prod.Stocks where store_id = @StoreId) as total_units_in_stock,

        -- Staff KPIs
        (select count(*) from Sales.Staffs where store_id = @StoreId and active = 1) as active_staff_count,
        cast(
            isnull(sum(orv.order_revenue),0) /
            nullif((select count(*) from Sales.Staffs where store_id = @StoreId and active = 1), 0)
        as decimal(18,2)) as revenue_per_active_staff
    from OrderRevenue orv;
end
go

exec sp_CalculateStoreKPI @StoreId = 2;
exec sp_CalculateStoreKPI @StoreId = 2, @StartDate='2018-01-01', @EndDate='2018-12-31';



create or alter procedure sp_GenerateRestockList
    @StoreId int,
    @ReorderLevel int = 10,   -- kam stock threshol'd
    @TargetLevel int = 25     -- to'ldirganda nechiga yetkazamiz
as
begin
    set nocount on;

    select
        s.store_id,
        st.store_name,
        s.product_id,
        p.product_name,
        s.quantity as current_qty,
        @ReorderLevel as reorder_level,
        @TargetLevel as target_level,
        (@TargetLevel - s.quantity) as suggested_order_qty,
        cast(p.list_price as decimal(10,2)) as current_list_price
    from Prod.Stocks s
    join Sales.Stores st on st.store_id = s.store_id
    join Prod.Products p on p.product_id = s.product_id
    where s.store_id = @StoreId
      and s.quantity < @ReorderLevel
    order by suggested_order_qty desc, p.product_name;
end
go

exec sp_GenerateRestockList @StoreId = 1;
exec sp_GenerateRestockList @StoreId = 1, @ReorderLevel = 5, @TargetLevel = 20;

create or alter procedure sp_CompareSalesYearOverYear
    @Year1 int,
    @Year2 int,
    @StoreId int = NULL  
as
begin
    set nocount on;

    ;with Base as (
        select
            year(o.order_date) as sales_year,
            o.order_id,
            sum(oi.quantity * oi.list_price * (1 - oi.discount)) as order_revenue
        from Sales.Orders o
        join Sales.Order_items oi on oi.order_id = o.order_id
        where year(o.order_date) in (@Year1, @Year2)
          and (@StoreId is null or o.store_id = @StoreId)
        group by year(o.order_date), o.order_id
    ),
    Agg as (
        select
            sales_year,
            sum(order_revenue) as revenue,
            count(*) as orders,
            sum(order_revenue) / nullif(count(*), 0) as aov
        from Base
        group by sales_year
    )
    select
        isnull(cast(@StoreId as varchar(20)), 'ALL') as scope_store,

        @Year1 as year1,
        cast(isnull((select revenue from Agg where sales_year = @Year1), 0) as decimal(18,2)) as revenue_year1,
        isnull((select orders from Agg where sales_year = @Year1), 0) as orders_year1,
        cast(isnull((select aov from Agg where sales_year = @Year1), 0) as decimal(18,2)) as aov_year1,

        @Year2 as year2,
        cast(isnull((select revenue from Agg where sales_year = @Year2), 0) as decimal(18,2)) as revenue_year2,
        isnull((select orders from Agg where sales_year = @Year2), 0) as orders_year2,
        cast(isnull((select aov from Agg where sales_year = @Year2), 0) as decimal(18,2)) as aov_year2,

        -- YoY growth (Revenue)
        cast(
            (isnull((select revenue from Agg where sales_year = @Year2), 0)
             - isnull((select revenue from Agg where sales_year = @Year1), 0))
            / nullif(isnull((select revenue from Agg where sales_year = @Year1), 0), 0)
        as decimal(18,4)) as yoy_revenue_growth_rate
    ;
end
go

exec sp_CompareSalesYearOverYear @Year1 = 2017, @Year2 = 2018;       -- company-wide
exec sp_CompareSalesYearOverYear @Year1 = 2017, @Year2 = 2018, @StoreId = 1;


create or alter procedure sp_GetCustomerProfile
    @CustomerId int,
    @TopN int = 5
as
begin
    set nocount on;

    -- 1) Customer Summary
    ;with OrderRevenue as (
        select
            o.order_id,
            sum(oi.quantity * oi.list_price * (1 - oi.discount)) as order_revenue
        from Sales.Orders o
        join Sales.Order_items oi on oi.order_id = o.order_id
        where o.customer_id = @CustomerId
        group by o.order_id
    )
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.phone,
        c.email,
        count(orv.order_id) as total_orders,
        cast(isnull(sum(orv.order_revenue),0) as decimal(18,2)) as total_spend,
        cast(isnull(sum(orv.order_revenue),0) / nullif(count(orv.order_id),0) as decimal(18,2)) as aov
    from Sales.Customers c
    left join OrderRevenue orv on 1=1
    where c.customer_id = @CustomerId
    group by c.customer_id, c.first_name, c.last_name, c.phone, c.email;

    -- 2) Most Bought Items (Top N)
    select top (@TopN)
        p.product_id,
        p.product_name,
        sum(oi.quantity) as total_qty,
        cast(sum(oi.quantity * oi.list_price * (1 - oi.discount)) as decimal(18,2)) as revenue
    from Sales.Orders o
    join Sales.Order_items oi on oi.order_id = o.order_id
    join Prod.Products p on p.product_id = oi.product_id
    where o.customer_id = @CustomerId
    group by p.product_id, p.product_name
    order by total_qty desc, revenue desc;
end
go

exec sp_GetCustomerProfile @CustomerId = 1;
exec sp_GetCustomerProfile @CustomerId = 1, @TopN = 10;

-- vw_Total_Revenue

create or alter view vw_TotalRevenue
as
select cast(sum(oi.quantity * oi.list_price * (1 - oi.discount)) as decimal(18,2)) as total_revenue
from Sales.Order_items oi;
go

select*from vw_TotalRevenue

-- vw_Inventory_Turnover

create or alter view vw_InventoryTurnoverProxy
as
with Rev as (
  select o.store_id,
         sum(oi.quantity * oi.list_price * (1 - oi.discount)) as revenue
  from Sales.Orders o
  join Sales.Order_items oi on oi.order_id = o.order_id
  group by o.store_id
),
Inv as (
  select store_id, sum(quantity) as current_units
  from Prod.Stocks
  group by store_id
)
select
  r.store_id,
  cast(r.revenue as decimal(18,2)) as revenue,
  i.current_units,
  cast(r.revenue / nullif(i.current_units,0) as decimal(18,4)) as inventory_turnover_proxy
from Rev r
join Inv i on i.store_id = r.store_id;
go

select*from vw_InventoryTurnoverProxy

-- vw_sales_by_brand

create or alter view vw_SalesByBrand
as
select
  b.brand_id,
  b.brand_name,
  sum(oi.quantity) as units_sold,
  cast(sum(oi.quantity * oi.list_price * (1 - oi.discount)) as decimal(18,2)) as brand_revenue
from Sales.Order_items oi
join Prod.Products p on p.product_id = oi.product_id
join Prod.Brands b on b.brand_id = p.brand_id
group by b.brand_id, b.brand_name;
go

select*from vw_SalesByBrand
