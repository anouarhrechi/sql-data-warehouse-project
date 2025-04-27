/*
================================================================================
Quality Checks
================================================================================

Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after loading the Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
================================================================================
*/

--cheking table silver
print'=============================================='
print'->>checking table : crm_cust_info'
print'=============================================='
--check dublicate
select 
	cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

select 
	*
from bronze.crm_cust_info
where cst_id=29466;


select 
	*
from(
select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_date
from bronze.crm_cust_info
)t where flag_date>1;

--check unwanted spaces

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);
-------------------------------------------------
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname);

-------------------------------------------------
use DataWarehouse;
select * 
from bronze.crm_cust_info;
------------------------------------------------
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_fitstname,
	trim(cst_lastname) as cst_lastname,

	case when upper(trim(cst_marital_status)) ='S' then 'Single'
		 when upper(trim(cst_marital_status)) = 'M' then 'Maried'
		 ELSE 'n/a'
	end cst_marital_status,
	case when upper(trim(cst_gndr)) ='F' then 'Femal'
		 when upper(trim(cst_gndr)) = 'M' then 'Male'
		 ELSE 'n/a'
	end cst_gndr,
	cst_create_date
from(
select 
	*,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_date
from bronze.crm_cust_info
)t where flag_date=1;
-----------------------------------------------------
select 
	cst_id,
	count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;
-----------------------------------------------------------------
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);
-----------------------------------------------------------------
select distinct cst_gndr
from silver.crm_cust_info;
print'========================================================='
print'cheking table : crm_cust_info'
print'========================================================='
use datawarehouse;
-------------------------------------------------------------

--check dublicate
select  *
from bronze.crm_prd_info;
-------------------------------------
select prd_id,
		count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null;
-------------------------------------------------
insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
)
select 
	prd_id,
	replace(substring(prd_key ,1 ,5),'-','_') as cat_id,
	substring(prd_key ,7 ,len(prd_key)) as  prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case when upper(trim(prd_line))= 'M' THEN 'Mountain'
		 when upper(trim(prd_line))= 'R'THEN 'Road'
		 when upper(trim(prd_line))= 'S'THEN  'Other sales'
		 when upper(trim(prd_line))= 'T'THEN  'Touring'
	     ELSE 'n/a'
	end as prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast( lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1as date) as prd_end_dt
from bronze.crm_prd_info;
--------------------------------------------------------------
--check unwanted spaces
select prd_key
from bronze.crm_prd_info
where prd_key != trim(prd_key);
----------------------------------------------------------------
--check for nulls or negative number
select 
	prd_cost
from bronze.crm_prd_info
------------------------------------------
--check the possibillity of each cas in prd_line?
select distinct prd_line
from bronze.crm_prd_info;
-------------------------------------------------------------------------------
select prd_id,
	   prd_key,
	   prd_nm,
	   prd_start_dt,
	   prd_end_dt,
	   lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1 as prd_end_dt
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509');
---------------------------------------------------------------
--check the quality
select  *
from silver.crm_prd_info;
print'============================================================'
print'checking table : crm_sales_details'
print'============================================================'
use datawarehouse;
----------------------------------------
select * 
from bronze.crm_sales_details;
---------------------------------------------------
insert into silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt =0 or len(sls_order_dt) !=8 then null
		else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt =0 or len(sls_ship_dt) !=8 then null
		else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt =0 or len(sls_due_dt) !=8 then null
		else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * sls_price 
			 then abs(sls_quantity * sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0 
			 then  abs(sls_sales) / nullif(abs(sls_quantity),0)
			 else sls_price
		end as sls_price
from bronze.crm_sales_details;
-----------------------------------------
-- check for invalid dates
select
		nullif(sls_order_dt,0) sls_oder_dt
from bronze.crm_sales_details
where sls_order_dt <= 0;
----------------------------------------------
--check Data consistency:Between Sales,Quantity, and Price
-->> Sales = Quantity * price 
-->> values must be not null ,zero, or negative
select distinct
sls_sales  as old_sales,
sls_quantity,
sls_price as old_price,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * sls_price 
			 then abs(sls_quantity * sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0 
			 then  abs(sls_sales) / nullif(abs(sls_quantity),0)
			 else sls_price
		end as sls_price
from bronze.crm_sales_details;
------------------------------------------------------
select sls_sales,sls_quantity,sls_price
from bronze.crm_sales_details
where sls_sales is null
or sls_sales <= 0
or sls_quantity is null
or sls_quantity <=0
or sls_price is null
or sls_price <=0;
------------------------------------------------
select * 
from silver.crm_sales_details;
print'==========================================================='
print'checking table : erp_cust_AZ12'
print'==============================================================='
use datawarehouse;
-----------------------------------------
select *
from bronze.erp_cust_AZ12;
-------------------------------------------
insert into silver.erp_cust_AZ12 ( cid ,bdate,gen)
select 
		case when cid like 'NAS%' then substring(cid,4,len(cid))
		     else cid
		end cid,
		case when bdate > getdate() then  null
			 else bdate 
		end bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in('M','MALE') then 'Male'
		     else 'n/a'
		end gen
from bronze.erp_cust_AZ12;
----------------------------------------------------
--check duplicates
select 
		cid,
		count(*)
from bronze.erp_cust_AZ12
group by cid
having count(*)>1 or cid is null;
-----------------------------------------------------
select 
		cid,
		bdate,
		gen
from bronze.erp_cust_AZ12
where cid like '%AW00011000%'
------------------------------------------------------
select 
		case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid 
		end cid,
		bdate,
		gen
from bronze.erp_cust_AZ12;
--Identify out-of-range Dates
select distinct  bdate
from bronze.erp_cust_AZ12
where bdate< '1924-01-01' or bdate > getdate()
------------------------------------------------------
select distinct case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in('M','MALE') then 'Male'
		     else 'n/a'
		end gen
from bronze.erp_cust_AZ12;

-----------------------------------------------------------
select *
from silver.erp_cust_AZ12;
print'==========================================================='
print'checking table : erp_loc_A101'
print'==========================================================='
use datawarehouse;
-------------------------------------------
insert into silver.erp_loc_A101 (cid,cntry)
select 
		replace(cid,'-','') as cid,
		case when trim(cntry)='DE' then 'Germany'
		     when trim(cntry) in('US', 'USA') then 'United States'
			 when trim(cntry)= '' or cntry is null then 'n/a'
			 else cntry
		end cntry
from bronze.erp_loc_A101;
------------------------------------------------
select cid
from bronze.erp_loc_A101
where cid != trim(cid);
----------------------------------------------
select distinct cntry  
from bronze.erp_loc_A101
------------------------------------------
select cntry 
from bronze.erp_loc_A101
where cntry != trim(cntry);
------------------------------------------
select *
from silver.erp_loc_A101;

select  distinct
cntry as old_cntry,

		case when trim(cntry)='DE' then 'Germany'
		     when trim(cntry) in('US', 'USA') then 'United States'
			 when trim(cntry)= '' or cntry is null then 'n/a'
			 else cntry
		end cntry
from bronze.erp_loc_A101
order by cntry asc;
print'=============================================================='
print' checking table : erp_PX_CAT_G1V2'
print'=============================================================='
use datawarehouse;
------------------------------------
select *
from bronze.erp_PX_CAT_G1V2;
--------------------------------------
insert into silver.erp_PX_CAT_G1V2 (id,cat,subcat,maintenance)
select 
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_PX_CAT_G1V2;
-----------------------------------------
--check for unwated spaces 
select id
from bronze.erp_PX_CAT_G1V2
where id != trim(id)
---------------------------------
select cat
from bronze.erp_PX_CAT_G1V2
where cat != trim(cat)
--------------------------------------
select subcat
from bronze.erp_PX_CAT_G1V2
where subcat != trim(subcat)
-------------------------------------
select MAINTENANCE
from bronze.erp_PX_CAT_G1V2
where maintenance != trim(MAINTENANCE)
-- data standarization & consistency

select distinct maintenance 
from bronze.erp_PX_CAT_G1V2

select distinct subcat 
from bronze.erp_PX_CAT_G1V2
---***********-------------------**********----------
select * 
from silver.erp_PX_CAT_G1V2
