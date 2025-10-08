
drop table if exists silver.crm_cust_info;
create table silver.crm_cust_info (
	cst_id INT,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_material_staus varchar(50),
	cst_gndr varchar(50),
	cst_create_date date,
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id INT,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost INT,
	prd_line varchar(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT, 
	sls_quantity INT, 
	sls_price INT,
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid varchar(50),
	cntry varchar(50),
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid varchar(50),
	bdate date,
	gen varchar(50),
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50),
	dwh_create_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);