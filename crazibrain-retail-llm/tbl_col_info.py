# tbl_col_info.py

def table_info_and_examples(query: str = "") -> str:
    """
    Returns full schema and example SQL queries for a LangChain SQL agent.
    """
    return """
📊 **DATABASE SCHEMA & EXAMPLES**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 📃️ **TABLE STRUCTURE:**


## New Structure

1. Table name : Distributor_Closing_Stock
	SKU_Code	STRING	Unique identifier for the product (SKU)
 	SKU Name	STRING	Name of the product (SKU)
 	SKU_Short_Description	STRING	Short name or description of the product
 	Brand_Name	STRING	Name of the product brand
 	Brand_Code	STRING	Code representing the brand
 	Distributor_Code	STRING	Code for the distributor
 	Quantity	FLOAT	Quantity of product present with distributor
 	Closing Stock Date	DATETIME	Date when stock count was recorded
Retailer_Order_Summary	Retailer code	STRING	Code representing a retailer
Retailer_Order_Summary	order_number	STRING	Unique number assigned to an order
Retailer_Order_Summary	shipping_details	STRING	Information related to delivery address
Retailer_Order_Summary	City Code	STRING	Code or name of the city
Retailer_Order_Summary	subtotal	FLOAT	Partial total before discounts and charges
Retailer_Order_Summary	total_discount	FLOAT	Total discount on the order
Retailer_Order_Summary	shipping_charge	FLOAT	Shipping charges on the order
Retailer_Order_Summary	total_amount	FLOAT	Total amount after all charges and discounts
Retailer_Order_Summary	Order Status	INT	Status of the order
Retailer_Order_Summary	created_at	DATETIME	Date and time when Order was created
Retailer_Order_Summary	updated_at	DATETIME	Date and time when Order was last updated
Retailer Order Product Details	order_number	INT	Unique number assigned to an order
Retailer Order Product Details	SKU Code	INT	Unique identifier for the product (SKU)
Retailer Order Product Details	SKU Name	STRING	Name of the product (SKU)
Retailer Order Product Details	Order Quantity	FLOAT	item quantity ordered by Retailer
Retailer Order Product Details	price	FLOAT	Price of a single unit
Retailer Order Product Details	subtotal	FLOAT	Partial total before discounts and charges
Retailer Order Product Details	created_at	DATETIME	Date and time when record was created
Product_Master	Segment_Code	STRING	Code representing Category or classification of the product
Product_Master	Segment_Name_Category	STRING	Category name or classification name of the product
Product_Master	Brand_Code	STRING	Code representing the brand
Product_Master	Brand_Name	STRING	Name of the product brand
Product_Master	Parent_Code	STRING	Code for the parent product
Product_Master	Parent_Name	STRING	Name of the Parent product
Product_Master	SKU_Code	STRING	Unique identifier for the product (SKU)
Product_Master	SKU_Name	STRING	Name of the product (SKU)
Product_Master	SKU_Short_Description	STRING	Short name or description of the product
Product_Master	MRP	FLOAT	Maximum retail price of product
Product_Master	Active	STRING	shows if product is active or not
Product_Master	Price_to_retailer	FLOAT	Price of a single unit at which retailer buys
Retailer_Master	Region_Code	STRING	Geographical region code of retailer
Retailer_Master	Region_Name	STRING	Geographical region name of retailer
Retailer_Master	RSM_Code	STRING	Regional Sales Manager code
Retailer_Master	RSM_Area	STRING	Regional Sales Manager name
Retailer_Master	SO	STRING	Sales Officer code
Retailer_Master	Distributor_Code	STRING	Code for the distributor
Retailer_Master	Distributor_Name	STRING	Distributor Name supplying goods to retailer
Retailer_Master	Distributor_Channel_Type	STRING	Distributor_Channel_Type
Retailer_Master	Distributor_Type	STRING	Distributor_Type
Retailer_Master	Distributor_City	STRING	Distributor_City
Retailer_Master	Distributor_State	STRING	Distributor_State
Retailer_Master	Retailer_Code	STRING	Code representing a retailer
Retailer_Master	Retailer_Name	STRING	Retailer Name
Retailer_Master	SalesMan_Code	STRING	Sales representative code
Retailer_Master	SalesMan_Name	STRING	Sales representative name
Retailer_Master	Saleman_Role	STRING	Role or designation for Salesman
Retailer_Master	Outlet_type	STRING	Outlet Type
Retailer_Master	Warehouse_id	STRING	Warehouse ID
Scheme_Details	Scheme name	STRING	Name of the promotional scheme
Scheme_Details	Discount_Percent	FLOAT	Tdiscount offered in the scheme
Scheme_Details	is_active	STRING	Indicates if the scheme is currently active
Scheme_Details	scheme_type	STRING	Type of promotional scheme
Scheme_Details	apply_type	STRING	How the scheme is applied
Scheme_Details	SKU_Code	STRING	Unique identifier for the product (SKU)
Scheme_Details	start_date_time	DATETIME	Scheme start date and time
Scheme_Details	end_date_time	DATETIME	Scheme end date and time
Scheme_Details	scheme_group_id	STRING	Scheme Group ID
Scheme_Details	Level_1	STRING	Level 1
Scheme_Details	Level_2	STRING	Level 2

"""
