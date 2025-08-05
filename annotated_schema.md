### **distributor_closing_stock**
```json
["This table contains information about the stock levels of various products held by different distributors as of a specific date. It includes details about the product, its brand, the distributor, the quantity in stock, and the date the stock was recorded.", 
[
["sku_code : A unique identifier for each product, represented as a string. <sample values: SENSODYNEF, CENTRUMF>"],
["sku_name : The name of the product, represented as a string. <sample values: Sensodyne Fresh, Centrum Forte>"],
["sku_short_description : A brief description of the product, represented as a string. <sample values: Sensitivity Toothpaste, Adult Multivitamin>"],
["brand_name : The name of the brand associated with the product, represented as a string. <sample values: Sensodyne, Centrum>"],
["brand_code : A unique code representing the brand, represented as a string. <sample values: B_SENS, B_CENTRUM>"],
["distributor_code : A unique code identifying the distributor, represented as a string. <sample values: DIS004, DIS003>"],
["quantity : The number of units of the product available in stock, represented as an integer. <sample values: 200, 75>"],
["closing_stock_date : The date and time when the stock count was recorded, represented as a datetime. <sample values: 08/06/24 12:00, 09/06/24 16:40>"]
]
]
```


### **retailer_order_summary**
```json
["This table contains summary-level information about each retailer's order, including identification, delivery details, cost breakdown, order status, and timestamps for creation and updates.", 
[
["retailer_code : A unique identifier for each retailer, represented as a string. <sample values: RTL003, RTL001>"],
["order_number : A unique identifier for each order, represented as a string. <sample values: ORD1003, ORD1001>"],
["shipping_details : The address where the order is to be delivered, represented as a string. <sample values: 15 Brigade Rd, Bangalore, 12 MG Road, Mumbai>"],
["city_code : A code representing the city of delivery, represented as a string. <sample values: BLR003, MUM001>"],
["subtotal : The total cost of items before discounts and charges, represented as an integer. <sample values: 1520, 600>"],
["total_discount : The total discount applied to the order, represented as an integer. <sample values: 100, 30>"],
["shipping_charge : The cost of shipping the order, represented as an integer. <sample values: 0, 20>"],
["total_amount : The final amount to be paid after discounts and charges, represented as an integer. <sample values: 1420, 590>"],
["order_status : A numeric code representing the current status of the order, represented as an integer. <sample values: 3, 1>"],
["created_at : The timestamp when the order was created, represented as a datetime. <sample values: 08/06/24 14:15, 10/06/24 10:01>"],
["updated_at : The timestamp when the order was last updated, represented as a datetime. <sample values: 08/06/24 17:25, 10/06/24 10:12>"]
]
]
```


### **retailer_order_product_details**
```json
["This table contains detailed item-level information for each order placed by a retailer, including product identification, quantity, pricing, and order logging details.", 
[
["order_number : Unique identifier for each order placed by a retailer, datatype: string, sample values: ORD1001, ORD1003"],
["sku_code : Unique code representing each product in the order, datatype: string, sample values: ENO001, CENTRUMF"],
["sku_name : Descriptive name of the product ordered, datatype: string, sample values: ENO Lemon, Centrum Forte"],
["order_quantity : Number of units of the product ordered, datatype: integer, sample values: 10, 20"],
["price : Cost per unit of the product, datatype: integer, sample values: 25, 76"],
["subtotal : Total cost for the ordered quantity of the product, calculated as order_quantity multiplied by price, datatype: integer, sample values: 250, 1520"],
["created_at : Timestamp indicating when the order record was logged, datatype: datetime, sample values: 10/06/24 10:03, 08/06/24 14:16"]
]
]
```


### **product_master**
```json
["This table contains master-level information about products, including their hierarchy, segmentation, brand details, and pricing. It provides insights into product categories, brand associations, and pricing structures relevant to the supply chain.", 
[
["segment_code : A code representing the product segment, typically indicating the category of the product. It is a string datatype. <sample values: SEG04, SEG01>"], 
["segment_name_category : The name of the product category associated with the segment code. It is a string datatype. <sample values: Toothpaste, Antacid>"], 
["brand_code : A unique code identifying the brand of the product. It is a string datatype. <sample values: B_SENS, B_ENO>"], 
["brand_name : The name of the brand associated with the product. It is a string datatype. <sample values: Sensodyne, ENO>"], 
["parent_code : A code representing the parent category or group to which the product belongs. It is a string datatype. <sample values: PR_SENS, PR_ENO>"], 
["parent_name : The name of the parent category or group associated with the product. It is a string datatype. <sample values: Sensitivity Toothpaste, ENO Sachet>"], 
["sku_code : A unique stock keeping unit code identifying the specific product. It is a string datatype. <sample values: SENSODYNEF, ENO001>"], 
["sku_name : The name of the product as identified by the SKU code. It is a string datatype. <sample values: Sensodyne Fresh, ENO Lemon>"], 
["sku_short_description : A brief description of the product, providing essential details. It is a string datatype. <sample values: Sensitivity Toothpaste, Antacid sachet ENO Lemon>"], 
["mrp : The maximum retail price of the product, indicating the highest price at which it can be sold. It is an integer datatype. <sample values: 88, 32>"], 
["active : A flag indicating whether the product is currently active and available for sale. It is a string datatype, typically 'Y' for active. <sample values: Y>"], 
["price_to_retailer : The price at which the retailer purchases the product, often lower than the MRP. It is an integer datatype. <sample values: 70, 25>"]
]
]
```


### **retailer_master**
```json
["This table contains master information about retailers and their associations, including regional and sales hierarchy, retailer identification, distributor linkage, channel information, location, and logistics warehouse connections.", 
[
["region_code : Code representing the region, used for regional hierarchy, datatype: VARCHAR, sample values: RG01, RG02"],
["region_name : Name of the region, datatype: VARCHAR, sample values: West, East"],
["rsm_code : Code for the Regional Sales Manager, part of the sales hierarchy, datatype: VARCHAR, sample values: SM001, SM002"],
["rsm_area : Area under the Regional Sales Manager's jurisdiction, datatype: VARCHAR, sample values: Mumbai Urban, Kolkata Central"],
["so : Sales office code, part of the sales hierarchy, datatype: VARCHAR, sample values: SO001, SO002"],
["distributor_code : Unique code identifying the distributor, linking retailers to distributors, datatype: VARCHAR, sample values: DIS001, DIS002"],
["distributor_name : Name of the distributor, datatype: VARCHAR, sample values: AP Distributors, Raj Pharma"],
["distributor_channel_type : Type of channel through which the distributor operates, datatype: VARCHAR, sample values: General Trade, Distributor"],
["distributor_type : Type of distributor, indicating the scale or scope, datatype: VARCHAR, sample values: City, Metro"],
["distributor_city : City where the distributor is located, datatype: VARCHAR, sample values: Mumbai, Kolkata"],
["distributor_state : State where the distributor is located, datatype: VARCHAR, sample values: Maharashtra, West Bengal"],
["retailer_code : Unique code identifying the retailer, datatype: VARCHAR, sample values: RTL001, RTL002"],
["retailer_name : Name of the retailer, datatype: VARCHAR, sample values: Harsha Medicals, Healthwell"],
["salesman_code : Code for the salesman associated with the retailer, datatype: VARCHAR, sample values: SL001, SL002"],
["salesman_name : Name of the salesman, datatype: VARCHAR, sample values: Ravi Kumar, Priya Dutta"],
["saleman_role : Role of the salesman in the sales hierarchy, datatype: VARCHAR, sample values: Field, Territory Manager"],
["outlet_type : Type of retail outlet, datatype: VARCHAR, sample values: Pharmacy, Chemist"],
["warehouse_id : Identifier for the logistics warehouse linked to the retailer, datatype: VARCHAR, sample values: WH1, WH2"]
]
]
```


### **scheme_details**
```json
["This table contains details about promotional schemes applied to various products, including their names, discount percentages, activity status, types, application methods, associated product codes, validity periods, group identifiers, and segmentation metadata.", 
[
["scheme_name : Name of the promotional scheme, represented as a string. <sample values: Crocin Summer, Centrum Offer>"], 
["discount_percent : Percentage discount offered by the scheme, represented as an integer. <sample values: 15, 12>"], 
["is_active : Indicates if the scheme is currently active, represented as a string with 'Y' for active and 'N' for inactive. <sample values: Y, N>"], 
["scheme_type : Type of the scheme, such as 'Bulk' or 'Flat', represented as a string. <sample values: Bulk, Flat>"], 
["apply_type : Method of application for the scheme, such as 'Manual' or 'Auto', represented as a string. <sample values: Manual, Auto>"], 
["sku_code : Code of the product to which the scheme applies, represented as a string. <sample values: CROCIN500, CENTRUMF>"], 
["start_date_time : Start date and time of the scheme's validity, represented as a string in 'DD/MM/YY HH:MM' format. <sample values: 01/06/24 8:00, 07/06/24 9:00>"], 
["end_date_time : End date and time of the scheme's validity, represented as a string in 'DD/MM/YY HH:MM' format. <sample values: 20/06/24 23:59, 21/06/24 17:00>"], 
["scheme_group_id : Identifier for the group to which the scheme belongs, represented as a string. <sample values: SG124, SG125>"], 
["level_1 : First level of metadata for scheme segmentation, represented as a string. <sample values: Category, Segment>"], 
["level_2 : Second level of metadata for scheme segmentation, represented as a string. <sample values: Brand, SKU>"]
]
]
```
