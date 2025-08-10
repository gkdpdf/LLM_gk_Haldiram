### **tbl_distributor_master**
```json
["This table stores hierarchical mapping between superstockists and their associated distributors, along with details of sales management personnel, distributor categorization, and location. It captures multi-level position holders in the sales hierarchy, providing traceability from top-level superstockist managers down to distributor operations. It is essential for understanding the sales network structure, identifying distributor coverage, and filtering by operational attributes such as channel, segmentation, and location.", 
[
["superstockist_name : Name of the superstockist entity supplying goods to distributors. Datatype: String, <sample values: S B Markplus Private Limited-2>"],
["level6_position_user : Level 6 manager responsible for the superstockist’s operations. Datatype: String, <sample values: Vinayak Mathur>"],
["level5_position_user : Level 5 manager in the hierarchy. Datatype: String, <sample values: Manoj Kumar Gaur>"],
["level4_position_user : Level 4 manager in the hierarchy. Datatype: String, <sample values: Lokesh Baweja>"],
["level3_position_user : Level 3 manager in the hierarchy. Datatype: String, <sample values: Ashwani Mudgil>"],
["level2_position_user : Level 2 manager in the hierarchy. Datatype: String, <sample values: Mohammad Gaffar>"],
["distributor_name : Distributor’s registered name with ERP code and location details. Datatype: String, <sample values: SAWARIYA TRADING 41496, V H TRADING COMPANY 41303 -DELHI>"],
["distributor_erp_id : Unique ERP identifier for the distributor. Datatype: Integer, <sample values: 460307, 460124>"],
["distributor_type : Classification of distributor (e.g., Sub Stockist). Datatype: String, <sample values: Sub Stockist>"],
["state : State of operation for the distributor. Datatype: String, <sample values: DELHI>"],
["distributor_segmentation : Market segmentation type (e.g., GT = General Trade). Datatype: String, <sample values: GT>"],
["distributor_channel : Sales channel assigned to distributor (e.g., GT, MT). Datatype: String, <sample values: GT>"],
["city_of_warehouse_address : City where the distributor’s warehouse is located. Datatype: String, <sample values: WEST DELHI WEST DELHI, DELHI>"],
["temp_created_date : Date when the record was created in the system. Datatype: Date, <sample values: 30/03/25>"]
]
]
```


### **tbl_Primary**
```json
["This table captures transactional sales data between superstockists and distributors, detailing product-level information, order quantities, billing, and invoicing. It links superstockist hierarchy to distributor geography and sales channels, essential for tracking order flow, analyzing fulfillment gaps, and reconciling sales orders with invoiced quantities.", 
[
["super_stockist_id : Unique identifier for the superstockist entity, integer, <sample values: 19000102>"],
["super_stockist_name : Name of the superstockist supplying products to distributors, string, <sample values: 'S B Markplus Private Limited-2'>"],
["super_stockist_zone : Zone assigned to the superstockist, string, <sample values: 'NORTH'>"],
["super_stockist_region : Region assigned to the superstockist, string, <sample values: 'DELHI'>"],
["super_stockist_state : State where the superstockist operates, string, <sample values: 'DELHI'>"],
["distributor_id : Unique identifier for the distributor, integer, <sample values: 460307, 460124>"],
["distributor_name : Registered name of the distributor with ERP code, string, <sample values: 'SAWARIYA TRADING 41496', 'V H TRADING COMPANY 41303 -DELHI'>"],
["distributor_zone : Zone assigned to the distributor, string, <sample values: 'NORTH'>"],
["distributor_region : Region assigned to the distributor, string, <sample values: 'DELHI'>"],
["distributor_state : State where the distributor operates, string, <sample values: 'DELHI'>"],
["channel_type : Distribution channel type, string, <sample values: 'GT'>"],
["product_id : Unique identifier for the product (SKU code), string, <sample values: 'FI220800450432000D', 'FE077200341020000D'>"],
["product_name : Name and description of the product, string, <sample values: 'Snaclite Soya Katori 45 GM*4.32 KG', 'PeanutRoasted(Crushed)MRP10|34GM*10.2KG'>"],
["ordered_quantity : Total quantity of product ordered by the distributor, integer, <sample values: 960, 1500>"],
["short_close_qty : Quantity not fulfilled (short closed) from the original order, integer, <sample values: 0>"],
["sales_order_date : Date when the sales order was placed, date, <sample values: '06/05/25'>"],
["bill_date : Date when the order was billed, date, <sample values: '21/06/25', '05/06/25'>"],
["invoiced_total_quantity : Quantity invoiced to the distributor, integer, <sample values: 960, 1500>"]
]
]
```


### **tbl_Product_Master**
```json
["This table contains detailed metadata for products, including classifications, pricing, and identifiers, essential for consistent product information across systems.", 
[
["industry_segment_name: High-level classification of the industry segment, typically indicating the product category, such as 'CAR' or 'Beverages'. Data type: VARCHAR. Sample values: CAR, Beverages."],
["pack_size_name: Standardized grouping of pack sizes, indicating the size or weight of the product, such as 'LARGE PACK' or '200 GM'. Data type: VARCHAR. Sample values: 100 GM, MRP 10."],
["base_pack_design_name: Descriptive name of the base pack design, providing details about the product's packaging. Data type: VARCHAR. Sample values: Masala Haldi Powder-100-Gm, All In One MRP 10."],
["base_pack_design_id: Unique identifier for the base pack design, used for tracking and reference. Data type: VARCHAR. Sample values: EGM7B443297B72305, BDM5A040126A10120."],
["industry_segment_id: Unique identifier for the industry segment, linking products to their respective categories. Data type: VARCHAR. Sample values: Spices_Emerging, Namkeen_Out of Home."],
["pack_size_id: Unique identifier for the pack size, combining size and segment information. Data type: VARCHAR. Sample values: 100 GM_Spices_Emerging, MRP 10_Namkeen_Out of Home."],
["product: Full product name including brand, variant, size, and packaging details, providing a comprehensive product description. Data type: VARCHAR. Sample values: Masala Haldi Powder 100 GM*18 KG, All In One MRP 10|36GM*9.072KG."],
["ptr: Price-to-retailer value, indicating the cost at which the product is sold to retailers. Data type: DECIMAL. Sample values: 26.6667, 7.5."],
["ptd: Price-to-distributor value, indicating the cost at which the product is sold to distributors. Data type: DECIMAL. Sample values: 24.69, 7.01."],
["display_mrp: Displayed Maximum Retail Price, the price shown to consumers. Data type: DECIMAL. Sample values: 35, 10."],
["mrp: Standard Maximum Retail Price, the official retail price of the product. Data type: DECIMAL. Sample values: 35, 10."],
["alternate_category: Alternate classification for the product, providing additional categorization. Data type: VARCHAR. Sample values: Masala - CP, Mix/Chivra/Chanachur."],
["product_erp_id: ERP system identifier for the product SKU, used for integration with enterprise systems. Data type: VARCHAR. Sample values: FG329701001800001D, FD012600360907200D."],
["is_promoted: Boolean flag indicating if the product is under promotion, used for marketing and sales strategies. Data type: BOOLEAN. Sample values: False."],
["product_weight_in_gm: Product weight in grams, providing the net weight of the product. Data type: INTEGER. Sample values: 0."]
]
]
```


### **tbl_superstockist_master**
```json
["This table maintains a master list of all superstockists in the distribution network, serving as a reference for mapping superstockist IDs to their corresponding names. It is used for joining transactional, mapping, and sales data to the correct superstockist entity, ensuring consistency in identifying superstockists across multiple datasets.", [["superstockist_name : Official registered name of the superstockist entity, datatype: string, <sample values: S B Markplus Private Limited-3, Kansal Estate Private Limited etc>"], ["superstockist_id : Unique numeric identifier assigned to the superstockist, datatype: integer, <sample values: 19000740, 19000149 etc>"]]]
```
