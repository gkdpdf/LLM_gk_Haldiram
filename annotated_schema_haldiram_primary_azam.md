### **tbl_distributor_master**
```json
["This table maps the hierarchical relationships between superstockists and their associated distributors, detailing the sales management personnel involved at various levels, distributor categorization, and location specifics. It captures the multi-level sales hierarchy from Level 6 to Level 2, providing insights into the sales network structure and distributor operations.", 
[
    ["superstockist_name: Name of the superstockist entity supplying goods to distributors, represented as a string. <sample values: S B Markplus Private Limited-2, ...>"],
    ["level6_position_user: Name of the Level 6 manager responsible for the superstockist’s operations, represented as a string. <sample values: Vinayak Mathur, ...>"],
    ["level5_position_user: Name of the Level 5 manager in the hierarchy, represented as a string. <sample values: Manoj Kumar Gaur, ...>"],
    ["level4_position_user: Name of the Level 4 manager in the hierarchy, represented as a string. <sample values: Lokesh Baweja, ...>"],
    ["level3_position_user: Name of the Level 3 manager in the hierarchy, represented as a string. <sample values: Ashwani Mudgil, ...>"],
    ["level2_position_user: Name of the Level 2 manager in the hierarchy, represented as a string. <sample values: Mohammad Gaffar, ...>"],
    ["distributor_name: Distributor’s registered name with ERP code and location details, represented as a string. <sample values: SAWARIYA TRADING 41496, ...>"],
    ["distributor_erp_id: Unique ERP identifier for the distributor, represented as an integer. <sample values: 460307, ...>"],
    ["distributor_type: Classification of distributor, such as Sub Stockist, represented as a string. <sample values: Sub Stockist, ...>"],
    ["state: State of operation for the distributor, represented as a string. <sample values: DELHI, ...>"],
    ["distributor_segmentation: Market segmentation type, such as GT for General Trade, represented as a string. <sample values: GT, ...>"],
    ["distributor_channel: Sales channel assigned to distributor, such as GT or MT, represented as a string. <sample values: GT, ...>"],
    ["city_of_warehouse_address: City where the distributor’s warehouse is located, represented as a string. <sample values: WEST DELHI WEST DELHI, ...>"],
    ["temp_created_date: Date when the record was created in the system, represented as a date. <sample values: 30/03/25, ...>"]
]]
```


### **tbl_Primary**
```json
["This table captures transactional sales data between superstockists and their associated distributors, including product-level details, order quantities, billing, and invoicing information. It provides full linkage from the superstockist hierarchy to distributor geography and sales channels, enabling analysis of sales performance, product demand, and distribution coverage. It is essential for tracking order flow from superstockists to distributors, analyzing fulfillment gaps (short closes), and reconciling sales orders with invoiced quantities.", 
[
["super_stockist_id : Unique identifier for the superstockist entity, integer. <sample values: 19000102>"],
["super_stockist_name : Name of the superstockist supplying products to distributors, string. <sample values: 'S B Markplus Private Limited-2'>"],
["super_stockist_zone : Zone assigned to the superstockist, string. <sample values: 'NORTH'>"],
["super_stockist_region : Region assigned to the superstockist, string. <sample values: 'DELHI'>"],
["super_stockist_state : State where the superstockist operates, string. <sample values: 'DELHI'>"],
["distributor_id : Unique identifier for the distributor, integer. <sample values: 460124, 460307>"],
["distributor_name : Registered name of the distributor with ERP code, string. <sample values: 'V H TRADING COMPANY 41303 -DELHI', 'SAWARIYA TRADING 41496'>"],
["distributor_zone : Zone assigned to the distributor, string. <sample values: 'NORTH'>"],
["distributor_region : Region assigned to the distributor, string. <sample values: 'DELHI'>"],
["distributor_state : State where the distributor operates, string. <sample values: 'DELHI'>"],
["channel_type : Distribution channel type, string. <sample values: 'GT'>"],
["product_id : Unique identifier for the product (SKU code), string. <sample values: 'FD048300200720000D', 'FI002000550396001D'>"],
["product_name : Name and description of the product, string. <sample values: 'Lite Mixture MRP 5|20GM*7.2KG', 'Chips Masala MRP 20|55 GM*3.96 KG'>"],
["ordered_quantity : Total quantity of product ordered by the distributor, integer. <sample values: 7200, 0>"],
["short_close_qty : Quantity not fulfilled (short closed) from the original order, integer. <sample values: 0>"],
["sales_order_date : Date when the sales order was placed, date. <sample values: '05/10/25', '04/10/25'>"],
["bill_date : Date when the order was billed, date. <sample values: '2025-05-10', '2025-06-27'>"],
["invoiced_total_quantity : Quantity invoiced to the distributor, integer. <sample values: 7200, 72>"]
]
]
```


### **tbl_Product_Master**
```json
["The table contains detailed metadata for products, including classifications, pricing, and identifiers, essential for consistent product information across systems.", 
[
    ["industry_segment_name: High-level classification of the industry segment, typically indicating the broader category of the product. Datatype: String, Sample values: CAR, Beverages"],
    ["pack_size_name: Standardized grouping of the product's pack size, indicating the quantity or volume. Datatype: String, Sample values: 100 GM, LARGE PACK"],
    ["base_pack_design_name: Descriptive name of the product's base pack design, detailing the packaging style and size. Datatype: String, Sample values: Masala Jeera Powder-100-Gm, Dodha 500Gm"],
    ["base_pack_design_id: Unique identifier assigned to each base pack design for tracking and reference. Datatype: String, Sample values: EGM7B443317B72319, DHH6A260272E51957"],
    ["industry_segment_id: Unique identifier for the industry segment, used for categorization and analysis. Datatype: String, Sample values: Spices_Emerging, Sweets_Ambient"],
    ["pack_size_id: Unique identifier for the pack size, combining size and segment information. Datatype: String, Sample values: 100 GM_Spices_Emerging, LARGE PACK_Sweets_Ambient"],
    ["product: Full name of the product, including brand, variant, size, and packaging details. Datatype: String, Sample values: Masala Jeera Powder 100 GM*12 KG, Dodha Large Pack|500GM*12KG"],
    ["ptr: Price-to-retailer value, indicating the cost at which the retailer purchases the product. Datatype: Float, Sample values: 49.5238, 346.32"],
    ["ptd: Price-to-distributor value, indicating the cost at which the distributor purchases the product. Datatype: Float, Sample values: 45.85, 329.82"],
    ["display_mrp: Displayed Maximum Retail Price, the price shown to consumers. Datatype: Integer, Sample values: 65, 400"],
    ["mrp: Standard Maximum Retail Price, the official retail price of the product. Datatype: Integer, Sample values: 65, 400"],
    ["alternate_category: Alternate classification of the product, providing additional categorization. Datatype: String, Sample values: Masala - CP, Milk Based"],
    ["product_erp_id: ERP system identifier for the product SKU, used for system integration and tracking. Datatype: String, Sample values: FG331701001200000D, FH027205001200000D"],
    ["is_promoted: Boolean flag indicating if the product is currently under promotion. Datatype: Boolean, Sample values: False"],
    ["product_weight_in_gm: Weight of the product in grams, providing additional product specification. Datatype: Integer, Sample values: 0"]
]]
```


### **tbl_superstockist_master**
```json
["This table maintains a master list of all superstockists in the distribution network, serving as a reference for mapping superstockist IDs to their corresponding names. It is used for joining transactional, mapping, and sales data to the correct superstockist entity, ensuring consistency in identifying superstockists across multiple datasets.", 
[
["superstockist_name : Official registered name of the superstockist entity, datatype: string, sample values: 'S B Markplus Private Limited-3', 'Kansal Estate Private Limited' etc."], 
["superstockist_id : Unique numeric identifier assigned to the superstockist, datatype: integer, sample values: 19000740, 19000149 etc."]
]
]
```


### **tbl_shipment**
```json
["This table captures primary sales transactions from manufacturing plants to superstockists, representing the initial stage of the supply chain. It includes details about the supplying plant, sales district, customer account, product codes and descriptions, quantities billed, and invoice dates. This data is crucial for reconciling production dispatches with downstream sales flows, analyzing plant-level supply patterns, and validating the movement of goods into the superstockist network.", 
[
    ["supplying_plant: Code of the manufacturing or dispatching plant, represented as a string. <sample values: HM55, HM54>"],
    ["sales_district: Geographic sales district associated with the supply, represented as a string. <sample values: NORTH>"],
    ["sold_to_party: ERP customer code for the superstockist, represented as an integer. <sample values: 19000102, 19000740>"],
    ["sold_to_party_name: Name of the superstockist entity, represented as a string. <sample values: S B Markplus Private Limited>"],
    ["city: City of the superstockist or billing location, represented as a string. <sample values: Delhi, New Delhi>"],
    ["material: ERP material code of the product, represented as a string. <sample values: FE077200341020000D, FD075200400960000D>"],
    ["material_description: Full description of the product including brand, variant, pack size, and weight, represented as a string. <sample values: PeanutRoasted(Crushed)MRP10|34GM*10.2KG, Punjabi Tadka MRP 10|40GM*9.6KG>"],
    ["actual_billed_quantity: Quantity invoiced from plant to superstockist, represented as an integer. <sample values: 20700, 240>"],
    ["invoice_date: Date of billing/invoicing for the dispatched product, represented in the format YY/MM/DD. <sample values: 18/05/25, 05/05/25>"]
]
]
```
