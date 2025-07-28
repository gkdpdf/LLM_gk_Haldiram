def table_info_and_examples(query: str = "") -> str:
    """
    Returns full schema and example SQL queries for a LangChain SQL agent.
    """
    return """
📊 **DATABASE SCHEMA & EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🗃️ **TABLE STRUCTURES**

---

### **1. distributor_closing_stock**
| Column Name              | Data Type | Description                                 |
|--------------------------|-----------|---------------------------------------------|
| sku_code                 | STRING    | Unique identifier for the product (SKU)     |
| sku_name                 | STRING    | Name of the product (SKU)                   |
| sku_short_description    | STRING    | Short name or description of the product    |
| brand_name               | STRING    | Name of the product brand                   |
| brand_code               | STRING    | Code representing the brand                 |
| distributor_code         | STRING    | Code for the distributor                    |
| quantity                 | FLOAT     | Quantity of product present with distributor|
| closing_stock_date       | DATETIME  | Date when stock count was recorded          |

---

### **2. retailer_order_summary**
| Column Name      | Data Type | Description                          |
|------------------|-----------|--------------------------------------|
| retailer_code    | STRING    | Code representing a retailer         |
| order_number     | STRING    | Unique number assigned to an order   |
| shipping_details | STRING    | Information related to delivery      |
| city_code        | STRING    | Code or name of the city             |
| subtotal         | FLOAT     | Partial total before discounts       |
| total_discount   | FLOAT     | Total discount on the order          |
| shipping_charge  | FLOAT     | Shipping charges on the order        |
| total_amount     | FLOAT     | Total amount after all charges       |
| order_status     | INT       | Status of the order                  |
| created_at       | DATETIME  | Date and time when order was created |
| updated_at       | DATETIME  | Date and time when order was updated |

---

### **3. retailer_order_product_details**
| Column Name       | Data Type | Description                                |
|-------------------|-----------|--------------------------------------------|
| order_number      | INT       | Unique number assigned to an order         |
| sku_code          | INT       | Unique identifier for the product (SKU)    |
| sku_name          | STRING    | Name of the product (SKU)                  |
| order_quantity    | FLOAT     | Item quantity ordered by retailer          |
| price             | FLOAT     | Price of a single unit                     |
| subtotal          | FLOAT     | Partial total before discounts and charges |
| created_at        | DATETIME  | Date and time when record was created      |

---

### **4. product_master**
| Column Name              | Data Type | Description                                  |
|--------------------------|-----------|----------------------------------------------|
| segment_code             | STRING    | Code representing product category           |
| segment_name_category    | STRING    | Category or classification name              |
| brand_code               | STRING    | Code representing the brand                  |
| brand_name               | STRING    | Name of the product brand                    |
| parent_code              | STRING    | Code for the parent product                  |
| parent_name              | STRING    | Name of the parent product                   |
| sku_code                 | STRING    | Unique identifier for the product (SKU)      |
| sku_name                 | STRING    | Name of the product (SKU)                    |
| sku_short_description    | STRING    | Short description of the product             |
| mrp                      | FLOAT     | Maximum retail price of the product          |
| active                   | STRING    | Product active status                        |
| price_to_retailer        | FLOAT     | Price at which retailer buys the product     |

---

### **5. retailer_master**
| Column Name              | Data Type | Description                                  |
|--------------------------|-----------|----------------------------------------------|
| region_code              | STRING    | Geographical region code                     |
| region_name              | STRING    | Geographical region name                     |
| rsm_code                 | STRING    | Regional Sales Manager code                  |
| rsm_area                 | STRING    | Regional Sales Manager name                  |
| so                       | STRING    | Sales Officer code                           |
| distributor_code         | STRING    | Code for the distributor                     |
| distributor_name         | STRING    | Name of the distributor                      |
| distributor_channel_type | STRING    | Type of distribution channel                 |
| distributor_type         | STRING    | Type of distributor                          |
| distributor_city         | STRING    | City of the distributor                      |
| distributor_state        | STRING    | State of the distributor                     |
| retailer_code            | STRING    | Code representing the retailer               |
| retailer_name            | STRING    | Name of the retailer                         |
| salesman_code            | STRING    | Sales representative code                    |
| salesman_name            | STRING    | Sales representative name                    |
| saleman_role             | STRING    | Role or designation of the salesman          |
| outlet_type              | STRING    | Type of outlet                               |
| warehouse_id             | STRING    | ID of the warehouse                          |

---

### **6. scheme_details**
| Column Name         | Data Type | Description                                   |
|---------------------|-----------|-----------------------------------------------|
| scheme_name         | STRING    | Name of the promotional scheme                |
| discount_percent    | FLOAT     | Discount offered in the scheme                |
| is_active           | STRING    | Indicates if the scheme is active             |
| scheme_type         | STRING    | Type of the promotional scheme                |
| apply_type          | STRING    | How the scheme is applied                     |
| sku_code            | STRING    | SKU to which the scheme is applied            |
| start_date_time     | DATETIME  | Start date and time of the scheme             |
| end_date_time       | DATETIME  | End date and time of the scheme               |
| scheme_group_id     | STRING    | Identifier for the scheme group               |
| level_1             | STRING    | Level 1 metadata                              |
| level_2             | STRING    | Level 2 metadata                              |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🧠 **EXAMPLE SQL QUERIES**

---

### 1. 🔍 Get Current Stock for Each SKU by Distributor


SELECT
    distributor_code,
    sku_code,
    sku_name,
    SUM(quantity) AS total_stock,
    MAX(closing_stock_date) AS latest_stock_date
FROM distributor_closing_stock
GROUP BY distributor_code, sku_code, sku_name;

2. 🧾 List Top 10 Orders by Value (after discount)

SELECT
    order_number,
    retailer_code,
    total_amount,
    total_discount,
    shipping_charge,
    created_at
FROM retailer_order_summary
ORDER BY total_amount DESC
LIMIT 10;


3. 📦 Order Details for a Specific Retailer

SELECT
    ros.order_number,
    ropd.sku_name,
    ropd.order_quantity,
    ropd.price,
    ropd.subtotal
FROM retailer_order_summary ros
JOIN retailer_order_product_details ropd
    ON ros.order_number = ropd.order_number
WHERE ros.retailer_code = 'rt12345';


4. 💰 Product MRP vs Price to Retailer

SELECT
    sku_code,
    sku_name,
    mrp,
    price_to_retailer,
    (mrp - price_to_retailer)*100/price_to_retailer AS margin
FROM product_master
WHERE active = 'Yes';

5. 🎯 Active Schemes Running Today

SELECT
    scheme_name,
    sku_code,
    discount_percent,
    scheme_type,
    start_date_time,
    end_date_time
FROM scheme_details
WHERE is_active = 'Yes'
  AND CURRENT_TIMESTAMP BETWEEN start_date_time AND end_date_time;

6. 🗺️ Retailer & Distributor Info in a Region

SELECT
    rm.retailer_name,
    rm.distributor_name,
    rm.region_name,
    rm.distributor_state,
    rm.distributor_city
FROM retailer_master rm
WHERE rm.region_name = 'East';
"""