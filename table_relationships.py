# table_relationships.py
def describe_table_relationships(query: str = "") -> str:
   """Provides comprehensive relationship information between tables based on the schema."""
   relationships = """
🔗 **DATABASE RELATIONSHIPS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🗂️ **PRIMARY RELATIONSHIPS:**

### 1. **Scheme ↔ Product**
- `scheme_details.sku_code` → `product_master.sku_code` (Many-to-One)

### 2. **Retailer Order ↔ Retailer**
- `retailer_order_summary.retailer_code` → `retailer_master.retailer_code` (Many-to-One)

### 3. **Retailer Order Product ↔ Product**
- `retailer_order_product_details.sku_code` → `product_master.sku_code` (Many-to-One)

### 4. **Retailer Order Product ↔ Order Summary**
- `retailer_order_product_details.order_number` → `retailer_order_summary.order_number` (Many-to-One)

### 5. **Retailer Master ↔ Distributor**
- `retailer_master.distributor_code` → `distributor_closing_stock.distributor_code` (Many-to-One)

### 6. **Distributor_Closing_Stock ↔ Product**
- `distributor_closing_stock.sku_code` → `product_master.sku_code` (Many-to-One)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🔧 **COMMON JOIN PATTERNS:**

### 🎯 **Active Schemes with Product Info**

SELECT 
    s.scheme_name, s.discount_percent, s.scheme_type,
    p.sku_name, p.brand_name
FROM scheme_details s
JOIN product_master p ON s.sku_code = p.sku_code
WHERE s.is_active = 'Yes';


🛒 Retailer Orders with Product Details

SELECT 
    ros.order_number, ros.total_amount, 
    ropd.sku_name, ropd.order_quantity, ropd.price
FROM retailer_order_summary ros
JOIN retailer_order_product_details ropd 
    ON ros.order_number = ropd.order_number;


🧾 Retailer and Distributor Mapping
 
SELECT 
    rm.retailer_name, rm.retailer_code,
    rm.distributor_name, rm.distributor_code,
    rm.region_name, rm.distributor_state
FROM retailer_master rm;


📦 Closing Stock with Product Info 
SELECT 
    dcs.distributor_code, dcs.sku_code, dcs.quantity, 
    dcs.closing_stock_date, p.sku_name, p.brand_name
FROM distributor_closing_stock dcs
JOIN product_master p ON dcs.sku_code = p.sku_code;
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚠️ JOINING GUIDELINES
product_master is Central
Most SKU-level joins happen via sku_code

Key Field Mapping
sku_code: shared across product, scheme, order_product, closing_stock
order_number: shared across order_summary & order_product tables
retailer_code: used in order_summary ↔ retailer_master

Use JOIN Types Wisely
INNER JOIN: to match only valid records
LEFT JOIN: when all records from left table are needed

Performance Tip
Add WHERE filters on dates, status, region, etc., to keep queries efficient

💡 Best Practice: Only join when multiple tables are needed — avoid unnecessary joins for single-table queries.

"""
   return relationships