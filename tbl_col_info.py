# tbl_col_info.py

def table_info_and_examples(query: str = "") -> str:
    """
    Returns full schema and example SQL queries for a LangChain SQL agent.
    """
    return """
📊 **DATABASE SCHEMA & EXAMPLES**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🗃️ **TABLE STRUCTURE:**

### 1. **tbl_scheme** (Discount Schemes)
- `sku_id`: SKU linked to the scheme
- `user_id`: Distributor who created the scheme  
- `is_active`: Boolean flag (1 = active, 0 = inactive)
- `discount_percent`: Percentage discount (FLOAT)
- `name`: Scheme name

### 2. **tbl_sales** (Sales Records)
- `order_id`: ID of the order
- `skunit_id`: SKU sold

### 3. **orders** (Order Details)
- `user_id`: Retailer ID who placed the order
- `order_number`: Order identifier
- `amount`: Order amount/value
- `created_at`: Order creation datetime

### 4. **order_items** (Order Line Items)
- `product_id`: Product in the order
- `order_id`: Related order ID

### 5. **tbl_product_master** (Product Catalog)
- `SKU_ID`: SKU code (Primary Key)
- `product_id`: Product code
- `Brand_Name`: Brand name
- `MRP`: Maximum Retail Price

### 6. **tbl_distributor_details** (Distributor Info)
- `User_ID`: Distributor ID (Primary Key)
- `Distributor_Code`: Distributor reference code

### 7. **sub_d_orders** (Sub-Distributor Orders)
- `distributor_id`: Sub-distributor placing the order
- `order_id`: Order ID

### 8. **sub_d_order_items** (Sub-Distributor Order Items)
- `product_id`: Product ordered
- `order_id`: Corresponding order ID

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 📌 **COMMON SQL QUERIES:**

### 🟢 **SCHEME QUERIES:**
```sql
-- Currently running schemes
SELECT name FROM tbl_scheme WHERE is_active = 1;

-- Top 5 schemes with highest discount
SELECT name, discount_percent 
FROM tbl_scheme 
ORDER BY discount_percent DESC 
LIMIT 5;

-- All schemes for a specific distributor
SELECT name, discount_percent, is_active
FROM tbl_scheme 
WHERE user_id = 'DISTRIBUTOR_ID';

-- Count of active vs inactive schemes
SELECT is_active, COUNT(*) as scheme_count
FROM tbl_scheme 
GROUP BY is_active;
```

### 📦 **ORDER QUERIES:**
```sql
-- Last order
SELECT order_number, amount 
FROM orders 
ORDER BY created_at DESC 
LIMIT 1;

-- Last 3 orders
SELECT order_number, amount, created_at
FROM orders 
ORDER BY created_at DESC 
LIMIT 3;x

-- Orders by a specific user
SELECT order_number, amount, created_at
FROM orders 
WHERE user_id = 'USER_ID'
ORDER BY created_at DESC;

-- Total orders count
SELECT COUNT(DISTINCT order_number) as total_orders 
FROM orders;
```

### 💰 **PRICING QUERIES:**
```sql
-- CRITICAL: For product price queries, ALWAYS search by Brand_Name first!
-- Step 1: Check if it exists as a brand name (case-insensitive)
SELECT DISTINCT Brand_Name 
FROM tbl_product_master 
WHERE LOWER(Brand_Name) LIKE LOWER('%PRODUCT_NAME%');

-- Step 2: If found as brand, show all SKUs and prices for that brand
SELECT SKU_ID, MRP
FROM tbl_product_master 
WHERE LOWER(Brand_Name) LIKE LOWER('%BRAND_NAME%');

-- Step 3: If not found as brand, check if it's an SKU
SELECT SKU_ID, MRP 
FROM tbl_product_master 
WHERE LOWER(SKU_ID) LIKE LOWER('%SKU_CODE%');

-- Step 4: If nothing found, show all available brands for user guidance
SELECT DISTINCT Brand_Name 
FROM tbl_product_master 
ORDER BY Brand_Name;

-- Examples:
-- User asks "price of crocin" -> search Brand_Name like '%crocin%' first
-- User asks "price of paracetamol" -> search Brand_Name like '%paracetamol%' first

-- Price range for a brand
SELECT Brand_Name, MIN(MRP) as min_price, MAX(MRP) as max_price
FROM tbl_product_master 
WHERE LOWER(Brand_Name) LIKE LOWER('%BRAND_NAME%')
GROUP BY Brand_Name;

-- All products with prices
SELECT SKU_ID, Brand_Name, MRP
FROM tbl_product_master 
ORDER BY MRP DESC;
```

### 🏷️ **SKU & BRAND QUERIES:**
```sql
-- All SKUs for a specific brand
SELECT SKU_ID, MRP
FROM tbl_product_master 
WHERE Brand_Name = 'BRAND_NAME';

-- All available brands
SELECT DISTINCT Brand_Name 
FROM tbl_product_master 
ORDER BY Brand_Name;

-- SKU count per brand
SELECT Brand_Name, COUNT(*) as sku_count
FROM tbl_product_master 
GROUP BY Brand_Name
ORDER BY sku_count DESC;

-- Check if brand exists
SELECT COUNT(*) 
FROM tbl_product_master 
WHERE Brand_Name = 'BRAND_NAME';
```

### 📊 **SALES ANALYSIS:**
```sql
-- Product-wise sales count
SELECT p.SKU_ID, p.Brand_Name, COUNT(*) as sales_count
FROM tbl_sales s
JOIN tbl_product_master p ON s.skunit_id = p.SKU_ID
GROUP BY p.SKU_ID, p.Brand_Name
ORDER BY sales_count DESC;

-- Sales by brand
SELECT p.Brand_Name, COUNT(*) as total_sales
FROM tbl_sales s
JOIN tbl_product_master p ON s.skunit_id = p.SKU_ID
GROUP BY p.Brand_Name
ORDER BY total_sales DESC;
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## ⚠️ **IMPORTANT NOTES:**

1. **CRITICAL FOR PRICE QUERIES**: When user asks "price of X", ALWAYS search Brand_Name first using LIKE with wildcards
2. **Search Pattern**: Use `LOWER(Brand_Name) LIKE LOWER('%search_term%')` for case-insensitive partial matching
3. **Fallback Strategy**: Brand_Name first → SKU_ID second → Show available brands if nothing found
4. **Use proper column names**: `MRP` not `Price`, `SKU_ID` not `sku_id`
5. **Boolean fields**: `is_active` uses 1 (true) and 0 (false)
6. **Join relationships**: Use appropriate JOINs when crossing tables
7. **Case sensitivity**: Always use LOWER() for text comparisons
8. **Date ordering**: Use `ORDER BY created_at DESC` for recent orders

## 🔍 **VALIDATION PATTERNS:**

**CRITICAL: For any product price query, use this exact sequence:**

```sql
-- Step 1: Always check Brand_Name first (case-insensitive partial match)
SELECT DISTINCT Brand_Name 
FROM tbl_product_master 
WHERE LOWER(Brand_Name) LIKE LOWER('%USER_INPUT%');

-- Step 2: If brand found, get all products for that brand
SELECT SKU_ID, MRP 
FROM tbl_product_master 
WHERE LOWER(Brand_Name) LIKE LOWER('%FOUND_BRAND%');

-- Step 3: If no brand found, check SKU_ID
SELECT SKU_ID, MRP 
FROM tbl_product_master 
WHERE LOWER(SKU_ID) LIKE LOWER('%USER_INPUT%');

-- Step 4: If nothing found, show available brands for guidance
SELECT DISTINCT Brand_Name 
FROM tbl_product_master 
ORDER BY Brand_Name;
```

**Examples:**
- "price of crocin" → Search Brand_Name LIKE '%crocin%' first
- "cost of paracetamol" → Search Brand_Name LIKE '%paracetamol%' first  
- "mrp of aspirin" → Search Brand_Name LIKE '%aspirin%' first

💡 **Tip**: Always use appropriate LIMIT clauses and ORDER BY for better performance and user experience.
"""