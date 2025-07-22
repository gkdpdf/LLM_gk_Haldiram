# table_relationships.py

def describe_table_relationships(query: str = "") -> str:
    """Provides comprehensive relationship information between tables based on the schema."""
    relationships = """
🔗 **DATABASE RELATIONSHIPS**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🗂️ **PRIMARY RELATIONSHIPS:**

### 1. **Scheme-Product Relationship**
   `tbl_scheme.sku_id` → `tbl_product_master.SKU_ID` (Many-to-One)
   
### 2. **Scheme-Distributor Relationship** 
   `tbl_scheme.user_id` → `tbl_distributor_details.User_ID` (Many-to-One)
   
### 3. **Sales-Product Relationship**
   `tbl_sales.skunit_id` → `tbl_product_master.SKU_ID` (Many-to-One)
   
### 4. **Orders-Distributor Relationship**
   `orders.user_id` → `tbl_distributor_details.User_ID` (Many-to-One)
   
### 5. **Order Items-Product Relationship**
   `order_items.product_id` → `tbl_product_master.product_id` (Many-to-One)
   `order_items.order_id` → `orders.order_number` (Many-to-One)
   
### 6. **Sub-Distributor Relationships**
   `sub_d_orders.distributor_id` → `tbl_distributor_details.Distributor_Code` (Many-to-One)
   `sub_d_order_items.product_id` → `tbl_product_master.product_id` (Many-to-One)
   `sub_d_order_items.order_id` → `sub_d_orders.order_id` (Many-to-One)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## 🔧 **COMMON JOIN PATTERNS:**

### **Scheme Analysis:**
```sql
-- Schemes with product details
SELECT s.name, s.discount_percent, p.Brand_Name, p.SKU_ID
FROM tbl_scheme s
JOIN tbl_product_master p ON s.sku_id = p.SKU_ID
WHERE s.is_active = 1;
```

### **Sales Analysis:**
```sql
-- Sales with product information
SELECT s.order_id, p.SKU_ID, p.Brand_Name, p.MRP
FROM tbl_sales s
JOIN tbl_product_master p ON s.skunit_id = p.SKU_ID;
```

### **Order Analysis:**
```sql
-- Orders with product details
SELECT o.order_number, o.amount, p.SKU_ID, p.Brand_Name
FROM orders o
JOIN order_items oi ON o.order_number = oi.order_id
JOIN tbl_product_master p ON oi.product_id = p.product_id;
```

### **Distributor Analysis:**
```sql
-- Distributor schemes
SELECT d.Distributor_Code, s.name, s.discount_percent
FROM tbl_distributor_details d
JOIN tbl_scheme s ON d.User_ID = s.user_id
WHERE s.is_active = 1;
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## ⚠️ **JOIN GUIDELINES:**

1. **Product Master is Central**: Most analysis requires joining with `tbl_product_master`
2. **Key Field Mapping**: 
   - `SKU_ID` in product_master ↔ `sku_id` in scheme ↔ `skunit_id` in sales
   - `product_id` in product_master ↔ `product_id` in order_items
3. **Use Appropriate JOIN Types**: 
   - INNER JOIN for required relationships
   - LEFT JOIN when you want to keep all records from left table
4. **Performance**: Always include WHERE clauses to limit results when possible

💡 **Remember**: Use JOINs only when you need data from multiple tables. For single-table queries, avoid unnecessary JOINs.
"""
    return relationships