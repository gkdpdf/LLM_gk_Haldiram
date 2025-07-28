import os
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection string
engine = create_engine('postgresql://postgres:12345678@localhost:5432/LLM')

# Folder path to CSVs
csv_folder = 'cooked_data_gk'

# Map CSVs to their target table names
csv_table_map = {
    'retailer_order_product_details.csv': 'retailer_order_product_details',
    'retailer_order_summary.csv': 'retailer_order_summary',
    'retailer_master.csv': 'retailer_master',
    'product_master.csv': 'product_master',
    'Distributor_Closing_Stock.csv':'Distributor_Closing_Stock',
    'Scheme_Details.csv':'Scheme_Details'

}

# Load CSVs into PostgreSQL
for filename, table_name in csv_table_map.items():
    file_path = os.path.join(csv_folder, filename)
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"✅ Loaded {filename} into table '{table_name}'")

# Run the SQL query
query = """
SELECT p.SKU_Name, SUM(d.Order_Quantity) AS Total_Sales
FROM retailer_order_product_details d
JOIN retailer_order_summary s ON d.order_number = s.order_number
JOIN retailer_master r ON s.Retailer_code = r.Retailer_Code
JOIN product_master p ON d.SKU_Code = p.SKU_Code
WHERE r.Distributor_City = 'Mumbai'
GROUP BY p.SKU_Name
ORDER BY Total_Sales DESC
LIMIT 1;
"""

# Execute and show result
result_df = pd.read_sql_query(query, engine)
print("\n🏆 Top-selling SKU in Mumbai:")
print(result_df)
