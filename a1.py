# Rex
import pandas as pd
import sqlite3

customers_df = pd.read_csv('customer.csv')
orders_df = pd.read_csv('orders.csv')

# Merge the orders and customers DataFrames based on the 'CustomerID' column using an inner join
merged_df = pd.merge(orders_df, customers_df, on='CustomerID', how='inner')

# Calculate the total amount for each order by multiplying the quantity and price
merged_df['TotalAmount'] = merged_df['Quantity'] * merged_df['Price']

merged_df['Status'] = merged_df['OrderDate'].apply(lambda d: 'New' if d.startswith('2024-03') else 'Old')

# Filter the merged DataFrame to get high-value orders where the total amount is greater than 4500
high_value_orders = merged_df[merged_df['TotalAmount'] > 4500]

# Establish a connection to an SQLite database named 'ecommerce.db'
conn = sqlite3.connect('ecommerce.db')

create_table_query = '''
CREATE TABLE IF NOT EXISTS HighValueOrders (
    OrderID INTEGER,
    CustomerID INTEGER,
    Name TEXT,
    Email TEXT,
    Product TEXT,
    Quantity INTEGER,
    Price REAL,
    OrderDate TEXT,
    TotalAmount REAL,
    Status TEXT
)
'''
# Execute the SQL query to create the table
conn.execute(create_table_query)

# Save the high-value orders DataFrame to the 'HighValueOrders' table in the SQLite database, replacing the table if it already exists
high_value_orders.to_sql('HighValueOrders', conn, if_exists='replace', index=False)

# Query all records from the 'HighValueOrders' table and print each row
result = conn.execute('SELECT * FROM HighValueOrders')
for row in result.fetchall():
    print(row)

conn.close()
print("ETL process completed successfully!")
