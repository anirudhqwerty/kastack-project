import pandas as pd
import mysql.connector
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Anirudh123",
    "database": "olist_db"
}

def create_connection():
    """Create MySQL connection."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        logger.info("[OK] MySQL connection established")
        return conn
    except mysql.connector.Error as e:
        logger.error(f"[ERROR] MySQL connection failed: {e}")
        raise


# EXTRACT

def extract_data():
    """Load all CSV files."""
    logger.info("Starting data extraction...")
    
    customers = pd.read_csv("data/olist_customers_dataset.csv")
    order_items = pd.read_csv("data/olist_order_items_dataset.csv")
    payments = pd.read_csv("data/olist_order_payments_dataset.csv")
    orders = pd.read_csv("data/olist_orders_dataset.csv")
    
    logger.info(f"   - Customers: {len(customers)} rows")
    logger.info(f"   - Orders: {len(orders)} rows")
    logger.info(f"   - Order Items: {len(order_items)} rows")
    logger.info(f"   - Payments: {len(payments)} rows")
    
    return customers, order_items, payments, orders


# TRANSFORM

def transform_data(customers, order_items, payments, orders):
    """Clean and merge all datasets."""
    logger.info("Starting data transformation...")
    
    # Basic cleaning - drop rows with missing critical IDs
    customers = customers.dropna(subset=['customer_id'])
    orders = orders.dropna(subset=['order_id', 'customer_id'])
    order_items = order_items.dropna(subset=['order_id'])
    payments = payments.dropna(subset=['order_id'])
    
    # Merge all data
    merged_df = (
        orders.merge(customers, on="customer_id", how="left")
              .merge(order_items, on="order_id", how="left")
              .merge(payments, on="order_id", how="left")
    )
    
    logger.info(f"   - Merged dataset: {len(merged_df)} rows")
    
    return merged_df, customers, orders


# LOAD - Master Table

def load_master_table(connection, merged_df):
    """Create and populate master table."""
    logger.info("Loading master table...")
    cursor = connection.cursor()
    
    # Drop and recreate table to ensure correct schema
    cursor.execute("DROP TABLE IF EXISTS olist_master")
    
    # Create master table with correct column names
    cursor.execute("""
    CREATE TABLE olist_master (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(50),
        customer_id VARCHAR(50),
        customer_city VARCHAR(100),
        customer_state VARCHAR(10),
        customer_zip_code_prefix VARCHAR(20),
        order_status VARCHAR(50),
        order_purchase_timestamp DATETIME,
        order_delivered_customer_date DATETIME,
        price DECIMAL(10,2),
        freight_value DECIMAL(10,2),
        payment_type VARCHAR(50),
        payment_value DECIMAL(10,2),
        product_id VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_order (order_id),
        INDEX idx_customer (customer_id),
        INDEX idx_state (customer_state)
    )
    """)
    
    logger.info("   - Master table created")
    
    # Helper function to safely get values
    def safe_get(row, column, default=None):
        """Safely get value from row, handling missing columns and NaN."""
        try:
            val = row.get(column, default)
            if pd.isna(val):
                return default
            return val
        except:
            return default
    
    def safe_float(val, default=0.0):
        """Safely convert to float."""
        try:
            if pd.isna(val):
                return default
            return float(val)
        except:
            return default
    
    def safe_datetime(val):
        """Safely convert to datetime string."""
        try:
            if pd.isna(val):
                return None
            return str(val)
        except:
            return None
    
    # Insert data in batches
    batch_size = 1000
    total_rows = 0
    errors = 0
    
    for i in range(0, len(merged_df), batch_size):
        batch = merged_df.iloc[i:i+batch_size]
        
        for _, row in batch.iterrows():
            try:
                cursor.execute("""
                INSERT INTO olist_master (
                    order_id, customer_id, customer_city, customer_state, customer_zip_code_prefix,
                    order_status, order_purchase_timestamp, order_delivered_customer_date,
                    price, freight_value, payment_type, payment_value, product_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    safe_get(row, 'order_id'),
                    safe_get(row, 'customer_id'),
                    safe_get(row, 'customer_city'),
                    safe_get(row, 'customer_state'),
                    safe_get(row, 'customer_zip_code_prefix'),  # FIXED: correct column name
                    safe_get(row, 'order_status'),
                    safe_datetime(safe_get(row, 'order_purchase_timestamp')),
                    safe_datetime(safe_get(row, 'order_delivered_customer_date')),
                    safe_float(safe_get(row, 'price'), 0.0),
                    safe_float(safe_get(row, 'freight_value'), 0.0),
                    safe_get(row, 'payment_type'),
                    safe_float(safe_get(row, 'payment_value'), 0.0),
                    safe_get(row, 'product_id')
                ))
                total_rows += 1
            except Exception as e:
                errors += 1
                if errors <= 5:  # Only log first 5 errors
                    logger.warning(f"   - Error inserting row: {e}")
        
        connection.commit()
        logger.info(f"   - Inserted {total_rows}/{len(merged_df)} rows (errors: {errors})")
    
    cursor.close()
    logger.info(f"[OK] Master table loaded: {total_rows} rows, {errors} errors")


# ANALYTICS TABLE 1: Sales Summary

def create_sales_summary(connection, merged_df):
    """Create sales summary by customer and state."""
    logger.info("Creating sales summary...")
    cursor = connection.cursor()
    
    # Calculate sales summary
    sales_summary = merged_df.groupby(
        ['customer_id', 'customer_state', 'customer_city']
    ).agg(
        total_spent=('payment_value', 'sum'),
        total_orders=('order_id', 'nunique'),
        total_items=('order_id', 'count'),
        avg_order_value=('payment_value', 'mean'),
        avg_price=('price', 'mean'),
        avg_freight=('freight_value', 'mean')
    ).reset_index()
    
    # Replace NaN with 0 for numeric columns
    sales_summary = sales_summary.fillna(0)
    
    # Create table
    cursor.execute("DROP TABLE IF EXISTS sales_summary")
    cursor.execute("""
    CREATE TABLE sales_summary (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_id VARCHAR(50),
        customer_state VARCHAR(10),
        customer_city VARCHAR(100),
        total_spent DECIMAL(10,2),
        total_orders INT,
        total_items INT,
        avg_order_value DECIMAL(10,2),
        avg_price DECIMAL(10,2),
        avg_freight DECIMAL(10,2),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_customer (customer_id),
        INDEX idx_state (customer_state)
    )
    """)
    
    # Insert data
    for _, row in sales_summary.iterrows():
        cursor.execute("""
        INSERT INTO sales_summary (
            customer_id, customer_state, customer_city, total_spent, 
            total_orders, total_items, avg_order_value, avg_price, avg_freight
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(row['customer_id']) if pd.notna(row['customer_id']) else None,
            str(row['customer_state']) if pd.notna(row['customer_state']) else None,
            str(row['customer_city']) if pd.notna(row['customer_city']) else None,
            float(row['total_spent']) if pd.notna(row['total_spent']) else 0.0,
            int(row['total_orders']) if pd.notna(row['total_orders']) else 0,
            int(row['total_items']) if pd.notna(row['total_items']) else 0,
            float(row['avg_order_value']) if pd.notna(row['avg_order_value']) else 0.0,
            float(row['avg_price']) if pd.notna(row['avg_price']) else 0.0,
            float(row['avg_freight']) if pd.notna(row['avg_freight']) else 0.0
        ))
    
    connection.commit()
    cursor.close()
    logger.info(f"[OK] Sales summary created: {len(sales_summary)} rows")


# ANALYTICS TABLE 2: Delivery Performance

def create_delivery_summary(connection, orders, customers):
    """Create delivery performance summary."""
    logger.info("Creating delivery performance summary...")
    cursor = connection.cursor()
    
    # Calculate delivery metrics
    orders['order_delivered_customer_date'] = pd.to_datetime(
        orders['order_delivered_customer_date'], errors='coerce'
    )
    orders['order_purchase_timestamp'] = pd.to_datetime(
        orders['order_purchase_timestamp'], errors='coerce'
    )
    orders['delivery_days'] = (
        orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']
    ).dt.days
    
    # Merge with customers
    delivery_data = orders.merge(customers, on="customer_id", how="left")
    
    # Group by state
    delivery_summary = delivery_data.groupby('customer_state').agg(
        total_orders=('order_id', 'count'),
        delivered_orders=('delivery_days', 'count'),
        avg_delivery_days=('delivery_days', 'mean'),
        median_delivery_days=('delivery_days', 'median'),
        fastest_delivery=('delivery_days', 'min'),
        slowest_delivery=('delivery_days', 'max'),
        std_delivery_days=('delivery_days', 'std')
    ).reset_index()
    
    # Calculate delivery rate
    delivery_summary['delivery_rate'] = (
        delivery_summary['delivered_orders'] / delivery_summary['total_orders'] * 100
    )
    
    # Create table
    cursor.execute("DROP TABLE IF EXISTS delivery_summary")
    cursor.execute("""
    CREATE TABLE delivery_summary (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_state VARCHAR(10),
        total_orders INT,
        delivered_orders INT,
        delivery_rate DECIMAL(5,2),
        avg_delivery_days DECIMAL(10,2),
        median_delivery_days DECIMAL(10,2),
        fastest_delivery INT,
        slowest_delivery INT,
        std_delivery_days DECIMAL(10,2),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_state (customer_state)
    )
    """)
    
    # Insert data
    for _, row in delivery_summary.iterrows():
        cursor.execute("""
        INSERT INTO delivery_summary (
            customer_state, total_orders, delivered_orders, delivery_rate,
            avg_delivery_days, median_delivery_days, fastest_delivery, 
            slowest_delivery, std_delivery_days
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['customer_state'],
            int(row['total_orders']),
            int(row['delivered_orders']),
            float(row['delivery_rate']),
            float(row['avg_delivery_days']) if pd.notna(row['avg_delivery_days']) else None,
            float(row['median_delivery_days']) if pd.notna(row['median_delivery_days']) else None,
            int(row['fastest_delivery']) if pd.notna(row['fastest_delivery']) else None,
            int(row['slowest_delivery']) if pd.notna(row['slowest_delivery']) else None,
            float(row['std_delivery_days']) if pd.notna(row['std_delivery_days']) else None
        ))
    
    connection.commit()
    cursor.close()
    logger.info(f"[OK] Delivery summary created: {len(delivery_summary)} rows")


# ANALYTICS TABLE 3: Product Performance

def create_product_summary(connection, merged_df):
    """Create product-level performance summary."""
    logger.info("Creating product summary...")
    cursor = connection.cursor()
    
    product_summary = merged_df.groupby('product_id').agg(
        total_orders=('order_id', 'nunique'),
        total_items_sold=('order_id', 'count'),
        total_revenue=('price', 'sum'),
        avg_price=('price', 'mean'),
        total_freight=('freight_value', 'sum'),
        avg_freight=('freight_value', 'mean')
    ).reset_index()
    
    # Create table
    cursor.execute("DROP TABLE IF EXISTS product_summary")
    cursor.execute("""
    CREATE TABLE product_summary (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_id VARCHAR(50),
        total_orders INT,
        total_items_sold INT,
        total_revenue DECIMAL(10,2),
        avg_price DECIMAL(10,2),
        total_freight DECIMAL(10,2),
        avg_freight DECIMAL(10,2),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_product (product_id)
    )
    """)
    
    # Insert data
    for _, row in product_summary.iterrows():
        cursor.execute("""
        INSERT INTO product_summary (
            product_id, total_orders, total_items_sold, total_revenue,
            avg_price, total_freight, avg_freight
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['product_id'],
            int(row['total_orders']),
            int(row['total_items_sold']),
            float(row['total_revenue']),
            float(row['avg_price']),
            float(row['total_freight']),
            float(row['avg_freight'])
        ))
    
    connection.commit()
    cursor.close()
    logger.info(f"[OK] Product summary created: {len(product_summary)} rows")


# ANALYTICS TABLE 4: State Performance

def create_state_summary(connection, merged_df):
    """Create state-level performance summary for Grafana visualization."""
    logger.info("Creating state performance summary...")
    cursor = connection.cursor()
    
    state_summary = merged_df.groupby('customer_state').agg(
        total_customers=('customer_id', 'nunique'),
        total_orders=('order_id', 'nunique'),
        total_revenue=('payment_value', 'sum'),
        avg_order_value=('payment_value', 'mean'),
        total_items=('order_id', 'count')
    ).reset_index()
    
    # Create table
    cursor.execute("DROP TABLE IF EXISTS state_summary")
    cursor.execute("""
    CREATE TABLE state_summary (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_state VARCHAR(10),
        total_customers INT,
        total_orders INT,
        total_revenue DECIMAL(10,2),
        avg_order_value DECIMAL(10,2),
        total_items INT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_state (customer_state)
    )
    """)
    
    # Insert data
    for _, row in state_summary.iterrows():
        cursor.execute("""
        INSERT INTO state_summary (
            customer_state, total_customers, total_orders, total_revenue,
            avg_order_value, total_items
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['customer_state'],
            int(row['total_customers']),
            int(row['total_orders']),
            float(row['total_revenue']),
            float(row['avg_order_value']),
            int(row['total_items'])
        ))
    
    connection.commit()
    cursor.close()
    logger.info(f"[OK] State summary created: {len(state_summary)} rows")


# MAIN PIPELINE

def run_pipeline():
    """Execute the complete ETL pipeline."""
    start_time = datetime.now()
    logger.info("="*60)
    logger.info(f"Starting ETL Pipeline at {start_time}")
    logger.info("="*60)
    
    try:
        # Extract
        customers, order_items, payments, orders = extract_data()
        
        # Transform
        merged_df, customers_clean, orders_clean = transform_data(
            customers, order_items, payments, orders
        )
        
        # Load
        connection = create_connection()
        
        load_master_table(connection, merged_df)
        create_sales_summary(connection, merged_df)
        create_delivery_summary(connection, orders_clean, customers_clean)
        create_product_summary(connection, merged_df)
        create_state_summary(connection, merged_df)
        
        connection.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info(f"[OK] Pipeline completed successfully!")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_pipeline()