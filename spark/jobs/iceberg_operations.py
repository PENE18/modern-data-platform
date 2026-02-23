"""
Apache Iceberg Operations with PySpark
Demonstrates advanced Iceberg features:
- Table creation with partitioning
- Time travel queries
- Schema evolution
- Snapshot management
- Performance optimization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

class IcebergOperations:
    
    def __init__(self):
        """Initialize Spark session with Iceberg configuration"""
        print("="*80)
        print("Initializing Spark with Iceberg Support")
        print("="*80)
        
        self.spark = SparkSession.builder \
            .appName("IcebergEcommerceAnalytics") \
            .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v2") \
            .config("spark.sql.catalog.iceberg.ref", "main") \
            .config("spark.sql.catalog.iceberg.authentication.type", "NONE") \
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        print("✓ Spark session created successfully\n")
    
    def create_namespaces(self):
        """Create Iceberg namespaces for medallion architecture"""
        print("Creating Iceberg namespaces...")
        
        for namespace in ['bronze', 'silver', 'gold']:
            try:
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
                print(f"  ✓ Namespace: iceberg.{namespace}")
            except Exception as e:
                print(f"  ⚠ Warning creating {namespace}: {str(e)}")
        
        print()
    
    def create_bronze_tables(self):
        """Create Bronze layer Iceberg tables"""
        print("="*80)
        print("Creating Bronze Layer Tables")
        print("="*80 + "\n")
        
        # Orders table with partitioning
        print("1. Creating bronze.orders table...")
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.bronze.orders (
                order_id STRING,
                customer_id STRING,
                order_date DATE,
                product_id STRING,
                quantity INT,
                status STRING,
                payment_method STRING,
                total_amount DOUBLE,
                ingested_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(order_date))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.metadata.compression-codec' = 'gzip',
                'commit.retry.num-retries' = '3'
            )
        """)
        print("  ✓ Created with daily partitioning\n")
        
        # Customers table
        print("2. Creating bronze.customers table...")
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.bronze.customers (
                customer_id STRING,
                email STRING,
                first_name STRING,
                last_name STRING,
                city STRING,
                state STRING,
                registration_date DATE,
                customer_segment STRING,
                ingested_at TIMESTAMP
            )
            USING iceberg
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        print("  ✓ Created\n")
        
        # Products table
        print("3. Creating bronze.products table...")
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.bronze.products (
                product_id STRING,
                product_name STRING,
                category STRING,
                price DOUBLE,
                stock_quantity INT,
                ingested_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (category)
            TBLPROPERTIES (
                'write.format.default' = 'parquet'
            )
        """)
        print("  ✓ Created with category partitioning\n")
    
    def load_sample_data(self):
        """Load sample data into Bronze tables"""
        print("="*80)
        print("Loading Sample Data to Bronze Layer")
        print("="*80 + "\n")
        
        # Load from CSV files (simulated)
        data_path = "/opt/data/raw"
        
        try:
            # Load orders
            print("1. Loading orders...")
            orders_df = self.spark.read.csv(f"{data_path}/orders.csv", header=True, inferSchema=True)
            orders_df = orders_df.withColumn("ingested_at", current_timestamp()) \
                                 .withColumn("order_date", to_date(col("order_date")))
            
            orders_df.writeTo("iceberg.bronze.orders").using("iceberg").createOrReplace()
            count = self.spark.table("iceberg.bronze.orders").count()
            print(f"  ✓ Loaded {count:,} orders\n")
            
            # Load customers
            print("2. Loading customers...")
            customers_df = self.spark.read.csv(f"{data_path}/customers.csv", header=True, inferSchema=True)
            customers_df = customers_df.withColumn("ingested_at", current_timestamp()) \
                                       .withColumn("registration_date", to_date(col("registration_date")))
            
            customers_df.writeTo("iceberg.bronze.customers").using("iceberg").createOrReplace()
            count = self.spark.table("iceberg.bronze.customers").count()
            print(f"  ✓ Loaded {count:,} customers\n")
            
            # Load products
            print("3. Loading products...")
            products_df = self.spark.read.csv(f"{data_path}/products.csv", header=True, inferSchema=True)
            products_df = products_df.withColumn("ingested_at", current_timestamp())
            
            products_df.writeTo("iceberg.bronze.products").using("iceberg").createOrReplace()
            count = self.spark.table("iceberg.bronze.products").count()
            print(f"  ✓ Loaded {count:,} products\n")
            
        except Exception as e:
            print(f"⚠ Note: Sample data loading skipped. Run data generator first.")
            print(f"  Error: {str(e)}\n")
    
    def demonstrate_time_travel(self):
        """Demonstrate Iceberg time travel capabilities"""
        print("="*80)
        print("Demonstrating Time Travel")
        print("="*80 + "\n")
        
        try:
            # Show table history
            print("1. Table History:")
            history_df = self.spark.sql("SELECT * FROM iceberg.bronze.orders.history")
            history_df.show(5, truncate=False)
            
            # Show snapshots
            print("\n2. Table Snapshots:")
            snapshots_df = self.spark.sql("SELECT snapshot_id, committed_at, operation FROM iceberg.bronze.orders.snapshots")
            snapshots_df.show(5, truncate=False)
            
            # Time travel query example
            print("\n3. Time Travel Query (if snapshots exist):")
            if snapshots_df.count() > 0:
                # Query specific snapshot
                first_snapshot = snapshots_df.first()['snapshot_id']
                print(f"   Querying snapshot: {first_snapshot}")
                
                query = f"""
                    SELECT COUNT(*) as record_count 
                    FROM iceberg.bronze.orders 
                    VERSION AS OF {first_snapshot}
                """
                result = self.spark.sql(query)
                result.show()
            else:
                print("   No snapshots available yet")
        
        except Exception as e:
            print(f"⚠ Time travel demo skipped: {str(e)}")
        
        print()
    
    def demonstrate_schema_evolution(self):
        """Demonstrate schema evolution without downtime"""
        print("="*80)
        print("Demonstrating Schema Evolution")
        print("="*80 + "\n")
        
        try:
            # Show current schema
            print("1. Current Schema:")
            self.spark.table("iceberg.bronze.orders").printSchema()
            
            # Add new column
            print("\n2. Adding new column 'order_source'...")
            self.spark.sql("""
                ALTER TABLE iceberg.bronze.orders 
                ADD COLUMN order_source STRING
            """)
            print("  ✓ Column added\n")
            
            # Show updated schema
            print("3. Updated Schema:")
            self.spark.table("iceberg.bronze.orders").printSchema()
            
            # Demonstrate that old queries still work
            print("\n4. Old queries still work:")
            result = self.spark.sql("""
                SELECT order_id, total_amount 
                FROM iceberg.bronze.orders 
                LIMIT 5
            """)
            result.show()
            
        except Exception as e:
            print(f"⚠ Schema evolution demo skipped: {str(e)}")
        
        print()
    
    def create_silver_tables(self):
        """Create Silver layer with enriched data"""
        print("="*80)
        print("Creating Silver Layer Tables")
        print("="*80 + "\n")
        
        try:
            print("1. Creating silver.orders_enriched...")
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS iceberg.silver.orders_enriched
                USING iceberg
                PARTITIONED BY (order_year, order_month)
                AS
                SELECT 
                    o.order_id,
                    o.customer_id,
                    o.order_date,
                    o.product_id,
                    o.quantity,
                    o.status,
                    o.total_amount,
                    c.customer_segment,
                    c.state,
                    p.category,
                    p.price as unit_price,
                    YEAR(o.order_date) as order_year,
                    MONTH(o.order_date) as order_month,
                    o.quantity * p.price as calculated_revenue,
                    CASE WHEN o.total_amount > 500 THEN 'High' ELSE 'Normal' END as order_value_tier,
                    current_timestamp() as processed_at
                FROM iceberg.bronze.orders o
                LEFT JOIN iceberg.bronze.customers c ON o.customer_id = c.customer_id
                LEFT JOIN iceberg.bronze.products p ON o.product_id = p.product_id
                WHERE o.status != 'cancelled'
            """)
            
            count = self.spark.table("iceberg.silver.orders_enriched").count()
            print(f"  ✓ Created with {count:,} records\n")
            
        except Exception as e:
            print(f"⚠ Silver layer creation skipped: {str(e)}\n")
    
    def create_gold_aggregates(self):
        """Create Gold layer analytical aggregates"""
        print("="*80)
        print("Creating Gold Layer Aggregates")
        print("="*80 + "\n")
        
        try:
            # Daily sales summary
            print("1. Creating gold.daily_sales...")
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS iceberg.gold.daily_sales
                USING iceberg
                AS
                SELECT 
                    order_date,
                    COUNT(DISTINCT order_id) as num_orders,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    SUM(quantity) as total_items_sold
                FROM iceberg.silver.orders_enriched
                GROUP BY order_date
                ORDER BY order_date DESC
            """)
            
            count = self.spark.table("iceberg.gold.daily_sales").count()
            print(f"  ✓ Created with {count:,} days of data\n")
            
            # Product performance
            print("2. Creating gold.product_performance...")
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS iceberg.gold.product_performance
                USING iceberg
                AS
                SELECT 
                    product_id,
                    category,
                    COUNT(order_id) as num_orders,
                    SUM(quantity) as units_sold,
                    SUM(calculated_revenue) as total_revenue,
                    AVG(unit_price) as avg_price
                FROM iceberg.silver.orders_enriched
                GROUP BY product_id, category
                ORDER BY total_revenue DESC
            """)
            
            count = self.spark.table("iceberg.gold.product_performance").count()
            print(f"  ✓ Created with {count:,} products\n")
            
        except Exception as e:
            print(f"⚠ Gold layer creation skipped: {str(e)}\n")
    
    def show_performance_stats(self):
        """Show Iceberg performance statistics"""
        print("="*80)
        print("Iceberg Performance Statistics")
        print("="*80 + "\n")
        
        try:
            # Partition information
            print("1. Partition Statistics (bronze.orders):")
            partitions = self.spark.sql("""
                SELECT * FROM iceberg.bronze.orders.partitions
            """)
            partitions.show(10)
            
            # File statistics
            print("\n2. File Statistics (bronze.orders):")
            files = self.spark.sql("""
                SELECT 
                    file_path,
                    file_size_in_bytes / 1024 / 1024 as size_mb,
                    record_count
                FROM iceberg.bronze.orders.files
                LIMIT 10
            """)
            files.show(truncate=False)
            
        except Exception as e:
            print(f"⚠ Stats display skipped: {str(e)}\n")
    
    def run_sample_queries(self):
        """Run sample analytical queries"""
        print("="*80)
        print("Sample Analytical Queries")
        print("="*80 + "\n")
        
        try:
            # Top customers
            print("1. Top 10 Customers by Revenue:")
            query1 = """
                SELECT 
                    customer_id,
                    customer_segment,
                    COUNT(order_id) as num_orders,
                    SUM(total_amount) as total_revenue
                FROM iceberg.silver.orders_enriched
                GROUP BY customer_id, customer_segment
                ORDER BY total_revenue DESC
                LIMIT 10
            """
            self.spark.sql(query1).show()
            
            # Revenue by category
            print("\n2. Revenue by Product Category:")
            query2 = """
                SELECT 
                    category,
                    COUNT(DISTINCT product_id) as num_products,
                    SUM(calculated_revenue) as total_revenue,
                    AVG(unit_price) as avg_price
                FROM iceberg.silver.orders_enriched
                GROUP BY category
                ORDER BY total_revenue DESC
            """
            self.spark.sql(query2).show()
            
            # Monthly trends
            print("\n3. Monthly Sales Trend:")
            query3 = """
                SELECT 
                    order_year,
                    order_month,
                    COUNT(order_id) as orders,
                    SUM(total_amount) as revenue
                FROM iceberg.silver.orders_enriched
                GROUP BY order_year, order_month
                ORDER BY order_year, order_month
            """
            self.spark.sql(query3).show()
            
        except Exception as e:
            print(f"⚠ Queries skipped: {str(e)}\n")
    
    def cleanup(self):
        """Stop Spark session"""
        print("="*80)
        print("Cleaning up...")
        print("="*80)
        self.spark.stop()
        print("✓ Spark session stopped\n")

def main():
    """Main execution"""
    print("\n" + "="*80)
    print(" "*20 + "APACHE ICEBERG OPERATIONS DEMO")
    print("="*80 + "\n")
    
    ops = IcebergOperations()
    
    try:
        # 1. Setup
        ops.create_namespaces()
        ops.create_bronze_tables()
        
        # 2. Load data
        ops.load_sample_data()
        
        # 3. Demonstrate features
        ops.demonstrate_time_travel()
        ops.demonstrate_schema_evolution()
        
        # 4. Create Silver and Gold layers
        ops.create_silver_tables()
        ops.create_gold_aggregates()
        
        # 5. Show stats and queries
        ops.show_performance_stats()
        ops.run_sample_queries()
        
        print("\n" + "="*80)
        print("✓ Demo completed successfully!")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        ops.cleanup()

if __name__ == "__main__":
    main()
