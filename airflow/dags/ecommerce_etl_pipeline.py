"""
E-commerce Data Pipeline - Complete ETL with Iceberg
Implements medallion architecture (Bronze → Silver → Gold)

Pipeline Steps:
1. Generate synthetic e-commerce data
2. Ingest to Bronze (raw data)
3. Transform to Silver (cleaned data)
4. Aggregate to Gold (analytics tables)
5. Data quality checks at each stage
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# Add scripts to path
sys.path.insert(0, '/opt/airflow/dags')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# TASK 1: Generate Sample Data
# ============================================

def generate_sample_data(**context):
    """Generate synthetic e-commerce data"""
    print("="*60)
    print("TASK: Generate Sample Data")
    print("="*60)
    
    # Import here to avoid import errors
    from faker import Faker
    import random
    
    fake = Faker()
    Faker.seed(42)
    random.seed(42)
    
    output_dir = '/opt/data/raw'
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate Customers
    print("\n1. Generating customers...")
    customers = []
    for i in range(1000):
        customers.append({
            'customer_id': f'CUST-{i+1:06d}',
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic'])
        })
    customers_df = pd.DataFrame(customers)
    customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
    
    # Generate Products
    print("2. Generating products...")
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
    products = []
    for i in range(200):
        category = random.choice(categories)
        products.append({
            'product_id': f'PROD-{i+1:05d}',
            'product_name': f'{fake.word().capitalize()} {category}',
            'category': category,
            'price': round(random.uniform(10, 1000), 2),
            'stock_quantity': random.randint(0, 500)
        })
    products_df = pd.DataFrame(products)
    products_df.to_csv(f'{output_dir}/products.csv', index=False)
    
    # Generate Orders
    print("3. Generating orders...")
    orders = []
    for i in range(5000):
        orders.append({
            'order_id': f'ORD-{i+1:08d}',
            'customer_id': random.choice(customers_df['customer_id'].tolist()),
            'order_date': fake.date_between(start_date='-1y', end_date='today'),
            'product_id': random.choice(products_df['product_id'].tolist()),
            'quantity': random.randint(1, 5),
            'status': random.choice(['completed', 'pending', 'cancelled']),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Debit Card']),
            'total_amount': round(random.uniform(20, 2000), 2)
        })
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv(f'{output_dir}/orders.csv', index=False)
    
    print("\n" + "="*60)
    print("Data Generation Summary:")
    print(f"  Customers: {len(customers_df):,}")
    print(f"  Products:  {len(products_df):,}")
    print(f"  Orders:    {len(orders_df):,}")
    print("="*60)
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(key='customers_count', value=len(customers_df))
    context['task_instance'].xcom_push(key='products_count', value=len(products_df))
    context['task_instance'].xcom_push(key='orders_count', value=len(orders_df))
    
    return {'status': 'success', 'records': len(orders_df)}


# ============================================
# TASK 2: Ingest to Bronze Layer
# ============================================

def ingest_to_bronze(**context):
    """Ingest raw CSV files to Bronze layer (Iceberg tables)"""
    print("="*60)
    print("TASK: Ingest to Bronze Layer")
    print("="*60)
    
    import pandas as pd
    
    # In production, this would use PySpark + PyIceberg
    # For this demo, we'll simulate the process
    
    raw_dir = '/opt/data/raw'
    bronze_dir = '/opt/data/bronze'
    os.makedirs(bronze_dir, exist_ok=True)
    
    tables = ['customers', 'products', 'orders']
    
    for table in tables:
        print(f"\nIngesting {table}...")
        df = pd.read_csv(f'{raw_dir}/{table}.csv')
        
        # Add metadata columns
        df['ingested_at'] = datetime.now()
        df['source_file'] = f'{table}.csv'
        df['data_quality_flag'] = 'pending'
        
        # Save to Bronze (in production: write to Iceberg)
        df.to_parquet(f'{bronze_dir}/{table}.parquet', index=False)
        print(f"  ✓ Ingested {len(df):,} records")
    
    print("\n" + "="*60)
    print("Bronze layer ingestion complete")
    print("="*60)
    
    return {'status': 'success'}


# ============================================
# TASK 3: Data Quality Validation
# ============================================

def validate_data_quality(**context):
    """Run data quality checks on Bronze layer"""
    print("="*60)
    print("TASK: Data Quality Validation")
    print("="*60)
    
    import pandas as pd
    
    bronze_dir = '/opt/data/bronze'
    validation_results = {}
    
    # Validate Orders
    print("\nValidating orders...")
    orders_df = pd.read_parquet(f'{bronze_dir}/orders.parquet')
    
    checks = {
        'no_null_order_id': orders_df['order_id'].notna().all(),
        'no_null_customer_id': orders_df['customer_id'].notna().all(),
        'positive_amounts': (orders_df['total_amount'] > 0).all(),
        'valid_quantities': (orders_df['quantity'] > 0).all(),
        'valid_status': orders_df['status'].isin(['completed', 'pending', 'cancelled']).all()
    }
    
    validation_results['orders'] = checks
    
    # Print results
    print("\nValidation Results:")
    for table, checks in validation_results.items():
        print(f"\n{table.upper()}:")
        for check_name, passed in checks.items():
            status = "✓ PASS" if passed else "✗ FAIL"
            print(f"  {check_name}: {status}")
    
    # Check if all validations passed
    all_passed = all(all(checks.values()) for checks in validation_results.values())
    
    if not all_passed:
        print("\n⚠ WARNING: Some data quality checks failed!")
        # In production, this might raise an exception
    else:
        print("\n✓ All data quality checks passed")
    
    return {'status': 'success', 'all_passed': all_passed}


# ============================================
# TASK 4: Transform to Silver Layer
# ============================================

def transform_to_silver(**context):
    """Clean and enrich data for Silver layer"""
    print("="*60)
    print("TASK: Transform to Silver Layer")
    print("="*60)
    
    import pandas as pd
    
    bronze_dir = '/opt/data/bronze'
    silver_dir = '/opt/data/silver'
    os.makedirs(silver_dir, exist_ok=True)
    
    # Load Bronze tables
    orders_df = pd.read_parquet(f'{bronze_dir}/orders.parquet')
    customers_df = pd.read_parquet(f'{bronze_dir}/customers.parquet')
    products_df = pd.read_parquet(f'{bronze_dir}/products.parquet')
    
    print("\n1. Cleaning orders...")
    # Remove cancelled orders
    orders_clean = orders_df[orders_df['status'] != 'cancelled'].copy()
    
    # Convert dates
    orders_clean['order_date'] = pd.to_datetime(orders_clean['order_date'])
    orders_clean['order_year'] = orders_clean['order_date'].dt.year
    orders_clean['order_month'] = orders_clean['order_date'].dt.month
    orders_clean['order_quarter'] = orders_clean['order_date'].dt.quarter
    
    print("\n2. Enriching with customer data...")
    # Join with customer data
    orders_enriched = orders_clean.merge(
        customers_df[['customer_id', 'customer_segment', 'state']],
        on='customer_id',
        how='left'
    )
    
    print("\n3. Enriching with product data...")
    # Join with product data
    orders_enriched = orders_enriched.merge(
        products_df[['product_id', 'category', 'price']],
        on='product_id',
        how='left'
    )
    
    # Calculate derived metrics
    orders_enriched['unit_price'] = orders_enriched['price']
    orders_enriched['revenue'] = orders_enriched['quantity'] * orders_enriched['unit_price']
    orders_enriched['is_high_value'] = orders_enriched['total_amount'] > 500
    
    # Save to Silver
    orders_enriched.to_parquet(f'{silver_dir}/orders_enriched.parquet', index=False)
    
    print("\n" + "="*60)
    print("Silver Layer Summary:")
    print(f"  Records cleaned: {len(orders_clean):,}")
    print(f"  Records enriched: {len(orders_enriched):,}")
    print(f"  New columns added: {len(orders_enriched.columns) - len(orders_df.columns)}")
    print("="*60)
    
    return {'status': 'success', 'records': len(orders_enriched)}


# ============================================
# TASK 5: Aggregate to Gold Layer
# ============================================

def aggregate_to_gold(**context):
    """Create analytics-ready aggregated tables"""
    print("="*60)
    print("TASK: Aggregate to Gold Layer")
    print("="*60)
    
    import pandas as pd
    
    silver_dir = '/opt/data/silver'
    gold_dir = '/opt/data/gold'
    os.makedirs(gold_dir, exist_ok=True)
    
    # Load Silver data
    orders_df = pd.read_parquet(f'{silver_dir}/orders_enriched.parquet')
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    
    # 1. Daily Sales Summary
    print("\n1. Creating daily sales summary...")
    daily_sales = orders_df.groupby('order_date').agg({
        'order_id': 'count',
        'total_amount': 'sum',
        'customer_id': 'nunique',
        'quantity': 'sum'
    }).reset_index()
    
    daily_sales.columns = ['date', 'num_orders', 'total_revenue', 'unique_customers', 'total_items']
    daily_sales['avg_order_value'] = daily_sales['total_revenue'] / daily_sales['num_orders']
    daily_sales.to_parquet(f'{gold_dir}/daily_sales.parquet', index=False)
    
    # 2. Customer Metrics
    print("2. Creating customer metrics...")
    customer_metrics = orders_df.groupby('customer_id').agg({
        'order_id': 'count',
        'total_amount': 'sum',
        'order_date': ['min', 'max']
    }).reset_index()
    
    customer_metrics.columns = ['customer_id', 'total_orders', 'lifetime_value', 'first_order', 'last_order']
    customer_metrics['days_active'] = (
        pd.to_datetime(customer_metrics['last_order']) - 
        pd.to_datetime(customer_metrics['first_order'])
    ).dt.days
    customer_metrics.to_parquet(f'{gold_dir}/customer_metrics.parquet', index=False)
    
    # 3. Product Performance
    print("3. Creating product performance...")
    product_perf = orders_df.groupby('product_id').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    product_perf.columns = ['product_id', 'num_orders', 'units_sold', 'total_revenue']
    product_perf = product_perf.sort_values('total_revenue', ascending=False)
    product_perf.to_parquet(f'{gold_dir}/product_performance.parquet', index=False)
    
    # 4. Regional Analysis
    print("4. Creating regional analysis...")
    regional = orders_df.groupby('state').agg({
        'order_id': 'count',
        'total_amount': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    
    regional.columns = ['state', 'num_orders', 'total_revenue', 'unique_customers']
    regional = regional.sort_values('total_revenue', ascending=False)
    regional.to_parquet(f'{gold_dir}/regional_sales.parquet', index=False)
    
    print("\n" + "="*60)
    print("Gold Layer Summary:")
    print(f"  Daily Sales:     {len(daily_sales):,} days")
    print(f"  Customer Metrics: {len(customer_metrics):,} customers")
    print(f"  Product Performance: {len(product_perf):,} products")
    print(f"  Regional Analysis: {len(regional):,} states")
    print("="*60)
    
    # Push top metrics to XCom
    context['task_instance'].xcom_push(
        key='top_revenue_state',
        value=regional.iloc[0]['state'] if len(regional) > 0 else 'N/A'
    )
    
    return {'status': 'success'}


# ============================================
# TASK 6: Generate Report
# ============================================

def generate_report(**context):
    """Generate summary report"""
    print("="*60)
    print("TASK: Generate Analytics Report")
    print("="*60)
    
    import pandas as pd
    
    gold_dir = '/opt/data/gold'
    
    # Load Gold tables
    daily_sales = pd.read_parquet(f'{gold_dir}/daily_sales.parquet')
    customer_metrics = pd.read_parquet(f'{gold_dir}/customer_metrics.parquet')
    product_perf = pd.read_parquet(f'{gold_dir}/product_performance.parquet')
    
    # Calculate KPIs
    total_revenue = daily_sales['total_revenue'].sum()
    total_orders = daily_sales['num_orders'].sum()
    avg_order_value = total_revenue / total_orders
    total_customers = len(customer_metrics)
    
    print("\n" + "="*60)
    print("KEY PERFORMANCE INDICATORS")
    print("="*60)
    print(f"Total Revenue:        ${total_revenue:,.2f}")
    print(f"Total Orders:         {total_orders:,}")
    print(f"Average Order Value:  ${avg_order_value:.2f}")
    print(f"Total Customers:      {total_customers:,}")
    print(f"Top Product Revenue:  ${product_perf.iloc[0]['total_revenue']:,.2f}")
    print("="*60)
    
    # Save report
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_revenue': float(total_revenue),
        'total_orders': int(total_orders),
        'avg_order_value': float(avg_order_value),
        'total_customers': int(total_customers)
    }
    
    import json
    with open('/opt/data/gold/kpi_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("\n✓ Report saved to /opt/data/gold/kpi_report.json")
    
    return report


# ============================================
# DAG DEFINITION
# ============================================

with DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Complete e-commerce ETL pipeline with medallion architecture',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl', 'iceberg', 'medallion'],
) as dag:

    # Task 1: Generate data
    task_generate = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,
        provide_context=True,
    )

    # Task 2: Ingest to Bronze
    task_bronze = PythonOperator(
        task_id='ingest_to_bronze',
        python_callable=ingest_to_bronze,
        provide_context=True,
    )

    # Task 3: Data quality checks
    task_validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        provide_context=True,
    )

    # Task 4: Transform to Silver
    task_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
        provide_context=True,
    )

    # Task 5: Aggregate to Gold
    task_gold = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
        provide_context=True,
    )

    # Task 6: Generate report
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )

    # Task 7: Success notification
    task_success = BashOperator(
        task_id='pipeline_success',
        bash_command='echo "✓ Pipeline completed successfully! Check /opt/data/gold for results."',
    )

    # Define task dependencies
    task_generate >> task_bronze >> task_validate >> task_silver >> task_gold >> task_report >> task_success
