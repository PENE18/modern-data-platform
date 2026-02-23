"""
E-commerce Data Generator
Generates realistic synthetic data for:
- Orders
- Customers
- Products
- Order Items
- Web Events
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import random
import os

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

class EcommerceDataGenerator:
    def __init__(self, n_customers=1000, n_products=200, n_orders=5000):
        self.n_customers = n_customers
        self.n_products = n_products
        self.n_orders = n_orders
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2024, 12, 31)
        
    def generate_customers(self):
        """Generate customer data"""
        print("Generating customers...")
        
        customers = []
        for i in range(self.n_customers):
            customer = {
                'customer_id': f'CUST-{i+1:06d}',
                'email': fake.email(),
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'zip_code': fake.zipcode(),
                'country': 'USA',
                'registration_date': fake.date_between(
                    start_date=self.start_date,
                    end_date=self.end_date
                ),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'VIP']),
                'lifetime_value': round(random.uniform(100, 10000), 2)
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)
    
    def generate_products(self):
        """Generate product catalog"""
        print("Generating products...")
        
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
        products = []
        
        for i in range(self.n_products):
            category = random.choice(categories)
            product = {
                'product_id': f'PROD-{i+1:05d}',
                'product_name': f'{fake.word().capitalize()} {category[:-1]}',
                'category': category,
                'subcategory': fake.word().capitalize(),
                'brand': fake.company(),
                'price': round(random.uniform(9.99, 999.99), 2),
                'cost': round(random.uniform(5, 500), 2),
                'stock_quantity': random.randint(0, 500),
                'weight_kg': round(random.uniform(0.1, 50), 2),
                'sku': fake.bothify(text='??-########'),
                'created_date': fake.date_between(
                    start_date=datetime(2022, 1, 1),
                    end_date=self.start_date
                )
            }
            # Ensure profit margin
            product['cost'] = min(product['cost'], product['price'] * 0.7)
            products.append(product)
        
        return pd.DataFrame(products)
    
    def generate_orders(self, customers_df, products_df):
        """Generate orders and order items"""
        print("Generating orders...")
        
        orders = []
        order_items = []
        
        for i in range(self.n_orders):
            # Select random customer
            customer_id = random.choice(customers_df['customer_id'].tolist())
            
            # Generate order date
            order_date = fake.date_time_between(
                start_date=self.start_date,
                end_date=self.end_date
            )
            
            # Order details
            order_id = f'ORD-{i+1:08d}'
            status = random.choices(
                ['completed', 'pending', 'cancelled', 'refunded', 'processing'],
                weights=[70, 15, 5, 3, 7]
            )[0]
            
            # Generate 1-5 items per order
            n_items = random.randint(1, 5)
            order_total = 0
            
            for item_num in range(n_items):
                product = products_df.sample(1).iloc[0]
                quantity = random.randint(1, 5)
                unit_price = product['price']
                
                # Apply random discount
                discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
                discount_amount = round(unit_price * quantity * discount_pct / 100, 2)
                
                line_total = round(unit_price * quantity - discount_amount, 2)
                order_total += line_total
                
                order_item = {
                    'order_item_id': f'{order_id}-{item_num+1}',
                    'order_id': order_id,
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'discount_amount': discount_amount,
                    'line_total': line_total
                }
                order_items.append(order_item)
            
            # Add shipping cost
            shipping_cost = round(random.uniform(0, 25), 2) if order_total < 50 else 0
            tax = round(order_total * 0.08, 2)  # 8% tax
            
            order = {
                'order_id': order_id,
                'customer_id': customer_id,
                'order_date': order_date,
                'order_time': order_date.strftime('%H:%M:%S'),
                'status': status,
                'payment_method': random.choice(['Credit Card', 'PayPal', 'Debit Card', 'Apple Pay']),
                'subtotal': order_total,
                'tax': tax,
                'shipping_cost': shipping_cost,
                'total_amount': order_total + tax + shipping_cost,
                'shipping_address': fake.address().replace('\n', ', '),
                'shipping_city': fake.city(),
                'shipping_state': fake.state_abbr(),
                'shipping_country': 'USA',
                'order_source': random.choice(['Web', 'Mobile App', 'Phone', 'In-Store']),
                'coupon_code': fake.bothify(text='???###') if random.random() < 0.3 else None,
                'is_first_order': random.choice([True, False]),
                'customer_notes': fake.sentence() if random.random() < 0.1 else None
            }
            orders.append(order)
        
        return pd.DataFrame(orders), pd.DataFrame(order_items)
    
    def generate_web_events(self, customers_df, products_df, n_events=10000):
        """Generate web clickstream events"""
        print("Generating web events...")
        
        events = []
        event_types = ['page_view', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase', 'search']
        
        for i in range(n_events):
            customer_id = random.choice(customers_df['customer_id'].tolist()) if random.random() < 0.7 else None
            
            event = {
                'event_id': f'EVT-{i+1:010d}',
                'customer_id': customer_id,
                'session_id': fake.uuid4(),
                'event_type': random.choice(event_types),
                'event_timestamp': fake.date_time_between(
                    start_date=self.start_date,
                    end_date=self.end_date
                ),
                'product_id': random.choice(products_df['product_id'].tolist()) if random.random() < 0.6 else None,
                'page_url': fake.url(),
                'referrer': random.choice(['Google', 'Facebook', 'Direct', 'Email', 'Instagram']),
                'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
                'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge']),
                'ip_address': fake.ipv4(),
                'country': 'USA',
                'city': fake.city()
            }
            events.append(event)
        
        return pd.DataFrame(events)
    
    def save_to_csv(self, output_dir='./data/raw'):
        """Generate and save all datasets"""
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate datasets
        customers_df = self.generate_customers()
        products_df = self.generate_products()
        orders_df, order_items_df = self.generate_orders(customers_df, products_df)
        events_df = self.generate_web_events(customers_df, products_df)
        
        # Save to CSV
        print(f"\nSaving datasets to {output_dir}...")
        customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
        products_df.to_csv(f'{output_dir}/products.csv', index=False)
        orders_df.to_csv(f'{output_dir}/orders.csv', index=False)
        order_items_df.to_csv(f'{output_dir}/order_items.csv', index=False)
        events_df.to_csv(f'{output_dir}/web_events.csv', index=False)
        
        # Print summary
        print("\n" + "="*60)
        print("Data Generation Complete!")
        print("="*60)
        print(f"Customers:    {len(customers_df):,} records")
        print(f"Products:     {len(products_df):,} records")
        print(f"Orders:       {len(orders_df):,} records")
        print(f"Order Items:  {len(order_items_df):,} records")
        print(f"Web Events:   {len(events_df):,} records")
        print("="*60)
        
        return {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df,
            'order_items': order_items_df,
            'events': events_df
        }

if __name__ == "__main__":
    generator = EcommerceDataGenerator(
        n_customers=1000,
        n_products=200,
        n_orders=5000
    )
    datasets = generator.save_to_csv()
