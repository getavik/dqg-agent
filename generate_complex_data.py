
import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
np.random.seed(42)

def generate_complex_data(num_rows=100):
    data = []
    
    regions = ['North America', 'Europe', 'Asia', 'South America', np.nan]
    
    for i in range(num_rows):
        # 1. PII Data
        name = fake.name()
        email = fake.email() if random.random() > 0.1 else np.nan # 10% missing emails
        
        # 2. Inconsistent Phone Numbers
        if random.random() > 0.8:
            phone = f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        elif random.random() > 0.9:
            phone = "Invalid_Phone"
        else:
            phone = fake.phone_number()
            
        # 3. Financial Data with Errors
        # Revenue: Mostly positive, but some negative (errors)
        if random.random() > 0.95:
            revenue = round(random.uniform(-1000, -10), 2)
        else:
            revenue = round(random.uniform(100, 50000), 2)
            
        # 4. Dates
        signup_date = fake.date_between(start_date='-5y', end_date='today')
        
        # 5. Categorical
        region = random.choice(regions)
        
        # 6. Logic / additional fields
        loyalty_score = random.randint(1, 100)
        
        data.append([i+1, name, email, phone, revenue, signup_date, region, loyalty_score])
        
    df = pd.DataFrame(data, columns=['customer_id', 'name', 'email', 'phone', 'revenue', 'signup_date', 'region', 'loyalty_score'])
    
    return df

if __name__ == "__main__":
    print("Generating complex dataset...")
    df = generate_complex_data(200)
    output_path = "complex_data.csv"
    df.to_csv(output_path, index=False)
    print(f"Dataset generated: {output_path}")
    print(f"Shape: {df.shape}")
    print("Sample preview:")
    print(df.head())
    
    # Validation stats
    print(f"Missing Emails: {df['email'].isnull().sum()}")
    print(f"Negative Revenue: {(df['revenue'] < 0).sum()}")
    print(f"Missing Regions: {df['region'].isnull().sum()}")
