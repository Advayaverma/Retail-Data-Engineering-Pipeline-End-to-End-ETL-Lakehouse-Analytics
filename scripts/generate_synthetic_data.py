#!/usr/bin/env python3
"""
Synthetic Retail Data Generator for Retail Data Engineering Pipeline.

Produces deterministic, high-volume retail datasets for distributed lakehouse processing:
- Transactions: 105,000+ records (with controlled data-quality defects)
- Customers: 10,500+ records
- Products: 1,200+ records
- Stores: 55 records

Also supports generating Day-2 incremental delta batches for demonstrating
Delta Lake MERGE and SCD Type 2 updates.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import csv
import random
from datetime import datetime, timedelta

# Fix deterministic random seed for exact reproducibility across runs
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

# Configuration counts
STORE_COUNT = 55
PRODUCT_COUNT = 1200
CUSTOMER_COUNT = 10500
TRANSACTION_COUNT = 105000

# Geographic Reference Data
CITIES_BY_STATE = [
    ("New York", "NY", "East"),
    ("Buffalo", "NY", "East"),
    ("Albany", "NY", "East"),
    ("Los Angeles", "CA", "West"),
    ("San Francisco", "CA", "West"),
    ("San Diego", "CA", "West"),
    ("Sacramento", "CA", "West"),
    ("Chicago", "IL", "Central"),
    ("Springfield", "IL", "Central"),
    ("Houston", "TX", "South"),
    ("Dallas", "TX", "South"),
    ("Austin", "TX", "South"),
    ("San Antonio", "TX", "South"),
    ("Miami", "FL", "South"),
    ("Orlando", "FL", "South"),
    ("Tampa", "FL", "South"),
    ("Seattle", "WA", "West"),
    ("Boston", "MA", "East"),
    ("Denver", "CO", "West"),
    ("Atlanta", "GA", "South"),
    ("Phoenix", "AZ", "West"),
    ("Philadelphia", "PA", "East"),
    ("Detroit", "MI", "Central"),
    ("Minneapolis", "MN", "Central"),
    ("Charlotte", "NC", "South"),
]

PRODUCT_TAXONOMY = {
    "Electronics": {
        "Audio": (["Sony", "Bose", "JBL", "Sennheiser"], (30.0, 350.0)),
        "Computers": (["Dell", "HP", "Lenovo", "Apple", "Asus"], (400.0, 2200.0)),
        "Smartphones": (["Samsung", "Apple", "Google", "OnePlus"], (250.0, 1400.0)),
        "Accessories": (["Anker", "Belkin", "Logitech"], (12.0, 90.0)),
    },
    "Apparel": {
        "Menswear": (["Nike", "Adidas", "Levi's", "Under Armour"], (20.0, 150.0)),
        "Womenswear": (["Zara", "H&M", "Lululemon", "Gap"], (25.0, 180.0)),
        "Footwear": (["Nike", "Puma", "New Balance", "Clarks"], (40.0, 220.0)),
    },
    "Home & Kitchen": {
        "Cookware": (["T-fal", "Cuisinart", "Le Creuset", "Calphalon"], (35.0, 300.0)),
        "Small Appliances": (["Ninja", "Keurig", "KitchenAid", "Breville"], (45.0, 450.0)),
        "Storage & Organization": (["Rubbermaid", "OXO", "Simplehuman"], (15.0, 80.0)),
    },
    "Health & Beauty": {
        "Skincare": (["CeraVe", "Neutrogena", "La Roche-Posay", "The Ordinary"], (10.0, 75.0)),
        "Haircare": (["Olaplex", "L'Oreal", "Pantene", "Redken"], (8.0, 60.0)),
        "Wellness": (["Nature Made", "Optimum Nutrition", "GNC"], (15.0, 90.0)),
    },
    "Sports & Outdoors": {
        "Fitness Equipment": (["Bowflex", "NordicTrack", "Rogue"], (50.0, 850.0)),
        "Camping & Hiking": (["Coleman", "The North Face", "Columbia"], (30.0, 400.0)),
    },
}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Jason", "Rebecca", "Edward", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen", "Stephen", "Anna",
    "Larry", "Brenda", "Justin", "Pamela", "Scott", "Nicole", "Brandon", "Emma",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Gregory", "Christine", "Frank", "Debra",
    "Alexander", "Rachel", "Raymond", "Catherine", "Patrick", "Carolyn", "Jack", "Janet",
    "Dennis", "Ruth", "Jerry", "Maria", "Tyler", "Heather", "Aaron", "Diane",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
]

PAYMENT_METHODS_STANDARD = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash"]
CUSTOMER_SEGMENTS = ["Standard", "Premium", "VIP", "Corporate"]


def ensure_dir(path: str):
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)


def generate_stores(count: int = STORE_COUNT) -> list:
    """Generate store metadata."""
    print(f"[1/4] Generating {count} Stores...")
    stores = []
    for i in range(1, count + 1):
        store_id = f"STR-{i:03d}"
        city_info = CITIES_BY_STATE[(i - 1) % len(CITIES_BY_STATE)]
        store_type = random.choice(["Flagship", "Supercenter", "Express", "Mall Outlet"])
        store_name = f"RetailOne {city_info[0]} {store_type}"
        stores.append({
            "store_id": store_id,
            "store_name": store_name,
            "city": city_info[0],
            "state": city_info[1],
            "region": city_info[2],
            "store_type": store_type,
            "opened_date": (datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3000))).strftime("%Y-%m-%d"),
        })
    return stores


def generate_products(count: int = PRODUCT_COUNT) -> list:
    """Generate product catalog with realistic taxonomy and pricing."""
    print(f"[2/4] Generating {count} Products...")
    products = []
    product_idx = 1
    
    categories = list(PRODUCT_TAXONOMY.keys())
    while product_idx <= count:
        category = random.choice(categories)
        subcategories = list(PRODUCT_TAXONOMY[category].keys())
        subcategory = random.choice(subcategories)
        brands, (min_cost, max_cost) = PRODUCT_TAXONOMY[category][subcategory]
        brand = random.choice(brands)
        
        unit_cost = round(random.uniform(min_cost, max_cost), 2)
        # Margin 20% to 50%
        margin = random.uniform(1.20, 1.50)
        unit_price = round(unit_cost * margin, 2)
        
        product_id = f"PRD-{product_idx:04d}"
        product_name = f"{brand} {subcategory} Model {random.randint(100, 999)}"
        
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "subcategory": subcategory,
            "brand": brand,
            "unit_cost": unit_cost,
            "recommended_price": unit_price,
        })
        product_idx += 1
    return products


def generate_customers(count: int = CUSTOMER_COUNT) -> list:
    """Generate customer master directory."""
    print(f"[3/4] Generating {count} Customers...")
    customers = []
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2025, 12, 31)
    date_delta_days = (end_date - start_date).days

    for i in range(1, count + 1):
        customer_id = f"CUST-{i:05d}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        city_info = random.choice(CITIES_BY_STATE)
        email = f"{fname.lower()}.{lname.lower()}{i}@example.com"
        signup_date = (start_date + timedelta(days=random.randint(0, date_delta_days))).strftime("%Y-%m-%d")
        segment = random.choices(
            CUSTOMER_SEGMENTS,
            weights=[0.60, 0.25, 0.10, 0.05],  # Realistic distribution
            k=1
        )[0]

        customers.append({
            "customer_id": customer_id,
            "customer_name": f"{fname} {lname}",
            "email": email,
            "city": city_info[0],
            "state": city_info[1],
            "signup_date": signup_date,
            "customer_segment": segment,
        })
    return customers


def generate_transactions(
    count: int,
    customers: list,
    products: list,
    stores: list,
    inject_defects: bool = True
) -> tuple:
    """
    Generate retail transactions with controlled, realistic data quality defects.
    
    Returns:
        (transactions_list, defects_summary_dict)
    """
    print(f"[4/4] Generating {count} Transactions (Inject defects = {inject_defects})...")
    transactions = []
    
    start_time = datetime(2024, 1, 1, 8, 0, 0)
    end_time = datetime(2025, 12, 31, 22, 0, 0)
    total_seconds = int((end_time - start_time).total_seconds())

    customer_ids = [c["customer_id"] for c in customers]
    product_map = {p["product_id"]: p for p in products}
    product_ids = list(product_map.keys())
    store_ids = [s["store_id"] for s in stores]

    defect_counts = {
        "missing_customer_id": 0,
        "invalid_customer_id": 0,
        "invalid_unit_price": 0,
        "invalid_quantity": 0,
        "malformed_timestamp": 0,
        "inconsistent_payment_method": 0,
        "duplicate_transactions": 0,
    }

    # Generate baseline transactions
    for i in range(1, count + 1):
        txn_id = f"TXN-{i:07d}"
        cust_id = random.choice(customer_ids)
        prod_id = random.choice(product_ids)
        store_id = random.choice(store_ids)
        
        # Realistic timestamp
        txn_time = start_time + timedelta(seconds=random.randint(0, total_seconds))
        timestamp_str = txn_time.strftime("%Y-%m-%d %H:%M:%S")

        # Quantities: most shoppers buy 1-3 items, occasional bulk 4-10
        quantity = random.choices([1, 2, 3, 4, 5, 8, 10], weights=[0.45, 0.25, 0.15, 0.08, 0.04, 0.02, 0.01], k=1)[0]
        
        base_price = product_map[prod_id]["recommended_price"]
        # Slight price fluctuation +/- 5%
        unit_price = round(base_price * random.uniform(0.95, 1.05), 2)
        
        # Discount: mostly 0, sometimes 5%, 10%, 15%, 20%
        discount = random.choices([0.0, 0.05, 0.10, 0.15, 0.20], weights=[0.60, 0.15, 0.15, 0.07, 0.03], k=1)[0]
        payment_method = random.choice(PAYMENT_METHODS_STANDARD)

        # -------------------------------------------------------------
        # Controlled Defect Injection (Demonstrates Data Quality Layer)
        # Target: ~1.5% - 2.0% total defect rate
        # -------------------------------------------------------------
        if inject_defects:
            defect_roll = random.random()

            # Defect 1: Missing Customer ID (0.25%)
            if defect_roll < 0.0025:
                cust_id = ""  # null / blank
                defect_counts["missing_customer_id"] += 1

            # Defect 2: Invalid/Unknown Customer ID (referential integrity failure) (0.20%)
            elif defect_roll < 0.0045:
                cust_id = f"CUST-{random.randint(99000, 99999)}"
                defect_counts["invalid_customer_id"] += 1

            # Defect 3: Invalid Price (negative or zero) (0.15%)
            elif defect_roll < 0.0060:
                unit_price = random.choice([-19.99, -5.00, 0.00])
                defect_counts["invalid_unit_price"] += 1

            # Defect 4: Invalid Quantity (negative or zero) (0.15%)
            elif defect_roll < 0.0075:
                quantity = random.choice([-1, -3, 0])
                defect_counts["invalid_quantity"] += 1

            # Defect 5: Malformed Timestamp (unparseable string) (0.12%)
            elif defect_roll < 0.0087:
                timestamp_str = random.choice([
                    "2024/02/31 25:99:99",
                    "INVALID_DATE_TIME",
                    "2024-13-45",
                    "NULL",
                ])
                defect_counts["malformed_timestamp"] += 1

            # Defect 6: Inconsistent categorical payment methods (0.30%)
            elif defect_roll < 0.0117:
                payment_method = random.choice([
                    "credit_card", "CREDIT CARD", "CC", "debit-card", "UNKNOWN", "Crypto"
                ])
                defect_counts["inconsistent_payment_method"] += 1

        transactions.append({
            "transaction_id": txn_id,
            "customer_id": cust_id,
            "product_id": prod_id,
            "store_id": store_id,
            "transaction_timestamp": timestamp_str,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "payment_method": payment_method,
        })

    # Defect 7: Duplicate transactions (duplicate identical or duplicate ID)
    if inject_defects:
        duplicate_sample_count = 210  # ~0.2%
        for _ in range(duplicate_sample_count):
            dupe_record = random.choice(transactions).copy()
            transactions.append(dupe_record)
            defect_counts["duplicate_transactions"] += 1

    return transactions, defect_counts


def save_csv(file_path: str, data: list, fieldnames: list):
    """Write list of dictionaries to CSV with header."""
    ensure_dir(os.path.dirname(file_path))
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"  -> Saved {file_path} ({len(data):,} rows, {size_mb:.2f} MB)")


def generate_day2_incremental(
    baseline_customers: list,
    products: list,
    stores: list,
    new_txn_count: int = 5000,
    updated_txn_count: int = 500,
    customer_updates_count: int = 250
):
    """
    Generate Day-2 incremental updates to demonstrate Delta Lake MERGE and SCD Type 2:
    - 5,000 new transactions
    - 500 updated/corrected transactions (e.g. quantity adjustment or discount correction)
    - 250 customer segment/city updates (triggering SCD Type 2 splits)
    """
    print(f"\n[Incremental] Generating Day-2 Delta Batch...")
    
    # 1. Day-2 New Transactions
    day2_start_time = datetime(2026, 1, 1, 8, 0, 0)
    day2_txns = []
    cust_ids = [c["customer_id"] for c in baseline_customers]
    prod_map = {p["product_id"]: p for p in products}
    prod_ids = list(prod_map.keys())
    store_ids = [s["store_id"] for s in stores]

    for i in range(1, new_txn_count + 1):
        txn_id = f"TXN-D2-{i:06d}"
        prod_id = random.choice(prod_ids)
        price = prod_map[prod_id]["recommended_price"]
        day2_txns.append({
            "transaction_id": txn_id,
            "customer_id": random.choice(cust_ids),
            "product_id": prod_id,
            "store_id": random.choice(store_ids),
            "transaction_timestamp": (day2_start_time + timedelta(seconds=random.randint(0, 86400))).strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": random.randint(1, 4),
            "unit_price": round(price * random.uniform(0.98, 1.02), 2),
            "discount": random.choice([0.0, 0.05, 0.10]),
            "payment_method": random.choice(PAYMENT_METHODS_STANDARD),
        })

    # 2. Day-2 Customer Updates (for SCD Type 2 changes)
    customer_updates = []
    sampled_customers = random.sample(baseline_customers, customer_updates_count)
    for cust in sampled_customers:
        updated_cust = cust.copy()
        # Upgrade segment or relocate city
        current_segment = cust["customer_segment"]
        new_segment = "VIP" if current_segment == "Premium" else "Premium" if current_segment == "Standard" else "Corporate"
        new_city = random.choice(CITIES_BY_STATE)
        updated_cust["customer_segment"] = new_segment
        updated_cust["city"] = new_city[0]
        updated_cust["state"] = new_city[1]
        updated_cust["effective_update_date"] = "2026-01-01"
        customer_updates.append(updated_cust)

    day2_txn_path = os.path.join(RAW_DATA_DIR, "transactions_day2.csv")
    day2_cust_path = os.path.join(RAW_DATA_DIR, "customers_updates_day2.csv")
    
    save_csv(
        day2_txn_path,
        day2_txns,
        ["transaction_id", "customer_id", "product_id", "store_id", "transaction_timestamp", "quantity", "unit_price", "discount", "payment_method"]
    )
    save_csv(
        day2_cust_path,
        customer_updates,
        ["customer_id", "customer_name", "email", "city", "state", "signup_date", "customer_segment", "effective_update_date"]
    )


def main():
    """Main generation execution."""
    print("=" * 70)
    print("Retail Data Engineering Pipeline — Synthetic Data Generation (Phase 2)")
    print(f"Deterministic Random Seed: {RANDOM_SEED}")
    print("=" * 70)

    ensure_dir(RAW_DATA_DIR)

    # 1. Stores
    stores = generate_stores(STORE_COUNT)
    save_csv(
        os.path.join(RAW_DATA_DIR, "stores.csv"),
        stores,
        ["store_id", "store_name", "city", "state", "region", "store_type", "opened_date"]
    )

    # 2. Products
    products = generate_products(PRODUCT_COUNT)
    save_csv(
        os.path.join(RAW_DATA_DIR, "products.csv"),
        products,
        ["product_id", "product_name", "category", "subcategory", "brand", "unit_cost", "recommended_price"]
    )

    # 3. Customers
    customers = generate_customers(CUSTOMER_COUNT)
    save_csv(
        os.path.join(RAW_DATA_DIR, "customers.csv"),
        customers,
        ["customer_id", "customer_name", "email", "city", "state", "signup_date", "customer_segment"]
    )

    # 4. Transactions (with controlled defects)
    transactions, defects = generate_transactions(
        TRANSACTION_COUNT,
        customers=customers,
        products=products,
        stores=stores,
        inject_defects=True
    )
    save_csv(
        os.path.join(RAW_DATA_DIR, "transactions.csv"),
        transactions,
        ["transaction_id", "customer_id", "product_id", "store_id", "transaction_timestamp", "quantity", "unit_price", "discount", "payment_method"]
    )

    # 5. Day-2 Incremental Batch (for Phase 7 Delta MERGE & Phase 8 SCD Type 2)
    generate_day2_incremental(customers, products, stores)

    total_defects = sum(defects.values())
    print("\n" + "=" * 70)
    print("DATASET GENERATION SUMMARY & DEFECT AUDIT LOG:")
    print("=" * 70)
    print(f"Total Transactions Generated: {len(transactions):,}")
    print(f"Total Unique Customers:       {len(customers):,}")
    print(f"Total Catalog Products:       {len(products):,}")
    print(f"Total Retail Stores:          {len(stores):,}")
    print("-" * 70)
    print("Controlled Injected Defects (for Phase 5 Data Quality Layer):")
    for defect_type, count in defects.items():
        print(f"  * {defect_type.replace('_', ' ').title():<32}: {count:>6,} rows")
    print(f"  * Total Defective / Anomaly Rows: {total_defects:>6,} rows (~{(total_defects / len(transactions)) * 100:.2f}%)")
    print("=" * 70)
    print("Phase 2 data generation completed successfully.\n")


if __name__ == "__main__":
    main()
