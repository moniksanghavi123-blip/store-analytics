import pandas as pd
import os
from app.database import run_query

REQUIRED_COLUMNS = [
    'product_name',
    'quantity_sold',
    'selling_price',
    'purchase_price',
    'sale_date'
]

OPTIONAL_COLUMNS = [
    'category',
    'opening_stock',
    'closing_stock'
]

COLUMN_ALIASES = {
    'product': 'product_name',
    'productname': 'product_name',
    'qty': 'quantity_sold',
    'quantity': 'quantity_sold',
    'units': 'quantity_sold',
    'sellingprice': 'selling_price',
    'saleprice': 'selling_price',
    'mrp': 'selling_price',
    'buying_price': 'purchase_price',
    'cost_price': 'purchase_price',
    'purchased_price': 'purchase_price',
    'date': 'sale_date',
    'sold_on': 'sale_date',
}

def read_file(file_path):
    """Read Excel or CSV file into a pandas dataframe"""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    return df

def normalize_columns(df, column_mapping=None):
    """Lowercase and strip all column names"""
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    if column_mapping:
        normalized_map = {
            str(k).lower().strip().replace(' ', '_'):
            str(v).lower().strip().replace(' ', '_')
            for k, v in column_mapping.items()
            if k and v
        }
        df = df.rename(columns=normalized_map)
    df = df.rename(columns={
        col: COLUMN_ALIASES.get(col, col)
        for col in df.columns
    })
    return df

def validate_columns(df):
    """Check all required columns are present"""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns: "
            f"{', '.join(missing)}. "
            "Required columns are: "
            f"{', '.join(REQUIRED_COLUMNS)}."
        )
    return True

def clean_data(df):
    """Clean and type cast all columns"""
    df = df.dropna(how='all')

    # Clean string columns
    if 'product_name' in df.columns:
        df['product_name'] = df['product_name'].str.lower().str.strip()

    # Fix category — use product_name based mapping if missing
    CATEGORY_MAP = {
        'butter': 'dairy', 'milk': 'dairy', 'cheese': 'dairy',
        'parle': 'biscuits', 'britannia': 'biscuits', 'biscuit': 'biscuits',
        'bread': 'bakery', 'cake': 'bakery',
        'maggi': 'food', 'atta': 'food', 'oil': 'food', 'rice': 'food',
        'tea': 'beverages', 'coffee': 'beverages', 'frooti': 'beverages',
        'surf': 'household', 'vim': 'household', 'lizol': 'household',
        'dettol': 'personal', 'colgate': 'personal', 'shampoo': 'personal',
        'chips': 'snacks', 'namkeen': 'snacks', 'lays': 'snacks',
        'dairy milk': 'chocolate', 'chocolate': 'chocolate',
        'gold': 'jewellery', 'ring': 'jewellery', 'necklace': 'jewellery',
        'shirt': 'clothing', 'jeans': 'clothing', 'dress': 'clothing',
        'shoe': 'footwear', 'sandal': 'footwear', 'chappal': 'footwear',
    }

    if 'category' in df.columns:
        df['category'] = df['category'].str.lower().str.strip()
        # Fill missing category based on product name
        def guess_category(row):
            if pd.notna(row.get('category')) and row.get('category') != '':
                return row['category']
            product = str(row.get('product_name', '')).lower()
            for keyword, cat in CATEGORY_MAP.items():
                if keyword in product:
                    return cat
            return 'general'
        df['category'] = df.apply(guess_category, axis=1)
    else:
        # No category column — guess from product name
        def guess_from_product(product):
            product = str(product).lower()
            for keyword, cat in CATEGORY_MAP.items():
                if keyword in product:
                    return cat
            return 'general'
        df['category'] = df['product_name'].apply(guess_from_product)

    # Convert numeric columns
    numeric_cols = ['quantity_sold', 'selling_price',
                    'purchase_price', 'opening_stock', 'closing_stock']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert date
    df['sale_date'] = pd.to_datetime(
        df['sale_date'], dayfirst=True, errors='coerce'
    )

    # Drop invalid rows
    df = df.dropna(subset=['product_name', 'quantity_sold',
                            'selling_price', 'sale_date'])
    df = df[df['quantity_sold'] > 0]

    return df

def save_to_db(df, store_id):
    """Save cleaned dataframe rows to sales_raw table"""
    success_count = 0
    fail_count = 0

    for _, row in df.iterrows():
        try:
            run_query('''
                insert into sales_raw (
                    store_id, sale_date, product_name, category,
                    quantity_sold, selling_price, purchase_price,
                    opening_stock, closing_stock
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                store_id,
                row['sale_date'].date(),
                row['product_name'],
                row.get('category', None),
                row['quantity_sold'],
                row['selling_price'],
                row.get('purchase_price', 0),
                row.get('opening_stock', None),
                row.get('closing_stock', None)
            ), fetch=False)
            success_count += 1
        except Exception as e:
            print(f"Row failed: {e}")
            fail_count += 1

    return success_count, fail_count

def process_file(file_path, store_id, column_mapping=None):
    """Main function — read, clean, save one file"""
    print(f"Processing file: {file_path}")

    df = read_file(file_path)
    if df.empty:
        raise ValueError("Uploaded file is empty. Please add data rows and try again.")

    df = normalize_columns(df, column_mapping=column_mapping)
    
    # DEBUG — print actual columns received
    print(f"Columns found: {list(df.columns)}")
    print(f"First row: {df.head(1).to_dict()}")
    
    validate_columns(df)
    total_rows = len(df)
    df = clean_data(df)

    print(f"Rows after cleaning: {len(df)}")

    if df.empty:
        raise ValueError(
            "No valid rows found after cleaning. "
            "Check dates, quantities, prices, and required columns."
        )

    success, failed = save_to_db(df, store_id)

    print(f"Saved: {success} rows, Failed: {failed} rows")

    return {
        "rows_received": total_rows,
        "rows_processed": success,
        "rows_failed": failed,
        "status": "success" if failed == 0 else "partial"
    }