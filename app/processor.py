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

def normalize_columns(df):
    """Lowercase and strip all column names"""
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    return df

def validate_columns(df):
    """Check all required columns are present"""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True

def clean_data(df):
    """Clean and type cast all columns"""
    # Drop completely empty rows
    df = df.dropna(how='all')

    # Clean string columns
    if 'product_name' in df.columns:
        df['product_name'] = df['product_name'].str.lower().str.strip()
    if 'category' in df.columns:
        df['category'] = df['category'].str.lower().str.strip()

    # Convert numeric columns
    numeric_cols = ['quantity_sold', 'selling_price', 
                    'purchase_price', 'opening_stock', 'closing_stock']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert date column
    df['sale_date'] = pd.to_datetime(df['sale_date'], 
                                      dayfirst=True, 
                                      errors='coerce')

    # Drop rows with missing critical values
    df = df.dropna(subset=['product_name', 'quantity_sold', 
                            'selling_price', 'sale_date'])

    # Drop rows with zero or negative quantity
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

def process_file(file_path, store_id):
    """Main function — read, clean, save one file"""
    print(f"Processing file: {file_path}")

    df = read_file(file_path)
    df = normalize_columns(df)
    
    # DEBUG — print actual columns received
    print(f"Columns found: {list(df.columns)}")
    print(f"First row: {df.head(1).to_dict()}")
    
    validate_columns(df)
    df = clean_data(df)

    print(f"Rows after cleaning: {len(df)}")

    success, failed = save_to_db(df, store_id)

    print(f"Saved: {success} rows, Failed: {failed} rows")

    return {
        "rows_processed": success,
        "rows_failed": failed,
        "status": "success" if failed == 0 else "partial"
    }