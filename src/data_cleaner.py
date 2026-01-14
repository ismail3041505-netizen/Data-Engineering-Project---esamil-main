"""
Data Cleaner for Books.toscrape.com ETL Project
TTTC3213

This script performs data cleaning and transformation on the scraped book data.
Data Processing Types:
1. Price Conversion - Remove '£' symbol, convert to float
2. Availability Extraction - Parse stock quantity from text
3. Missing Value Handling - Fill/remove missing descriptions
4. Category Standardization - Clean category names
5. Text Cleaning - Remove extra whitespace and special characters
"""

import pandas as pd
import numpy as np
import re
import os

def load_raw_data(filepath):
    """Load raw scraped data from CSV."""
    print(f"Loading raw data from {filepath}")
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} records")
    return df

def clean_price(price_str):
    """
    Data Processing Type 1: Price Conversion
    Remove currency symbol and convert to float.
    Example: '£51.77' -> 51.77
    """
    if pd.isna(price_str):
        return np.nan
    # Remove currency symbol (£) and any whitespace
    cleaned = re.sub(r'[£$€]', '', str(price_str).strip())
    try:
        return float(cleaned)
    except ValueError:
        return np.nan

def extract_stock_quantity(availability_str):
    """
    Data Processing Type 2: Availability Extraction
    Extract numeric stock quantity from availability text.
    Example: 'In stock (22 available)' -> 22
    """
    if pd.isna(availability_str):
        return 0
    # Find numbers in the text
    match = re.search(r'\((\d+)\s*available\)', str(availability_str))
    if match:
        return int(match.group(1))
    # If "In stock" but no number, assume at least 1
    if 'in stock' in str(availability_str).lower():
        return 1
    return 0

def handle_missing_descriptions(df):
    """
    Data Processing Type 3: Missing Value Handling
    Fill missing descriptions with a placeholder or use title-based description.
    """
    # Count missing before
    missing_count = df['description'].isna().sum()
    print(f"  - Missing descriptions before: {missing_count}")
    
    # Fill missing descriptions with a generated message
    df['description'] = df.apply(
        lambda row: f"No description available for '{row['title']}'." 
        if pd.isna(row['description']) or str(row['description']).strip() == '' 
        else row['description'],
        axis=1
    )
    
    # Count missing after
    missing_after = df['description'].isna().sum()
    print(f"  - Missing descriptions after: {missing_after}")
    
    return df

def standardize_category(category_str):
    """
    Data Processing Type 4: Category Standardization
    Clean and standardize category names.
    - Remove extra whitespace
    - Capitalize properly
    - Handle edge cases
    """
    if pd.isna(category_str):
        return 'Unknown'
    
    # Clean whitespace
    cleaned = ' '.join(str(category_str).split())
    
    # Remove special characters but keep spaces and hyphens
    cleaned = re.sub(r'[^\w\s\-]', '', cleaned)
    
    # Title case
    cleaned = cleaned.title()
    
    return cleaned if cleaned else 'Unknown'

def clean_text(text):
    """
    Data Processing Type 5: Text Cleaning
    Remove extra whitespace and normalize text.
    """
    if pd.isna(text):
        return text
    # Remove extra whitespace, newlines, tabs
    cleaned = ' '.join(str(text).split())
    return cleaned

def create_price_category(price):
    """
    Additional Processing: Create price range categories for analysis.
    """
    if pd.isna(price):
        return 'Unknown'
    elif price < 20:
        return 'Budget (Under £20)'
    elif price < 35:
        return 'Mid-range (£20-£35)'
    elif price < 50:
        return 'Premium (£35-£50)'
    else:
        return 'Luxury (Over £50)'

def create_rating_category(rating):
    """
    Additional Processing: Create rating categories for analysis.
    """
    if pd.isna(rating) or rating == 0:
        return 'No Rating'
    elif rating <= 2:
        return 'Low (1-2 stars)'
    elif rating <= 3:
        return 'Medium (3 stars)'
    else:
        return 'High (4-5 stars)'

def calculate_value_score(row):
    """
    Calculate a value score (rating per pound spent).
    Higher score = better value
    """
    if pd.isna(row['price_clean']) or row['price_clean'] <= 0:
        return 0
    if pd.isna(row['rating']) or row['rating'] == 0:
        return 0
    # Value score: rating / (price / 10)
    # Normalized so a 5-star book at £10 gets score of 5
    return round(row['rating'] / (row['price_clean'] / 10), 2)

def clean_data(df):
    """
    Main data cleaning function.
    Applies all data processing transformations.
    """
    print("\n" + "=" * 60)
    print("Starting Data Cleaning Process")
    print("=" * 60)
    
    # Create a copy to preserve original
    df_clean = df.copy()
    
    # Store original data summary for comparison
    original_summary = {
        'total_records': len(df),
        'missing_prices': df['price'].isna().sum(),
        'missing_descriptions': df['description'].isna().sum(),
        'missing_categories': df['category'].isna().sum(),
    }
    
    print(f"\nOriginal Data Summary:")
    print(f"  - Total records: {original_summary['total_records']}")
    print(f"  - Missing prices: {original_summary['missing_prices']}")
    print(f"  - Missing descriptions: {original_summary['missing_descriptions']}")
    print(f"  - Missing categories: {original_summary['missing_categories']}")
    
    # ========================================
    # Data Processing Type 1: Price Conversion
    # ========================================
    print("\n[1/5] Processing prices...")
    df_clean['price_clean'] = df_clean['price'].apply(clean_price)
    print(f"  - Converted {df_clean['price_clean'].notna().sum()} prices to numeric")
    print(f"  - Price range: £{df_clean['price_clean'].min():.2f} - £{df_clean['price_clean'].max():.2f}")
    
    # ========================================
    # Data Processing Type 2: Availability Extraction
    # ========================================
    print("\n[2/5] Extracting stock quantities...")
    df_clean['stock_quantity'] = df_clean['availability'].apply(extract_stock_quantity)
    print(f"  - Total stock across all books: {df_clean['stock_quantity'].sum()}")
    print(f"  - Average stock per book: {df_clean['stock_quantity'].mean():.1f}")
    
    # Create in_stock boolean column
    df_clean['in_stock'] = df_clean['stock_quantity'] > 0
    print(f"  - Books in stock: {df_clean['in_stock'].sum()}")
    
    # ========================================
    # Data Processing Type 3: Missing Value Handling
    # ========================================
    print("\n[3/5] Handling missing values...")
    df_clean = handle_missing_descriptions(df_clean)
    
    # ========================================
    # Data Processing Type 4: Category Standardization
    # ========================================
    print("\n[4/5] Standardizing categories...")
    df_clean['category_clean'] = df_clean['category'].apply(standardize_category)
    unique_categories = df_clean['category_clean'].nunique()
    print(f"  - Unique categories: {unique_categories}")
    print(f"  - Top 5 categories: {df_clean['category_clean'].value_counts().head().to_dict()}")
    
    # ========================================
    # Data Processing Type 5: Text Cleaning
    # ========================================
    print("\n[5/5] Cleaning text fields...")
    df_clean['title_clean'] = df_clean['title'].apply(clean_text)
    df_clean['description_clean'] = df_clean['description'].apply(clean_text)
    print(f"  - Cleaned {len(df_clean)} titles and descriptions")
    
    # ========================================
    # Additional Processing: Create categorical columns
    # ========================================
    print("\n[Bonus] Creating analytical categories...")
    df_clean['price_category'] = df_clean['price_clean'].apply(create_price_category)
    df_clean['rating_category'] = df_clean['rating'].apply(create_rating_category)
    df_clean['value_score'] = df_clean.apply(calculate_value_score, axis=1)
    print(f"  - Created price categories, rating categories, and value scores")
    
    # ========================================
    # Summary
    # ========================================
    print("\n" + "=" * 60)
    print("Data Cleaning Complete!")
    print("=" * 60)
    
    cleaned_summary = {
        'total_records': len(df_clean),
        'missing_prices_clean': df_clean['price_clean'].isna().sum(),
        'missing_descriptions_clean': df_clean['description_clean'].isna().sum(),
        'missing_categories_clean': (df_clean['category_clean'] == 'Unknown').sum(),
    }
    
    print(f"\nCleaned Data Summary:")
    print(f"  - Total records: {cleaned_summary['total_records']}")
    print(f"  - Missing prices: {cleaned_summary['missing_prices_clean']}")
    print(f"  - Missing descriptions: {cleaned_summary['missing_descriptions_clean']}")
    print(f"  - Unknown categories: {cleaned_summary['missing_categories_clean']}")
    
    return df_clean, original_summary, cleaned_summary

def save_cleaned_data(df, filepath):
    """Save cleaned data to CSV."""
    # Select and reorder columns for the final output
    output_columns = [
        'title_clean',
        'category_clean',
        'price_clean',
        'rating',
        'rating_category',
        'price_category',
        'value_score',
        'stock_quantity',
        'in_stock',
        'description_clean',
        'upc',
        'image_url',
        'book_url',
        # Keep original columns for reference
        'title',
        'price',
        'availability',
        'category',
        'description'
    ]
    
    # Only include columns that exist
    output_columns = [col for col in output_columns if col in df.columns]
    
    df_output = df[output_columns]
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    df_output.to_csv(filepath, index=False, encoding='utf-8')
    print(f"\nSaved cleaned data to {filepath}")
    print(f"Output columns: {len(output_columns)}")
    
    return df_output

def main():
    """Main execution function."""
    print("=" * 60)
    print("Data Cleaning - Books.toscrape.com ETL Project")
    print("TTTC3213")
    print("=" * 60)
    
    # Paths
    script_dir = os.path.dirname(__file__)
    raw_data_path = os.path.join(script_dir, '..', 'data', 'raw_books.csv')
    cleaned_data_path = os.path.join(script_dir, '..', 'data', 'cleaned_books.csv')
    
    raw_data_path = os.path.abspath(raw_data_path)
    cleaned_data_path = os.path.abspath(cleaned_data_path)
    
    # Load data
    df_raw = load_raw_data(raw_data_path)
    
    # Clean data
    df_clean, original_summary, cleaned_summary = clean_data(df_raw)
    
    # Save cleaned data
    df_output = save_cleaned_data(df_clean, cleaned_data_path)
    
    # Display sample
    print("\nSample of cleaned data:")
    print(df_output[['title_clean', 'category_clean', 'price_clean', 'rating', 'value_score']].head(10))
    
    return df_clean, original_summary, cleaned_summary

if __name__ == "__main__":
    main()
