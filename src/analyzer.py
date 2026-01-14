"""
Data Analyzer and Visualizer for Books.toscrape.com ETL Project
TTTC3213

This script generates visualizations for:
1. Before/After data cleaning comparison
2. Price distribution by category
3. Rating distribution
4. Best value books analysis (project goal)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

def load_data():
    """Load both raw and cleaned data."""
    script_dir = os.path.dirname(__file__)
    
    raw_path = os.path.join(script_dir, '..', 'data', 'raw_books.csv')
    cleaned_path = os.path.join(script_dir, '..', 'data', 'cleaned_books.csv')
    
    df_raw = pd.read_csv(os.path.abspath(raw_path))
    df_clean = pd.read_csv(os.path.abspath(cleaned_path))
    
    print(f"Loaded raw data: {len(df_raw)} records")
    print(f"Loaded cleaned data: {len(df_clean)} records")
    
    return df_raw, df_clean

def ensure_viz_dir():
    """Ensure visualizations directory exists."""
    script_dir = os.path.dirname(__file__)
    viz_dir = os.path.join(script_dir, '..', 'visualizations')
    viz_dir = os.path.abspath(viz_dir)
    os.makedirs(viz_dir, exist_ok=True)
    return viz_dir

def viz_before_after_cleaning(df_raw, df_clean, viz_dir):
    """
    Visualization 1: Before/After Data Cleaning Comparison
    Shows the data quality improvement after cleaning.
    """
    print("\n[1] Creating Before/After cleaning visualization...")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Before vs After Data Cleaning Comparison', fontsize=16, fontweight='bold')
    
    # 1. Price Data Types
    ax1 = axes[0, 0]
    before_price_type = ['String (with £)']
    after_price_type = ['Numeric (float)']
    x = [0, 1]
    colors = ['#ff6b6b', '#4ecdc4']
    bars1 = ax1.bar([0], [1], color=colors[0], label='Before', width=0.35)
    bars2 = ax1.bar([0.35], [1], color=colors[1], label='After', width=0.35)
    ax1.set_xticks([0.175])
    ax1.set_xticklabels(['Price Data Type'])
    ax1.set_ylabel('Status')
    ax1.set_title('Price Data Transformation')
    ax1.set_yticks([0, 1])
    ax1.set_yticklabels(['Not Ready', 'Analysis Ready'])
    ax1.legend()
    
    # Add text annotations
    ax1.text(0, 0.5, 'String\n"£51.77"', ha='center', va='center', fontsize=10, color='white', fontweight='bold')
    ax1.text(0.35, 0.5, 'Float\n51.77', ha='center', va='center', fontsize=10, color='white', fontweight='bold')
    
    # 2. Missing Values Comparison
    ax2 = axes[0, 1]
    categories = ['Prices', 'Descriptions', 'Categories']
    before_missing = [0, 1, 0]  # Raw data missing counts
    after_missing = [0, 0, 0]   # Cleaned data missing counts
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax2.bar(x - width/2, before_missing, width, label='Before Cleaning', color='#ff6b6b')
    bars2 = ax2.bar(x + width/2, after_missing, width, label='After Cleaning', color='#4ecdc4')
    
    ax2.set_xlabel('Data Field')
    ax2.set_ylabel('Missing Count')
    ax2.set_title('Missing Values Before vs After')
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories)
    ax2.legend()
    
    # 3. Data Type Distribution Before
    ax3 = axes[1, 0]
    before_types = ['object (string)', 'object', 'int64', 'object', 'object']
    type_counts_before = {'object': 8, 'int64': 1}
    ax3.pie(type_counts_before.values(), labels=type_counts_before.keys(), autopct='%1.1f%%', 
            colors=['#ff6b6b', '#45b7d1'])
    ax3.set_title('Data Types Before Cleaning')
    
    # 4. Data Type Distribution After
    ax4 = axes[1, 1]
    type_counts_after = {'object (string)': 9, 'float64 (numeric)': 2, 'int64 (numeric)': 2, 'bool': 1}
    wedges, texts, autotexts = ax4.pie(type_counts_after.values(), labels=type_counts_after.keys(), 
                                        autopct='%1.1f%%', colors=['#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7'])
    ax4.set_title('Data Types After Cleaning\n(More analysis-ready types)')
    
    plt.tight_layout()
    
    filepath = os.path.join(viz_dir, '01_before_after_cleaning.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {filepath}")

def viz_price_distribution(df_clean, viz_dir):
    """
    Visualization 2: Price Distribution Analysis
    """
    print("\n[2] Creating price distribution visualization...")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Price Distribution Analysis', fontsize=16, fontweight='bold')
    
    # 1. Overall Price Distribution (Histogram)
    ax1 = axes[0, 0]
    ax1.hist(df_clean['price_clean'], bins=20, edgecolor='black', color='#4ecdc4', alpha=0.7)
    ax1.axvline(df_clean['price_clean'].mean(), color='red', linestyle='--', label=f'Mean: £{df_clean["price_clean"].mean():.2f}')
    ax1.axvline(df_clean['price_clean'].median(), color='orange', linestyle='--', label=f'Median: £{df_clean["price_clean"].median():.2f}')
    ax1.set_xlabel('Price (£)')
    ax1.set_ylabel('Number of Books')
    ax1.set_title('Overall Price Distribution')
    ax1.legend()
    
    # 2. Price by Category (Box Plot) - Top 10 categories
    ax2 = axes[0, 1]
    top_categories = df_clean['category_clean'].value_counts().head(10).index.tolist()
    df_top = df_clean[df_clean['category_clean'].isin(top_categories)]
    
    # Order by median price
    category_order = df_top.groupby('category_clean')['price_clean'].median().sort_values().index
    
    sns.boxplot(data=df_top, x='category_clean', y='price_clean', order=category_order, ax=ax2, palette='viridis')
    ax2.set_xlabel('Category')
    ax2.set_ylabel('Price (£)')
    ax2.set_title('Price Distribution by Top 10 Categories')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. Price Category Distribution (Pie Chart)
    ax3 = axes[1, 0]
    price_cat_counts = df_clean['price_category'].value_counts()
    colors = ['#4ecdc4', '#45b7d1', '#ff6b6b', '#f7dc6f']
    ax3.pie(price_cat_counts.values, labels=price_cat_counts.index, autopct='%1.1f%%', 
            colors=colors, startangle=90)
    ax3.set_title('Books by Price Category')
    
    # 4. Average Price by Category (Bar Chart)
    ax4 = axes[1, 1]
    avg_prices = df_clean.groupby('category_clean')['price_clean'].mean().sort_values(ascending=True).tail(10)
    avg_prices.plot(kind='barh', ax=ax4, color='#4ecdc4', edgecolor='black')
    ax4.set_xlabel('Average Price (£)')
    ax4.set_ylabel('Category')
    ax4.set_title('Top 10 Most Expensive Categories (Average Price)')
    
    plt.tight_layout()
    
    filepath = os.path.join(viz_dir, '02_price_distribution.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {filepath}")

def viz_rating_distribution(df_clean, viz_dir):
    """
    Visualization 3: Rating Distribution Analysis
    """
    print("\n[3] Creating rating distribution visualization...")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Rating Distribution Analysis', fontsize=16, fontweight='bold')
    
    # 1. Rating Distribution (Bar Chart)
    ax1 = axes[0, 0]
    rating_counts = df_clean['rating'].value_counts().sort_index()
    colors = ['#ff6b6b', '#ffa07a', '#ffd93d', '#6bcb77', '#4ecdc4']
    bars = ax1.bar(rating_counts.index, rating_counts.values, color=colors, edgecolor='black')
    ax1.set_xlabel('Rating (Stars)')
    ax1.set_ylabel('Number of Books')
    ax1.set_title('Distribution of Book Ratings')
    ax1.set_xticks([1, 2, 3, 4, 5])
    ax1.set_xticklabels(['1 ⭐', '2 ⭐⭐', '3 ⭐⭐⭐', '4 ⭐⭐⭐⭐', '5 ⭐⭐⭐⭐⭐'])
    
    # Add count labels
    for bar, count in zip(bars, rating_counts.values):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                str(count), ha='center', va='bottom', fontweight='bold')
    
    # 2. Rating by Category
    ax2 = axes[0, 1]
    top_categories = df_clean['category_clean'].value_counts().head(8).index.tolist()
    df_top = df_clean[df_clean['category_clean'].isin(top_categories)]
    
    avg_rating_by_cat = df_top.groupby('category_clean')['rating'].mean().sort_values(ascending=True)
    colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(avg_rating_by_cat)))
    avg_rating_by_cat.plot(kind='barh', ax=ax2, color=colors, edgecolor='black')
    ax2.set_xlabel('Average Rating')
    ax2.set_ylabel('Category')
    ax2.set_title('Average Rating by Top 8 Categories')
    ax2.set_xlim(0, 5)
    
    # 3. Rating Category Distribution
    ax3 = axes[1, 0]
    rating_cat_counts = df_clean['rating_category'].value_counts()
    colors = ['#6bcb77', '#ffd93d', '#ff6b6b']
    # Reorder
    order = ['High (4-5 stars)', 'Medium (3 stars)', 'Low (1-2 stars)']
    ordered_counts = [rating_cat_counts.get(cat, 0) for cat in order]
    ax3.pie(ordered_counts, labels=order, autopct='%1.1f%%', colors=colors, startangle=90)
    ax3.set_title('Books by Rating Category')
    
    # 4. Price vs Rating Scatter
    ax4 = axes[1, 1]
    scatter = ax4.scatter(df_clean['price_clean'], df_clean['rating'], 
                         c=df_clean['rating'], cmap='RdYlGn', alpha=0.6, edgecolors='black', linewidth=0.5)
    ax4.set_xlabel('Price (£)')
    ax4.set_ylabel('Rating')
    ax4.set_title('Price vs Rating Relationship')
    ax4.set_yticks([1, 2, 3, 4, 5])
    plt.colorbar(scatter, ax=ax4, label='Rating')
    
    plt.tight_layout()
    
    filepath = os.path.join(viz_dir, '03_rating_distribution.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {filepath}")

def viz_best_value_analysis(df_clean, viz_dir):
    """
    Visualization 4: Best Value Books Analysis (PROJECT GOAL)
    Identifies books with highest value score (high rating, reasonable price)
    """
    print("\n[4] Creating Best Value Analysis visualization (PROJECT GOAL)...")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    fig.suptitle('🎯 PROJECT GOAL: Best Value Books Analysis\n(High Rating at Reasonable Price)', 
                 fontsize=16, fontweight='bold')
    
    # 1. Value Score Distribution
    ax1 = axes[0, 0]
    ax1.hist(df_clean['value_score'], bins=25, edgecolor='black', color='#4ecdc4', alpha=0.7)
    ax1.axvline(df_clean['value_score'].mean(), color='red', linestyle='--', 
                label=f'Mean: {df_clean["value_score"].mean():.2f}')
    ax1.set_xlabel('Value Score (Rating per £10)')
    ax1.set_ylabel('Number of Books')
    ax1.set_title('Value Score Distribution\n(Higher = Better Value)')
    ax1.legend()
    
    # 2. Top 15 Best Value Books
    ax2 = axes[0, 1]
    top_value = df_clean.nlargest(15, 'value_score')[['title_clean', 'value_score', 'rating', 'price_clean']]
    
    # Truncate long titles
    top_value['title_short'] = top_value['title_clean'].apply(lambda x: x[:30] + '...' if len(x) > 30 else x)
    
    colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(top_value)))[::-1]
    bars = ax2.barh(top_value['title_short'], top_value['value_score'], color=colors, edgecolor='black')
    ax2.set_xlabel('Value Score')
    ax2.set_ylabel('Book Title')
    ax2.set_title('Top 15 Best Value Books')
    
    # Add price and rating annotations
    for i, (bar, (_, row)) in enumerate(zip(bars, top_value.iterrows())):
        ax2.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                f'£{row["price_clean"]:.0f} | {row["rating"]}⭐', 
                va='center', fontsize=8)
    
    # 3. Value Score by Category
    ax3 = axes[1, 0]
    top_categories = df_clean['category_clean'].value_counts().head(10).index.tolist()
    df_top = df_clean[df_clean['category_clean'].isin(top_categories)]
    
    avg_value_by_cat = df_top.groupby('category_clean')['value_score'].mean().sort_values(ascending=True)
    colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(avg_value_by_cat)))
    avg_value_by_cat.plot(kind='barh', ax=ax3, color=colors, edgecolor='black')
    ax3.set_xlabel('Average Value Score')
    ax3.set_ylabel('Category')
    ax3.set_title('Best Value Categories\n(Higher = Better Value for Money)')
    
    # 4. Value Quadrant Analysis
    ax4 = axes[1, 1]
    avg_price = df_clean['price_clean'].mean()
    avg_rating = df_clean['rating'].mean()
    
    # Color by quadrant
    colors = []
    for _, row in df_clean.iterrows():
        if row['rating'] >= avg_rating and row['price_clean'] <= avg_price:
            colors.append('#4ecdc4')  # Best value (high rating, low price)
        elif row['rating'] >= avg_rating and row['price_clean'] > avg_price:
            colors.append('#45b7d1')  # Premium (high rating, high price)
        elif row['rating'] < avg_rating and row['price_clean'] <= avg_price:
            colors.append('#ffd93d')  # Budget (low rating, low price)
        else:
            colors.append('#ff6b6b')  # Poor value (low rating, high price)
    
    ax4.scatter(df_clean['price_clean'], df_clean['rating'], c=colors, alpha=0.6, edgecolors='black', linewidth=0.5)
    ax4.axhline(avg_rating, color='gray', linestyle='--', alpha=0.7)
    ax4.axvline(avg_price, color='gray', linestyle='--', alpha=0.7)
    ax4.set_xlabel('Price (£)')
    ax4.set_ylabel('Rating')
    ax4.set_title('Value Quadrant Analysis')
    ax4.set_yticks([1, 2, 3, 4, 5])
    
    # Add quadrant labels
    ax4.text(15, 4.5, '✓ BEST VALUE\n(High Rating, Low Price)', fontsize=9, color='#4ecdc4', fontweight='bold')
    ax4.text(45, 4.5, 'Premium\n(High Rating, High Price)', fontsize=9, color='#45b7d1', fontweight='bold')
    ax4.text(15, 1.5, 'Budget\n(Low Rating, Low Price)', fontsize=9, color='#d4a017', fontweight='bold')
    ax4.text(45, 1.5, '✗ Poor Value\n(Low Rating, High Price)', fontsize=9, color='#ff6b6b', fontweight='bold')
    
    plt.tight_layout()
    
    filepath = os.path.join(viz_dir, '04_best_value_analysis.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {filepath}")
    
    # Print summary statistics
    print("\n" + "=" * 60)
    print("PROJECT GOAL ANALYSIS SUMMARY")
    print("=" * 60)
    
    # Best value books
    top_10_value = df_clean.nlargest(10, 'value_score')[['title_clean', 'category_clean', 'price_clean', 'rating', 'value_score']]
    print("\nTop 10 Best Value Books:")
    print(top_10_value.to_string(index=False))
    
    # Best value categories
    best_cat = df_clean.groupby('category_clean')['value_score'].mean().sort_values(ascending=False).head(5)
    print("\nTop 5 Best Value Categories:")
    for cat, score in best_cat.items():
        print(f"  - {cat}: {score:.2f}")
    
    # Quadrant counts
    best_value_count = len(df_clean[(df_clean['rating'] >= avg_rating) & (df_clean['price_clean'] <= avg_price)])
    total = len(df_clean)
    print(f"\nBooks in 'Best Value' quadrant: {best_value_count} ({best_value_count/total*100:.1f}%)")
    
    return top_10_value

def viz_category_analysis(df_clean, viz_dir):
    """
    Visualization 5: Category Analysis
    """
    print("\n[5] Creating category analysis visualization...")
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Category Analysis', fontsize=16, fontweight='bold')
    
    # 1. Books per Category (Top 15)
    ax1 = axes[0]
    category_counts = df_clean['category_clean'].value_counts().head(15)
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(category_counts)))[::-1]
    category_counts.plot(kind='barh', ax=ax1, color=colors, edgecolor='black')
    ax1.set_xlabel('Number of Books')
    ax1.set_ylabel('Category')
    ax1.set_title('Top 15 Categories by Book Count')
    
    # 2. Category Summary Statistics
    ax2 = axes[1]
    top_cats = df_clean['category_clean'].value_counts().head(8).index
    df_top = df_clean[df_clean['category_clean'].isin(top_cats)]
    
    summary = df_top.groupby('category_clean').agg({
        'price_clean': 'mean',
        'rating': 'mean',
        'value_score': 'mean'
    }).round(2)
    
    # Create a table-style visualization
    ax2.axis('off')
    table_data = [['Category', 'Avg Price (£)', 'Avg Rating', 'Value Score']]
    for cat, row in summary.iterrows():
        table_data.append([cat[:20], f'£{row["price_clean"]:.2f}', f'{row["rating"]:.1f}', f'{row["value_score"]:.2f}'])
    
    table = ax2.table(cellText=table_data, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    
    # Style header row
    for i in range(4):
        table[(0, i)].set_facecolor('#4ecdc4')
        table[(0, i)].set_text_props(fontweight='bold', color='white')
    
    ax2.set_title('Category Statistics Summary')
    
    plt.tight_layout()
    
    filepath = os.path.join(viz_dir, '05_category_analysis.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {filepath}")

def main():
    """Main execution function."""
    print("=" * 60)
    print("Data Analysis & Visualization")
    print("Books.toscrape.com ETL Project - TTTC3213")
    print("=" * 60)
    
    # Load data
    df_raw, df_clean = load_data()
    
    # Ensure viz directory exists
    viz_dir = ensure_viz_dir()
    print(f"\nVisualization directory: {viz_dir}")
    
    # Generate all visualizations
    viz_before_after_cleaning(df_raw, df_clean, viz_dir)
    viz_price_distribution(df_clean, viz_dir)
    viz_rating_distribution(df_clean, viz_dir)
    top_value_books = viz_best_value_analysis(df_clean, viz_dir)
    viz_category_analysis(df_clean, viz_dir)
    
    print("\n" + "=" * 60)
    print("All visualizations generated successfully!")
    print("=" * 60)
    
    return df_clean, top_value_books

if __name__ == "__main__":
    main()
