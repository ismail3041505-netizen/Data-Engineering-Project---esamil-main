"""
Web Scraper for Books.toscrape.com
ETL Project - TTTC3213

This script extracts book data from the books.toscrape.com website.
Attributes scraped: Title, Price, Rating, Availability, Category, UPC, Description, Image URL
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from urllib.parse import urljoin

# Base URL
BASE_URL = "https://books.toscrape.com/"
CATALOGUE_URL = "https://books.toscrape.com/catalogue/"

def get_soup(url):
    """
    Fetch a webpage and return a BeautifulSoup object.
    Includes error handling and rate limiting.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def convert_rating_to_number(rating_class):
    """
    Convert rating class name to numeric value.
    Example: 'Three' -> 3
    """
    rating_map = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    return rating_map.get(rating_class, 0)

def get_book_details(book_url):
    """
    Scrape detailed information from a book's individual page.
    Returns: category, upc, description
    """
    soup = get_soup(book_url)
    if not soup:
        return None, None, None
    
    # Get category from breadcrumb
    breadcrumb = soup.find('ul', class_='breadcrumb')
    category = None
    if breadcrumb:
        links = breadcrumb.find_all('a')
        if len(links) >= 3:
            category = links[2].text.strip()
    
    # Get UPC from product information table
    upc = None
    table = soup.find('table', class_='table-striped')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td and th.text.strip() == 'UPC':
                upc = td.text.strip()
                break
    
    # Get description
    description = None
    desc_div = soup.find('div', id='product_description')
    if desc_div:
        desc_p = desc_div.find_next_sibling('p')
        if desc_p:
            description = desc_p.text.strip()
    
    return category, upc, description

def scrape_books(max_pages=15):
    """
    Main scraping function.
    Scrapes book data from multiple pages of the catalogue.
    
    Args:
        max_pages: Maximum number of pages to scrape (default 15 = 300 books)
    
    Returns:
        List of dictionaries containing book data
    """
    books = []
    
    for page_num in range(1, max_pages + 1):
        # Construct page URL
        if page_num == 1:
            page_url = f"{CATALOGUE_URL}page-1.html"
        else:
            page_url = f"{CATALOGUE_URL}page-{page_num}.html"
        
        print(f"Scraping page {page_num}: {page_url}")
        
        soup = get_soup(page_url)
        if not soup:
            print(f"Failed to fetch page {page_num}")
            continue
        
        # Find all book articles on the page
        book_articles = soup.find_all('article', class_='product_pod')
        
        for article in book_articles:
            try:
                # Get title
                title_tag = article.find('h3').find('a')
                title = title_tag['title']
                
                # Get book detail page URL
                book_relative_url = title_tag['href']
                book_url = urljoin(page_url, book_relative_url)
                
                # Get price
                price_tag = article.find('p', class_='price_color')
                price = price_tag.text.strip() if price_tag else None
                
                # Get rating
                rating_tag = article.find('p', class_='star-rating')
                rating_class = None
                if rating_tag:
                    classes = rating_tag.get('class', [])
                    for cls in classes:
                        if cls != 'star-rating':
                            rating_class = cls
                            break
                rating = convert_rating_to_number(rating_class)
                
                # Get availability
                availability_tag = article.find('p', class_='instock')
                availability = availability_tag.text.strip() if availability_tag else None
                
                # Get image URL
                img_tag = article.find('img')
                image_url = None
                if img_tag:
                    img_src = img_tag.get('src', '')
                    image_url = urljoin(BASE_URL, img_src)
                
                # Get detailed information from book page
                category, upc, description = get_book_details(book_url)
                
                # Create book record
                book = {
                    'title': title,
                    'price': price,
                    'rating': rating,
                    'availability': availability,
                    'category': category,
                    'upc': upc,
                    'description': description,
                    'image_url': image_url,
                    'book_url': book_url
                }
                
                books.append(book)
                print(f"  Scraped: {title[:50]}...")
                
                # Rate limiting - be respectful to the server
                time.sleep(0.2)
                
            except Exception as e:
                print(f"Error scraping book: {e}")
                continue
        
        # Rate limiting between pages
        time.sleep(0.5)
    
    return books

def save_to_csv(books, filename):
    """
    Save scraped books to a CSV file.
    """
    df = pd.DataFrame(books)
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"\nSaved {len(books)} books to {filename}")
    return df

def main():
    """
    Main execution function.
    """
    print("=" * 60)
    print("Books.toscrape.com Web Scraper")
    print("ETL Project - TTTC3213")
    print("=" * 60)
    print()
    
    # Scrape books (15 pages = 300 books max)
    print("Starting data extraction...")
    books = scrape_books(max_pages=15)
    
    print(f"\nTotal books scraped: {len(books)}")
    
    # Save raw data
    output_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw_books.csv')
    output_path = os.path.abspath(output_path)
    
    df = save_to_csv(books, output_path)
    
    # Display sample
    print("\nSample of scraped data:")
    print(df.head())
    
    print("\nData columns:")
    print(df.columns.tolist())
    
    print("\nData types:")
    print(df.dtypes)
    
    return df

if __name__ == "__main__":
    main()
