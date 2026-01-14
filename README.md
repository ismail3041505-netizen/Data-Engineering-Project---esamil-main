# Books ETL Project - TTTC3213

> **Analyzing Book Pricing Trends and Ratings to Identify Best Value Books**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Data Source](https://img.shields.io/badge/Source-books.toscrape.com-green)](https://books.toscrape.com)

## Overview

This ETL (Extract, Transform, Load) project scrapes book data from [books.toscrape.com](https://books.toscrape.com), performs data cleaning and transformation, and produces analysis to identify the best value books - those with high ratings at reasonable prices.

## Key Findings

| Metric | Value |
|--------|-------|
| Books Analyzed | 300 |
| Categories | 40 |
| Best Value Book | Old School (Diary of a Wimpy Kid #10) - £11.83, 5★ |
| Best Value Category | Humor (avg score: 2.35) |
| "Best Value" Quadrant | 26.7% of books |

## Project Structure

```
├── src/
│   ├── scraper.py          # Web scraper (BeautifulSoup + requests)
│   ├── data_cleaner.py     # Data cleaning (5 processing types)
│   └── analyzer.py         # Visualization generator
├── data/
│   ├── raw_books.csv       # Raw scraped data
│   └── cleaned_books.csv   # Cleaned analysis-ready data
├── visualizations/         # Generated charts (5 PNG files)
├── PROJECT_REPORT.md       # Full project report
└── README.md               # This file
```

## Quick Start

```bash
# Install dependencies
pip install requests beautifulsoup4 pandas matplotlib seaborn

# Run full ETL pipeline
python src/scraper.py        # Extract (takes ~2 min)
python src/data_cleaner.py   # Transform
python src/analyzer.py       # Load & Visualize
```

## Data Processing

5 transformation types implemented:
1. **Price Conversion** - String to float (remove £)
2. **Availability Extraction** - Parse stock quantity
3. **Missing Value Handling** - Fill missing descriptions
4. **Category Standardization** - Clean and normalize
5. **Text Cleaning** - Remove extra whitespace

## Visualizations

| File | Description |
|------|-------------|
| `01_before_after_cleaning.png` | Data quality comparison |
| `02_price_distribution.png` | Price analysis by category |
| `03_rating_distribution.png` | Rating patterns |
| `04_best_value_analysis.png` | **Project Goal** - Best value books |
| `05_category_analysis.png` | Category breakdown |

## Report

See [PROJECT_REPORT.md](PROJECT_REPORT.md) for the full analysis report including:
- Data scraping methodology with code snippets
- Data cleaning processes with before/after visuals
- Best value analysis with rankings
- Conclusions and recommendations

---

*TTTC3213 Group Project*
