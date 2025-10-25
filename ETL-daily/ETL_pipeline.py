import requests
import json
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import psycopg2  # For PostgreSQL
import sys

# Load environment variables 
load_dotenv()
BASE_URL = os.getenv('BASE_URL')
API_KEY = os.getenv('API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL') 


def get_db_connection():
    """Establishes and returns a new database connection."""
    if not DATABASE_URL:
        print("Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database: {e}", file=sys.stderr)
        return None


def classify_text(text: str, tier: str = "fast"):
    url = BASE_URL
    headers = {"Content-Type": "application/json"}
    if API_KEY: 
        headers["Authorization"] = f"Bearer {API_KEY}"
    
    data = {"text": text, "tier": tier}
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=20)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error classifying text: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during classification request: {e}")
        return None

def get_article_text(url):
    """Fetches an article URL and scrapes its text content from <p> tags."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p')
            article_text = ' '.join([p.get_text() for p in paragraphs])
            article_text = ' '.join(article_text.split())
            return article_text
        else:
            print(f"Failed to retrieve article (Status code: {response.status_code}) for url: {url}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during scraping {url}: {e}")
        return None


us_major_sources = {
    'cnn.com', 'foxnews.com',
    'nytimes.com', 'washingtonpost.com', 'wsj.com',
    'politico.com', 'apnews.com', 'thehill.com'
}
domain_query = "(" + " OR ".join([f"domain:{source}" for source in us_major_sources]) + ")"
theme_query = "(theme:ACT_MAKESTATEMENT OR theme:ACT_HARMTHREATEN)"
lang_query = "sourcelang:english"
query = f"{domain_query} {theme_query} {lang_query}"

params = {
    "query": query,
    "mode": "ArtList",
    "format": "json",
    "sort": "DateDesc",
    "timespan": "1d",
    "maxrecords": 100
}
base_url = "https://api.gdeltproject.org/api/v2/doc/doc"

print(f"Querying GDELT DOC API...")


articles_processed = 0
articles_inserted = 0


insert_query = """
    INSERT INTO public.articles (source_domain, title, url, scraped_text, classification_json)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (url) DO NOTHING;
"""

# Establish DB connection
conn = get_db_connection()
cur = None

if conn:
    try:
        cur = conn.cursor()
        
        # Make the GDELT request
        response = requests.get(base_url, params=params)
        
        if response.status_code == 200 and response.text:
            try:
                data = response.json()
                
                if 'articles' in data and len(data['articles']) > 0:
                    print(f"Found {len(data['articles'])} articles. Processing and inserting into DB...\n")
                    
                    for article in data['articles']:
                        try:
                            title = article['title']
                            url = article['url']
                            source = article['domain']
                            
                            print(f"Processing: {title[:70]}...")
                            
                            article_body = get_article_text(url)
                            
                            if article_body is None:
                                print(f"   > Skipping (scraping failed).")
                                continue
                            
                            classification_result = classify_text(article_body, "fast")
                            classification_json = json.dumps(classification_result)
                            
                            data_tuple = (source, title, url, article_body, classification_json)
                            cur.execute(insert_query, data_tuple)
                            
                            articles_processed += 1
                            if cur.rowcount > 0:
                                articles_inserted += 1
                                print(f"   > Source: {source} (Inserted)")
                            else:
                                print(f"   > Source: {source} (Skipped as duplicate)")
                                
                            print("-" * 20)

                        except Exception as e:
                            print(f"Error processing article {url}: {e}", file=sys.stderr)
                            pass
                    
                    conn.commit()
                    
                    print(f"\nSuccessfully processed {articles_processed} articles.")
                    print(f"Inserted {articles_inserted} new articles into the database.")

                else:
                    print(f"No articles found matching criteria.")
            
            except json.JSONDecodeError:
                print(f"Error: API returned non-JSON content.", file=sys.stderr)
        
        else:
            print(f"Error: GDELT API request failed (Status: {response.status_code})", file=sys.stderr)

    except (Exception, psycopg2.Error) as error:
        print(f"A critical error occurred: {error}", file=sys.stderr)
        if conn:
            conn.rollback()
            
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")
else:
    print("Script aborted: Could not connect to the database.", file=sys.stderr)
    sys.exit(1) # Exit with an error code
