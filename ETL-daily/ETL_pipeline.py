import requests
import json
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import psycopg2  # For PostgreSQL
import sys

# Load environment variables
load_dotenv()
api_url= os.getenv('BASE_URL')
api_key = os.getenv('API_KEY')
db_url = os.getenv('DATABASE_URL') # Render will inject this

# --- Database Connection Function ---
def get_db_connection():
    if not db_url:
        print("Error: DATABASE_URL environment variable is not set.", file=sys.stderr)
        return None
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e: # generic exception
        print(f"Error: Unable to connect to the database: {e}", file=sys.stderr)
        return None

# --- API and Scraping Functions (Unchanged) ---
def classify_text(text, tier = "fast"):
    url = f"{api_url}/v1/classify"
    headers = {"Content-Type": "application/json"}
    if api_key: 
        headers["Authorization"] = f"Bearer {api_key}"
    
    data = {"text":text, "tier":tier}
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=20)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error classifying text: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error during classification request: {e}")
        return None

def get_article_text(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout = 10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p')
            
            # article_text = ' '.join([p.get_text() for p in paragraphs])
            all_text = []
            for p in paragraphs: # loop through all the <p> tags
                all_text.append(p.get_text())
            article_text = ' '.join(all_text)

            article_text = ' '.join(article_text.split()) # this cleans up whitespace
            return article_text
        else:
            print(f"Failed to retrieve article (Status code: {response.status_code}) for url: {url}")
            return None
    except Exception as e:
        print(f"Error during scraping {url}: {e}")
        return None

# --- GDELT Query Setup ---
sources = {
    'cnn.com', 'foxnews.com',
    'nytimes.com', 'washingtonpost.com', 'wsj.com',
    'politico.com', 'apnews.com', 'thehill.com'
}

# build the domain query part
# domain_query = "(" + " OR ".join([f"domain:{source}" for source in sources]) + ")"
domain_bits = []
for s in sources:
    domain_bits.append(f"domain:{s}")
domain_query = "(" + " OR ".join(domain_bits) + ")"


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
gdelt_url = "https://api.gdeltproject.org/api/v2/doc/doc"

print(f"Querying GDELT DOC API...")

# --- Main Processing and Database Insertion Logic ---
processed_count = 0
inserted_count = 0

# SQL query for inserting data
insert_query = """
    INSERT INTO public.articles (source_domain, title, url, scraped_text, classification_json)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (url) DO NOTHING;
"""

# Establish DB connection
conn = get_db_connection()
cur = None

if conn:
    print("DB connected!")
    try:
        cur = conn.cursor()
        
        # Make the GDELT request
        response = requests.get(gdelt_url, params=params)
        
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
                            
                            class_res = classify_text(article_body, "fast")
                            print(f"DEBUG: got classification: {class_res}") # debug print
                            
                            class_json = json.dumps(class_res)
                            
                            data_tuple = (source, title, url, article_body, class_json)
                            cur.execute(insert_query, data_tuple)
                            
                            processed_count += 1
                            if cur.rowcount > 0:
                                inserted_count += 1
                                print(f"   > Source: {source} (Inserted)")
                            else:
                                print(f"   > Source: {source} (Skipped as duplicate)")
                                
                            print("-" * 20)

                        except Exception as e:
                            print(f"Error processing article {url}: {e}", file=sys.stderr)
                            pass
                    
                    print("...loop done")
                    # ok, now save changes
                    conn.commit()
                    
                    print(f"\nSuccessfully processed {processed_count} articles.")
                    print(f"Inserted {inserted_count} new articles into the database.")

                else:
                    print(f"No articles found matching criteria.")
            
            except: # bare except
                print(f"Error: API returned non-JSON content.", file=sys.stderr)
        
        else:
            print(f"Error: GDELT API request failed (Status: {response.status_code})", file=sys.stderr)

    except Exception as error: # changed from (Exception, psycopg2.Error)
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