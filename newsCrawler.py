import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
from selenium.webdriver.chrome.options import Options
from scrapy.crawler import CrawlerProcess
from openai import OpenAI
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor
import pytz
import time
from database import  insert_news,extract_last_news

load_dotenv('.env')

# Set up OpenAI API client
api = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=api ,base_url="https://api.deepseek.com")
def assign_industry(client,news_title):
    """Assigns industry to the news title using DeepSeek API."""
    try:
        response = client.chat.completions.create(
        model="deepseek-chat",  # ✅ correct model name for DeepSeek
        messages=[
            {
                "role": "user",
                "content": f"""Classify the following news title into one of these industries:

        Construction, Consumer Products & Services(such as Automotive,F&B,Retailers,so forth), Energy, Financial Services, Health Care, Industrial Products & Services(such as Auto Parts,Building Materials,Chemicals,Metal,Wood and so on), Plantation, Property, REIT, Technology, Telecommunications & Media, Transportation & Logistics, Utilities,Unknown

        Respond with only a single word in quotes — such as Construction, Financial Services, or Unknown . no quotation marks
        the main idea is to categorize the  BUSINESS-RELATED news!!! OTHERS CLASSIFIED AS UNKNOWN IF THEY HAVE NO IMPACT ON ANY INDUSTRY

        Title: {news_title}"""
            }
        ],
        )

        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error in API call: {e}")
        return None



def batch_assign_industries(client, items):
    def classify(item):
        item['Sector'] = assign_industry(client, item['Title'])
        return item

    with ThreadPoolExecutor(max_workers=10) as executor:
        return list(executor.map(classify, items))


def extract_key_entities(client, news_title):
    """Extracts structured named entities from a news title using DeepSeek API."""
    try:
        response = client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {
                            "role": "user",
                            "content": 
                            f"""Extract the most relevant and precise Noun, Verb, and Object from the following news headline. 
            Focus only on the **core subject**, **main verb**, and **main object**. 
            Return the result in the following format:

            Noun: noun,  
            Verb: verb,  
            Object: object
            for example: Truck maker Volvo cuts North America market outlook amid tariff uncertainty after Q1 profit falls
            the result should be:
            Noun: Volvo,
            Verb: cut,
            Object: market outlook

            Guidelines:
            - Only include the **main subject noun** 
            - Only include the **root verb** in its **base form**
            - Only include the **primary object** or concept being acted upon 
            - No quotes or extra explanation. Just a clean 3-line output as shown.

        Title: {news_title}"""
                        }
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error extracting entities: {e}")
        return "None"

def batch_extract_entities(client, news_items):
    """
    Takes a list of dicts (each with a 'Title') and adds a 'Key_Entities' field.
    """
    def enrich_with_entities(item):
        item['Extracted_Entities'] = extract_key_entities(client, item['Title'])
        return item

    with ThreadPoolExecutor(max_workers=10) as executor:
        return list(executor.map(enrich_with_entities, news_items))
    
def unstructured_news(): 
    latest_data = extract_last_news('Market_News')
    print(latest_data)
    # Ensure target_date is initialized and timezone-aware
    target_date = datetime.now(pytz.timezone('Asia/Kuala_Lumpur'))
    print(target_date)
    # Parse the target date (ignoring time)
    market_news = []
    # Set Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")  # Useful for Linux options=chrome_options  

    # Initialize the driver with options
    driver = webdriver.Chrome(chrome_options)
    driver.get("https://www.klsescreener.com/v2/news")
    
    language_ids = ['checkbox_language_ms', 'checkbox_language_zh']

    for lang_id in language_ids:
        label = WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"label[for='{lang_id}']")))
        driver.execute_script("arguments[0].click();", label)  # Faster & more robust than .click()

        # Wait for the page to load
        time.sleep(2)
    driver.refresh()
    time.sleep(3)



    def process_page():
        """Process the current page with BeautifulSoup"""
        try:
            body = driver.page_source
            soup = BeautifulSoup(body, "html.parser")
            articles = soup.find("div", id="section")
           
            if not articles:
                print("No articles found.")
                return False

            individual_articles = articles.find_all('div', class_='item figure flex-block')
            found_new = False

            for article in individual_articles:
                # print(f'article: {article}')
                title = article.find('h2').get_text(strip=True)
                if latest_data is not None and not latest_data.empty:
                    if latest_data['Title'].str.contains(title).any():
                         return False  # Stop if we reached previously scraped title

                date_str = article.find('span', attrs={"data-date": True})['data-date']
                article_date = datetime.fromisoformat(date_str)
                article_date = article_date.replace(tzinfo=pytz.timezone('Asia/Kuala_Lumpur'))  # Ensure timezone-aware
              
                # Stop if this article is too old
                if abs(target_date - article_date) > timedelta(hours=1):
                    return False

                news_hyperlink = 'https://www.klsescreener.com' + article.find('a')['href']

                market_news.append({
                    'Title': title,
                    'News_Hyperlinks': news_hyperlink,
                    'Published_Date': date_str
                })
                found_new = True
              
            return found_new  # Continue if at least one new item is found

        except Exception as e:
            print(f"Error processing page: {e}")
            return False

    while True:
        try:
            # Process the current page
            if not process_page():
                break

            # Click the "Load More" button
            load_more_button = WebDriverWait(driver, 0.01).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".figure_loading"))
            )
            load_more_button.click()

        except TimeoutException:
            print("No 'Load More' button found or content loading timed out.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

    # Close the browser
    driver.quit()
    
    print(f'{len(market_news)} articles found.')
    # Return the results
    return  market_news


import scrapy
from bs4 import BeautifulSoup

class NewsMainStorySpider(scrapy.Spider):
    name = 'main_story_spider'


    def __init__(self, market_news):
        self.market_news = market_news
        self.market_news = batch_assign_industries(client, self.market_news)
        self.market_news = batch_extract_entities(client, self.market_news)
        self.start_urls = [item['News_Hyperlinks'] for item in market_news]

    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
        }
        for idx, url in enumerate(self.start_urls):
            yield scrapy.Request(url=url, headers=headers, callback=self.parse, meta={'index': idx})

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        news_container = soup.find('div', class_='news-container')
        body_text_container = news_container.find('div',class_='content text-justify')
        paragraphs = body_text_container.find_all('p')
        related_stocks_section = soup.find('div', class_='stock-list table-responsive')
        full_text = ' '.join([p.get_text(strip=True) for p in paragraphs[:-1]])
        img = news_container.find('img')
        if img:
            img_url = img['src']
            # print(f"Image URL: {img_url}")
        else:
            img_url = 'https://www.klsescreener.com/v2/img/icon_navbar.png'
            # print("No image found.")
         

        related_stocks = []
        if related_stocks_section:
            related_stocks = [
                stock.find('span').get_text(strip=True)
                for stock in related_stocks_section.find_all('tr')
            ]

        for item in self.market_news:
            if item['News_Hyperlinks'] == response.url:
                item['Body'] = full_text
                item['Related_Stock'] = ', '.join(related_stocks)
                item['Img'] = img_url
                break  # No need to continue once matched

    def closed(self, reason):
        """Called when spider finishes crawling."""
        new_data = pd.DataFrame(self.market_news)
        print(new_data)
        
        if not new_data.empty:
             insert_news(new_data, 'Market_News')

      


if __name__ == "__main__":
    # Step 1: Scrape unstructured news
    market_news = unstructured_news()

    # Step 2: Pass dictionaries to Scrapy
    process = CrawlerProcess()
    process.crawl(NewsMainStorySpider, market_news=market_news)
    process.start()
