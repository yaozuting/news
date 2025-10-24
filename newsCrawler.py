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



    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, 'section')))
    while True:
        try:
            # Wait for the articles section to be present
            # Find all published-date spans and inspect the last one (oldest loaded)
            date_spans = driver.find_elements(By.CSS_SELECTOR, "div.item.figure.flex-block span[data-date]")
            if not date_spans:
                # Nothing loaded yet, break and attempt to scrape whatever is present
                break

            last_date_str = date_spans[-1].get_attribute('data-date')
            try:
                last_date = datetime.fromisoformat(last_date_str).replace(tzinfo=pytz.timezone('Asia/Kuala_Lumpur'))   # Skip articles older than our threshold
                if abs(target_date - last_date) > timedelta(hours=4):
                    break
            except Exception:
                # If parsing fails, stop loading more and fall back to scraping current page
                print(f"Unable to parse date string: {last_date_str}; stopping load-more loop.")
                break
   
           
            # Otherwise click the "Load More" button and continue
            try:
                load_more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".figure_loading"))
                )
                load_more_button.click()
                # Scroll to bottom to trigger lazy loading / ensure new content loads
                
                # small wait for additional content to load
                time.sleep(1)
            except TimeoutException:
                # No more button available; stop loading more
                print("No 'Load More' button found or clickable; proceeding to scrape current content.")
                break

        except TimeoutException:
            print("Articles section did not appear in time; proceeding to scrape current content.")
            break
        except Exception as e:
            print(f"An error occurred during load-more loop: {e}")
            break

    # After finishing loading more content (or on error), parse the page ONCE
    try:
        body = driver.page_source
        soup = BeautifulSoup(body, "html.parser")
        articles = soup.find("div", id="section")

        if not articles:
            print("No articles found.")
        else:
            individual_articles = articles.find_all('div', class_='item figure flex-block')

            for article in individual_articles:
                title_tag = article.find('h2')
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                if title == latest_data['Title'].iloc[0]:
                    break

                date_span = article.find('span', attrs={"data-date": True})
                if not date_span:
                    continue
                date_str = date_span['data-date']

                news_hyperlink = 'https://www.klsescreener.com' + (article.find('a')['href'] if article.find('a') else '')

                market_news.append({
                    'Title': title,
                    'News_Hyperlinks': news_hyperlink,
                    'Published_Date': date_str
                })

    except Exception as e:
        print(f"Error processing final page scrape: {e}")

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
        new_data = pd.DataFrame(self.market_news).drop_duplicates()
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



