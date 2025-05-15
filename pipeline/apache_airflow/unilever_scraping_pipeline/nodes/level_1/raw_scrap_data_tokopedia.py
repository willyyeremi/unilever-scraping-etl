import time
from datetime import datetime
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import requests
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.firefox.service import Service
from bs4 import BeautifulSoup
from sqlalchemy import Column, Integer, Float, String, Date
from sqlalchemy.orm import declarative_base, Session

##################################################
# common used variable
##################################################

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
HEADERS = {"User-Agent": "Mozilla/5.0"}


##################################################
# database table object
##################################################

base = declarative_base()
class raw_scrap_data(base):
    __tablename__ = 'raw_scrap_data'
    __table_args__ = {'schema': 'main'}
    id = Column(Integer, primary_key = True)
    name = Column(String)
    detail = Column(String)
    price = Column(Integer)
    originalprice = Column(Integer)
    discountpercentage = Column(Float)
    platform = Column(String)
    createdate = Column(Date)


##################################################
# function for pipeline
##################################################

def driver_maker():
    service = Service(executable_path = "/home/airflow/browser_driver/geckodriver")
    options = FirefoxOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--display=:99')
    driver = Firefox(service = service, options = options)
    return driver

def scroll_until_next_button(driver):
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Cek apakah tombol next muncul di halaman
        next_button = soup.find('a', {'data-testid': 'btnShopProductPageNext', 'class': 'css-buross'})
        if next_button:
            break 

def product_validity_count(url, full_check = False):
    global HEADERS
    if not full_check:
        res = requests.get(url, headers = HEADERS)
        if res.status_code != 200:
            product_validity_count(url, full_check = True)
        soup = BeautifulSoup(res.text, 'html.parser')
    else:
        with driver_maker() as driver:
            driver.get(url)
            scroll_until_next_button(driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
    all_products = soup.find_all('div', class_ = "css-1sn1xa2")
    invalid_products = soup.find_all('div', attrs = {'data-testid': 'divImgProductOverlay'})
    valid_count = len(all_products) - len(invalid_products)
    return valid_count, len(invalid_products)

def find_last_valid_page(base_url,  step = 10):
    page = step
    while True:
        valid, invalid = product_validity_count(f"{base_url}/page/{page}")
        if invalid > 0:
            if valid > 0:
                logger.info(f"INFO - Last valid page: {page}")
                return page
            else:
                page -= (step // 2)
                break
        page += step
    status = 0
    while True:
        valid, invalid = product_validity_count(f"{base_url}/page/{page}")
        if valid > 0:
            if invalid > 0:
                logger.info(f"INFO - Last valid page: {page}")
                return page
            elif invalid == 0:
                valid, invalid = product_validity_count(f"{base_url}/page/{page}", full_check = True)
                if invalid > 0:
                    logger.info(f"INFO - Last valid page: {page}")
                    return page 
                else:
                    page += 1
                    status = 1
        if valid == 0:
            if status == 1:
                page -= 1
                logger.info(f"INFO - Last valid page: {page}")
                return page
            if status == 0:
                page -= 1

def extract_active_product_links(soup, url):
    try:
        link_list = []
        product_containers = soup.find_all('div', class_='css-1sn1xa2')
        for product_container in product_containers:
            if product_container.find('div', attrs = {'data-testid': 'divImgProductOverlay'}):
                continue 
            link_tag = product_container.find('a', class_='pcv3__info-content css-gwkf0u')
            if link_tag:
                link_list.append(link_tag.get('href'))
        return link_list
    except Exception as e:
        logger.error(f"ERROR - extract_active_product_links() error: {e} | url: {url}")

def collect_product_links_from_catalog_page(url):
    try:
        with driver_maker() as driver:
            driver.get(url)
            time.sleep(3)
            scroll_until_next_button(driver = driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return extract_active_product_links(soup, url)
    except Exception as e:
        logger.error(f"ERROR - collect_product_links_from_catalog_page() error: {e} | url: {url}")

def is_page_empty(soup, product_url) -> bool:
    try:
        name_element = soup.find('h1', class_='css-j63za0') if soup.find('h1', class_='css-j63za0') else None
        price_element = soup.find('div', class_='price') if soup.find('div', class_='price') else None
        if name_element is None or price_element is None:
            logger.info(f"INFO - Page is empty | url: {product_url}")
            return True
        return False
    except Exception as e:
        logger.error(f"ERROR - is_page_empty() error: {e}")
        
def scrape_product_detail(product_url):
    global HEADERS
    current_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d')
    try:
        product_data = {}
        res = requests.get(product_url, headers = HEADERS)
        if res.status_code != 200:
            logger.info(f"INFO - Status code: {res.status_code} ({res.reason}) | url: {product_url}")
        soup = BeautifulSoup(res.text, 'html.parser')
        soup_body = BeautifulSoup(str(soup.body), 'html.parser')
        page_empty_status = is_page_empty(soup_body, product_url)
        if page_empty_status == True:
            with driver_maker() as driver:
                driver.get(product_url)
                time.sleep(3)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_data['name'] = soup.find('h1', class_='css-j63za0').text.strip()
        product_data['detail'] = soup.select_one('div[data-testid="lblPDPDescriptionProduk"]').text if soup.select_one('div[data-testid="lblPDPDescriptionProduk"]') else None
        product_data['price'] = int(soup.find('div', class_='price').text.replace("Rp", "").replace(".", ""))
        product_data['originalprice'] = int(soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]').text.replace("Rp", "").replace(".", "")) if soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]') else None
        product_data['discountpercentage'] = float(soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]').text.replace("%", "")) / 100 if soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]') else None
        product_data['platform'] = 'tokopedia'
        product_data['createdate'] = current_timestamp
        return product_data
    except Exception as e:
        logger.error(f"ERROR - scrape_product_detail() error: {e} | url: {product_url} | data: {product_data}")

def data_insert(connection_engine, data):
    try:
        with Session(autocommit = False, autoflush = False, bind = connection_engine) as session:
            new_data = raw_scrap_data(
                name = data['name']
                ,detail = data['detail']
                ,price = data['price']
                ,originalprice = data['originalprice']
                ,discountpercentage = data['discountpercentage']
                ,platform = data['platform']
                ,createdate = data['createdate']
            )
            session.add(new_data)
            session.commit()
    except Exception as e:
        logger.error(f"ERROR - data_insert() error: {e} | data: {data}")

def collect_active_product_links_parallel_executor(base_url, last_valid_page, connection_engine, num_processes = 5):
    try:
        catalog_urls = [base_url] + [f"{base_url}/page/{page}" for page in range(2, last_valid_page + 1)]
        # Pool 1: scraping halaman katalog
        with ProcessPoolExecutor(max_workers = num_processes) as catalog_executor:
            future_catalog = [catalog_executor.submit(collect_product_links_from_catalog_page, url) for url in catalog_urls]
            for finished_catalog in as_completed(future_catalog):
                product_url = finished_catalog.result()
                # Pool 2: scraping halaman produk (hanya pakai requests)
                with ProcessPoolExecutor(max_workers = num_processes) as product_executor:
                    future_product = [product_executor.submit(scrape_product_detail, link) for link in product_url]
                    for finished_product in as_completed(future_product):
                        product_data = finished_product.result()
                        data_insert(connection_engine, product_data)
    except Exception as e:
        logger.error(f"ERROR - collect_active_product_links_parallel_executor() error: {e}")


##################################################
# pipeline for container airflow
##################################################

def run_pipeline(connection_engine):
    engine = connection_engine
    last_valid_page = find_last_valid_page("https://www.tokopedia.com/unilever/product")
    collect_active_product_links_parallel_executor("https://www.tokopedia.com/unilever/product", last_valid_page, engine, num_processes = 5)

