import time
from datetime import datetime
import logging
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
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

UNILEVER_SHOP_LINKS = ["rumah-bersih-unilever", "unilever-hair-beauty-studio", "daily-care-by-unilever", "unilever-food", "unilevermall"]
CURRENT_TIMESTAMP = datetime.strftime(datetime.now(), '%Y-%m-%d')


##################################################
# request headers setup
##################################################

software_names = [SoftwareName.CHROME.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value] 
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)

# HEADERS = {
#     "User-Agent": f"{user_agent_rotator.get_random_user_agent()}"
# }
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


##################################################
# filtering html
##################################################

NEXT_BUTTON = {
    "name": "a"
    ,"attrs": {"data-testid": "btnShopProductPageNext", "class": "css-buross"}
}

INVALID_PAGE_MARK = {
    "name": "div"
    ,"attrs": {"class": "css-3ytcpr-unf-emptystate e1mmy8p70"}
}

ALL_PRODUCTS = {
    "name": "a"
    ,"attrs": {"class": "Ui5-B4CDAk4Cv-cjLm4o0g== XeGJAOdlJaxl4+UD3zEJLg=="}
}

INVALID_PRODUCTS = {
    "name": "div"
    ,"attrs": {"class": "_4A0sz2e6IddlQgpD0HR6qw=="}
}

PRODUCT_NAME = {
    "name": "h1"
    ,"attrs": {"class": "css-j63za0", "data-testid": "lblPDPDetailProductName"}
}

PRODUCT_PRICE = {
    "name": "div"
    ,"attrs": {"class": "price", "data-testid": "lblPDPDetailProductPrice"}
}

PRODUCT_DETAIL = {
    "name": "div"
    ,"attrs": {"data-testid": "lblPDPDescriptionProduk"}
}

PRODUCT_ORIGINALPRICE = {
    "name": "span"
    ,"attrs": {"data-testid": "lblPDPDetailOriginalPrice"}
}

PRODUCT_DISCOUNTPERCENTAGE = {
    "name": "span"
    ,"attrs": {"data-testid": "lblPDPDetailDiscountPercentage"}
}


##################################################
# database table object
##################################################

base = declarative_base()
class tr_raw_scrap_data(base):
    __tablename__ = 'tr_raw_scrap_data'
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
    options.add_argument("--display=:99")
    options.set_preference("general.useragent.override", user_agent_rotator.get_random_user_agent())
    driver = Firefox(service = service, options = options)
    return driver

def scroll_until_next_button(driver):
    counter = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Cek apakah tombol next muncul di halaman
        next_button = soup.find(name = NEXT_BUTTON["name"], attrs = NEXT_BUTTON["attrs"])
        if next_button:
            break 
        if counter > 5:
            break
        counter += 1

def product_validity_count(url, full_check = False):
    if full_check != True:
        res = requests.Session().get(url = url, headers = HEADERS)
        if res.status_code != 200:
            product_validity_count(url, full_check = True)
        soup = BeautifulSoup(res.text, 'html.parser')
    else:
        with driver_maker() as driver:
            driver.get(url)
            scroll_until_next_button(driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
    invalid_page_mark = soup.find_all(name = INVALID_PAGE_MARK["name"], attrs = INVALID_PAGE_MARK["attrs"])
    if len(invalid_page_mark) > 0:
        return 0, 0
    all_products = soup.find_all(name = ALL_PRODUCTS["name"], attrs = ALL_PRODUCTS["attrs"])
    invalid_products = soup.find_all(name = INVALID_PRODUCTS["name"], attrs = INVALID_PRODUCTS["attrs"])
    valid_count = len(all_products) - len(invalid_products)
    return valid_count, len(invalid_products)

def find_last_valid_page(base_url, step = 10):
    page = step
    while True:
        valid, invalid = product_validity_count(f"{base_url}/page/{page}")
        if valid == 0 and invalid == 0:
            page -= 1
            break
        elif invalid > 0:
            if valid > 0:
                logger.info(f"Last valid page: {page}")
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
                logger.info(f"Last valid page: {page}")
                return page
            elif invalid == 0:
                valid, invalid = product_validity_count(f"{base_url}/page/{page}", full_check = True)
                if invalid > 0:
                    logger.info(f"Last valid page: {page}")
                    return page 
                else:
                    page += 1
                    status = 1
        if valid == 0:
            if status == 1:
                page -= 1
                logger.info(f"Last valid page: {page}")
                return page
            if status == 0:
                page -= 1     

def extract_active_product_links(soup, url):
    try:
        link_list = []
        link_tags = soup.find_all(name = ALL_PRODUCTS["name"], attrs = ALL_PRODUCTS["attrs"])
        # Filter to select only valid product within a catalog page by using shadow object as marker
        for link_tag in link_tags:
            if link_tag.find(name = INVALID_PRODUCTS["name"], attrs = INVALID_PRODUCTS["attrs"]):
                continue 
            link_list.append(link_tag.get("href"))
        return link_list
    except Exception as e:
        logger.error(f"On extract_active_product_links(): {e}")
        logger.info(f"url: {url}")

def collect_product_links_from_catalog_page(url):
    try:
        with driver_maker() as driver:
            driver.get(url)
            time.sleep(3)
            scroll_until_next_button(driver = driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            return extract_active_product_links(soup, url)
    except Exception as e:
        logger.error(f"On collect_product_links_from_catalog_page(): {e}")
        logger.info(f"url: {url}")
        return []

def is_page_empty(soup, product_url) -> Optional[bool]:
    try:
        product_name = soup.find(name = PRODUCT_NAME["name"], attrs = PRODUCT_NAME["attrs"]) if soup.find(name = PRODUCT_NAME["name"], attrs = PRODUCT_NAME["attrs"]) else None
        product_price = soup.find(name = PRODUCT_PRICE["name"], attrs = PRODUCT_PRICE["attrs"]) if soup.find(name = PRODUCT_PRICE["name"], attrs = PRODUCT_PRICE["attrs"]) else None
        if product_name is None or product_price is None:
            logger.info(f"Page is empty | url: {product_url}")
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"On is_page_empty(): {e}")

def scrape_product_detail(product_url):
    product_data = {}
    try:
        res = requests.Session().get(url = product_url, headers = HEADERS)
        if res.status_code != 200:
            logger.info(f"Status code: {res.status_code} ({res.reason}) | url: {product_url}")
        soup = BeautifulSoup(res.text, 'html.parser')
        soup_body = BeautifulSoup(str(soup.body), 'html.parser')
        page_empty_status = is_page_empty(soup_body, product_url)
        if page_empty_status == True:
            with driver_maker() as driver:
                driver.get(product_url)
                time.sleep(3)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_data['name'] = soup.find(name = PRODUCT_NAME["name"], attrs = PRODUCT_NAME["attrs"]).get_text(strip = True)
        product_data['detail'] = soup.find(name = PRODUCT_DETAIL["name"], attrs = PRODUCT_DETAIL["attrs"]).get_text(strip = True) if soup.find(name = PRODUCT_DETAIL["name"], attrs = PRODUCT_DETAIL["attrs"]).get_text(strip = True) else None
        product_data['price'] = int(soup.find(name = PRODUCT_PRICE["name"], attrs = PRODUCT_PRICE["attrs"]).get_text(strip = True).replace("Rp", "").replace(".", ""))
        product_data['originalprice'] = int(soup.find(name = PRODUCT_ORIGINALPRICE["name"], attrs = PRODUCT_ORIGINALPRICE["attrs"]).get_text(strip = True).replace("Rp", "").replace(".", "")) if soup.find(name = PRODUCT_ORIGINALPRICE["name"], attrs = PRODUCT_ORIGINALPRICE["attrs"]).get_text(strip = True)else None
        product_data['discountpercentage'] = float(soup.find(name = PRODUCT_DISCOUNTPERCENTAGE["name"], attrs = PRODUCT_DISCOUNTPERCENTAGE["attrs"]).get_text(strip = True).replace("%", "")) / 100 if soup.find(name = PRODUCT_DISCOUNTPERCENTAGE["name"], attrs = PRODUCT_DISCOUNTPERCENTAGE["attrs"]).get_text(strip = True) else None
        product_data['platform'] = 'tokopedia'
        product_data['createdate'] = CURRENT_TIMESTAMP
        return product_data
    except Exception as e:
        logger.error(f"On scrape_product_detail(): {e}")
        logger.info(f"url: {product_url}")
        if product_data is not {}:
            logger.info(f"data: {product_data}")
        logger.info(f"Probably the url page is changed in the middle of scraping")

def data_insert(connection_engine, data):
    try:
        with Session(autocommit = False, autoflush = False, bind = connection_engine) as session:
            new_data = tr_raw_scrap_data(
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
        logger.error(f"On data_insert(): {e}")
        logger.info(f"data: {data}")

def collect_active_product_links_parallel_executor(base_url, last_valid_page, connection_engine, num_processes = 5):
    try:
        catalog_urls = [base_url] + [f"{base_url}/page/{page}" for page in range(2, last_valid_page + 1)]
        # Pool 1: scraping halaman katalog
        with ProcessPoolExecutor(max_workers = num_processes) as catalog_executor:
            future_catalog = [catalog_executor.submit(collect_product_links_from_catalog_page, url) for url in catalog_urls]
            for finished_catalog in as_completed(future_catalog):
                product_url = finished_catalog.result()
                if product_url == []:
                    logger.warning("No product URL collected, skipping...")
                    continue
                # Pool 2: scraping halaman produk (hanya pakai requests)
                with ProcessPoolExecutor(max_workers = num_processes) as product_executor:
                    future_product = [product_executor.submit(scrape_product_detail, link) for link in product_url]
                    for finished_product in as_completed(future_product):
                        product_data = finished_product.result()
                        data_insert(connection_engine, product_data)
    except Exception as e:
        logger.error(f"On collect_active_product_links_parallel_executor(): {e}")


##################################################
# pipeline for container airflow
##################################################

def run_pipeline(connection_engine):
    engine = connection_engine
    for link in UNILEVER_SHOP_LINKS:
        last_valid_page = find_last_valid_page(f"https://www.tokopedia.com/{link}/product")
        collect_active_product_links_parallel_executor(f"https://www.tokopedia.com/{link}/product", last_valid_page, engine, num_processes = 5)

