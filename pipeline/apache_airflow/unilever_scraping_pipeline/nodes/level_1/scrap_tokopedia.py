import time
from datetime import datetime
import logging
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from curl_cffi import requests
# import requests
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

software_names = [SoftwareName.FIREFOX.value]
operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value] 
user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)

UNILEVER_SHOP_LINKS = ["rumah-bersih-unilever", "unilever-hair-beauty-studio", "daily-care-by-unilever", "unilever-food", "unilevermall"]
CURRENT_TIMESTAMP = datetime.strftime(datetime.now(), '%Y-%m-%d')


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

# def driver_maker():
#     service = Service(executable_path = "/home/airflow/browser_driver/geckodriver")
#     options = FirefoxOptions()
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--display=:99")
#     options.set_preference("general.useragent.override", user_agent_rotator.get_random_user_agent())
#     driver = Firefox(service = service, options = options)
#     return driver

def driver_maker():
    service = Service(executable_path = "./pipeline/apache_airflow/unilever_scraping_pipeline/nodes/level_1/geckodriver.exe")
    options = FirefoxOptions()
    options.binary_location = "C:/Program Files/Mozilla Firefox/firefox.exe"
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.set_preference("general.useragent.override", user_agent_rotator.get_random_user_agent())
    driver = Firefox(service = service, options = options)
    return driver

def requests_page_get(url: str):
    res = requests.get(
        url,
        impersonate = "firefox",
        timeout = 10
    )
    return res

def scroll_until_next_button(driver):
    next_button_local_var = NEXT_BUTTON
    counter = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Cek apakah tombol next muncul di halaman
        next_button = soup.find(name = next_button_local_var["name"], attrs = next_button_local_var["attrs"])
        if next_button:
            break 
        if counter > 5:
            break
        counter += 1

def product_validity_count(url: str, full_check: bool = False) -> tuple[int, int, bool]:
    invalid_page_mark_local_var = INVALID_PAGE_MARK
    all_products_local_var = ALL_PRODUCTS
    invalid_products_local_var = INVALID_PRODUCTS
    if full_check != True:
        res = requests_page_get(url = url)
        if res.status_code != 200:
            product_validity_count(url, full_check = True)
        soup = BeautifulSoup(res.text, 'html.parser')
    else:
        with driver_maker() as driver:
            driver.get(url)
            scroll_until_next_button(driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
    invalid_page_mark = soup.find_all(name = invalid_page_mark_local_var["name"], attrs = invalid_page_mark_local_var["attrs"])
    if len(invalid_page_mark) > 0:
        return 0, 0, False
    all_products = soup.find_all(name = all_products_local_var["name"], attrs = all_products_local_var["attrs"])
    invalid_products = soup.find_all(name = invalid_products_local_var["name"], attrs = invalid_products_local_var["attrs"])
    valid_count = len(all_products) - len(invalid_products)
    return valid_count, len(invalid_products), True

def find_last_valid_page(base_url, step = 10) -> int:
    page = step
    while True:
        valid, invalid, page_validity = product_validity_count(f"{base_url}/page/{page}")
        if invalid > 0 or page_validity == False:
            if valid > 0:
                logger.info(f"Last valid page: {page}")
                return page
            else:
                page -= (step // 2)
                break
        page += step
    status = 0
    while True:
        valid, invalid, page_validity = product_validity_count(f"{base_url}/page/{page}")
        if valid > 0:
            if invalid > 0:
                logger.info(f"Last valid page: {page}")
                return page
            elif invalid == 0:
                valid, invalid, page_validity = product_validity_count(f"{base_url}/page/{page}", full_check = True)
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

def collect_product_links_from_catalog_page(url: str) -> Optional[list[str]]:
    all_products_local_var = ALL_PRODUCTS
    invalid_products_local_var = INVALID_PRODUCTS
    try:
        with driver_maker() as driver:
            driver.get(url)
            time.sleep(3)
            scroll_until_next_button(driver = driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            link_list = []
            link_tags = soup.find_all(name = all_products_local_var["name"], attrs = all_products_local_var["attrs"])
            # Filter to select only valid product within a catalog page by using shadow object as marker
            for link_tag in link_tags:
                if link_tag.find(name = invalid_products_local_var["name"], attrs = invalid_products_local_var["attrs"]):
                    continue 
                link_list.append(link_tag.get("href"))
            return link_list
    except Exception as e:
        logger.error(f"On collect_product_links_from_catalog_page(): {e}")
        logger.info(f"url: {url}")

def is_page_empty(soup, product_url) -> Optional[bool]:
    product_name_local_var = PRODUCT_NAME
    product_price_local_var = PRODUCT_PRICE
    try:
        if soup.find(name = product_name_local_var["name"], attrs = product_name_local_var["attrs"]):
            product_name = soup.find(name = product_name_local_var["name"], attrs = product_name_local_var["attrs"])
        else:
            product_name = None
        if soup.find(name = product_price_local_var["name"], attrs = product_price_local_var["attrs"]):
            product_price = soup.find(name = product_price_local_var["name"], attrs = product_price_local_var["attrs"]) 
        else:
            product_price = None
        if product_name is None or product_price is None:
            logger.info(f"Page is empty | url: {product_url}")
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"On is_page_empty(): {e}")

def scrape_product_detail(product_url):
    product_name_local_var = PRODUCT_NAME
    product_detail_local_var = PRODUCT_DETAIL
    product_price_local_var = PRODUCT_PRICE
    product_originalprice_local_var = PRODUCT_ORIGINALPRICE
    product_discountpercentage_local_var = PRODUCT_DISCOUNTPERCENTAGE
    current_timestamp_local_var = CURRENT_TIMESTAMP
    product_data = {}
    try:
        res = requests_page_get(url = product_url)
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
        product_data['name'] = soup.find(name = product_name_local_var["name"], attrs = product_name_local_var["attrs"]).get_text(strip = True)
        if soup.find(name = product_detail_local_var["name"], attrs = product_detail_local_var["attrs"]):
            product_data['detail'] = soup.find(name = product_detail_local_var["name"], attrs = product_detail_local_var["attrs"]).get_text(strip = True) 
        else: 
            product_data['detail'] = None
        product_data['price'] = int(soup.find(name = product_price_local_var["name"], attrs = product_price_local_var["attrs"]).get_text(strip = True).replace("Rp", "").replace(".", ""))
        if soup.find(name = product_originalprice_local_var["name"], attrs = product_originalprice_local_var["attrs"]):
            product_data['originalprice'] = int(soup.find(name = product_originalprice_local_var["name"], attrs = product_originalprice_local_var["attrs"]).get_text(strip = True).replace("Rp", "").replace(".", ""))
        else:
            product_data['originalprice'] = None
        if soup.find(name = product_discountpercentage_local_var["name"], attrs = product_discountpercentage_local_var["attrs"]): 
            product_data['discountpercentage'] = float(soup.find(name = product_discountpercentage_local_var["name"], attrs = product_discountpercentage_local_var["attrs"]).get_text(strip = True).replace("%", "")) / 100
        else: 
            product_data['discountpercentage'] = None
        product_data['platform'] = 'tokopedia'
        product_data['createdate'] = current_timestamp_local_var
        return product_data
    except Exception as e:
        if len(product_data) != 0:
            data_log = f"data: {product_data}"
        else:
            data_log = ""
        logger.error(
            f"""On scrape_product_detail(): {e}
            url: {product_url}
            {data_log}"""
        )

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

if __name__ == "__main__":
    from sqlalchemy import URL, create_engine
    url_object = URL.create(
        f"postgresql+psycopg2"
        ,username = "admin"
        ,password = "admin"
        ,host = "localhost"
        ,port = 5432
        ,database = "online_shop"
    )
    engine = create_engine(url_object)
    for link in UNILEVER_SHOP_LINKS:
        last_valid_page = find_last_valid_page(f"https://www.tokopedia.com/{link}/product")
        collect_active_product_links_parallel_executor(f"https://www.tokopedia.com/{link}/product", last_valid_page, engine, num_processes = 5)