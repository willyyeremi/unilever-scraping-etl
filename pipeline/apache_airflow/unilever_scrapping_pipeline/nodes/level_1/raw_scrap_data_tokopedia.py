import os
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine, URL, Column, Integer, Float, String, Date
from sqlalchemy.orm import declarative_base, Session


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
    from selenium.webdriver import Firefox, FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    # from random_user_agent.user_agent import UserAgent
    # from random_user_agent.params import SoftwareName, OperatingSystem
    # service = Service(executable_path = "/home/airflow/browser_driver/geckodriver")
    service  = Service(executable_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geckodriver.exe"))
    # software_names = [SoftwareName.FIREFOX.value]
    # operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    # user_agent_rotator = UserAgent(software_names = software_names, operating_systems = operating_systems, limit = 100)
    # user_agent = user_agent_rotator.get_random_user_agent()
    options = FirefoxOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--display=:99')
    options.binary_location = "C:/Program Files/Mozilla Firefox/firefox.exe" 
    # options.add_argument("--window-size=1920x1080")
    # options.add_argument(f'user-agent={user_agent}')
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
    headers = {"User-Agent": "Mozilla/5.0"}
    if not full_check:
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            return 0, 0
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
    # Naik hingga ketemu halaman yang hanya berisi invalid
    while True:
        valid, invalid = product_validity_count(f"{base_url}/page/{page}")
        if invalid > 0:
            if valid > 0:
                return page  # Halaman campuran valid+invalid => halaman terakhir ketemu
            else:
                page -= (step // 2)
                break  # Hanya invalid, kita mundur nanti
        page += step
    status = 0
    while True:
        valid, invalid = product_validity_count(f"{base_url}/page/{page}")
        if valid > 0:
            if invalid > 0:
                return page  # Halaman campuran => terakhir
            elif invalid == 0:
                valid, invalid = product_validity_count(f"{base_url}/page/{page}", full_check = True)
                if invalid > 0:
                    return page 
                else:
                    page += 1
                    status = 1
        if valid == 0:
            if status == 1:
                page -= 1
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
        print(f"extract_active_product_links() error: {e}\n url: {url}")

def collect_product_links_from_catalog_page(url):
    try:
        with driver_maker() as driver:
            driver.get(url)
            time.sleep(3)
            scroll_until_next_button(driver = driver)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            driver.quit()
            return extract_active_product_links(soup, url)
    except Exception as e:
        print(f"collect_product_links_from_catalog_page() error: {e}\n url: {url}")

def is_page_empty(soup) -> bool:
    try:
        name_element = soup.find('h1', class_='css-j63za0') if soup.find('h1', class_='css-j63za0') else None
        price_element = soup.find('div', class_='price') if soup.find('div', class_='price') else None
        if name_element is None or price_element is None:
            return True
        return False
    except Exception as e:
        print(f"is_page_empty() error: {e}")
        
def scrape_product_detail(product_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    current_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d')
    try:
        selenium_action = 0
        product_data = {}
        res = requests.get(product_url, headers = headers)
        if res.status_code != 200:
            print(f"Status code: {res.status_code} ({res.reason})\n url: {product_url}")
        request_soup = BeautifulSoup(res.text, 'html.parser')
        request_soup_body = BeautifulSoup(str(request_soup.body), 'html.parser')
        page_empty_status = is_page_empty(request_soup_body)
        if page_empty_status == True or res.status_code == 410:
            selenium_action = 1
        if selenium_action == 1:
            with driver_maker() as driver:
                driver.get(product_url)
                time.sleep(3)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
        else:
            soup = request_soup
        product_data['name'] = soup.find('h1', class_='css-j63za0').text.strip()
        product_data['detail'] = soup.select_one('div[data-testid="lblPDPDescriptionProduk"]').text if soup.select_one('div[data-testid="lblPDPDescriptionProduk"]') else None
        product_data['price'] = int(soup.find('div', class_='price').text.replace("Rp", "").replace(".", ""))
        product_data['originalprice'] = int(soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]').text.replace("Rp", "").replace(".", "")) if soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]') else None
        product_data['discountpercentage'] = float(soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]').text.replace("%", "")) / 100 if soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]') else None
        product_data['platform'] = 'tokopedia'
        product_data['createdate'] = current_timestamp
        return product_data
    except Exception as e:
        print(f"scrape_product_detail() error: {e}\n url: {product_url}\n data: {product_data}")

def data_insert(connection_engine, data):
    try:
        with Session(autocommit = False, autoflush = False, bind = connection_engine) as session:
            new_data = data(
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
        print(f"data_insert() error: {e}\n data: {data}")

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
        print(f"collect_active_product_links_parallel_executor() error: {e}")


##################################################
# pipeline for container airflow
##################################################

def run_pipeline(connection_engine):
    engine = connection_engine
    last_valid_page = find_last_valid_page("https://www.tokopedia.com/unilever/product")
    print(f"Last valid page: {last_valid_page}")
    collect_active_product_links_parallel_executor("https://www.tokopedia.com/unilever/product", last_valid_page, engine, num_processes = 5)


##################################################
# pipeline for local windows
##################################################

if __name__ == "__main__":
    online_shop = {
        "conn_type": "postgresql",
        "login": "admin",
        "password": "admin",
        "host": "localhost",
        "port": 5432,
        "schema": "online_shop"
    }
    driver_dict = {
        "postgresql": "psycopg2"
    }
    def create_url(config: dict[str:str], database_product: str):
        driver = driver_dict[database_product]
        url_object = URL.create(
            f"{database_product}+{driver}"
            ,username = config["login"]
            ,password = config["password"]
            ,host = config["host"]
            ,port = config["port"]
            ,database = config["schema"]
        )
        return url_object
    url = create_url(online_shop, online_shop["conn_type"])
    engine = create_engine(url)
    last_valid_page = find_last_valid_page("https://www.tokopedia.com/unilever/product")
    collect_active_product_links_parallel_executor("https://www.tokopedia.com/unilever/product", last_valid_page, engine, num_processes = 5)