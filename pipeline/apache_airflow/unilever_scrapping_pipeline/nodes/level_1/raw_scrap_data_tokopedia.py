import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine, Column, Integer, Float, String, Date
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
    service = Service(executable_path = "/home/airflow/browser_driver/geckodriver")
    # service  = Service(executable_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "geckodriver.exe"))
    # software_names = [SoftwareName.FIREFOX.value]
    # operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    # user_agent_rotator = UserAgent(software_names = software_names, operating_systems = operating_systems, limit = 100)
    # user_agent = user_agent_rotator.get_random_user_agent()
    options = FirefoxOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--display=:99')
    # options.binary_location = "C:/Program Files/Mozilla Firefox/firefox.exe" 
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

def is_valid_page(url, full_check = False):
    headers = {"User-Agent": "Mozilla/5.0"}
    if full_check == False:
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            return False
        soup = BeautifulSoup(res.text, 'html.parser')
        inactive_products = soup.find_all('div', attrs = {'data-testid': 'divImgProductOverlay'})
        return len(inactive_products) == 0
    else:
        driver = driver_maker()
        driver.get(url)
        scroll_until_next_button(driver = driver)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        inactive_products = soup.find_all('div', attrs = {'data-testid': 'divImgProductOverlay'})
        return len(inactive_products) == 0

def find_last_valid_page(base_url, step = 10):
    page = step
    while is_valid_page(f"{base_url}/page/{page}", full_check = False):
        page += step
    mid_start = page - step // 2
    current = mid_start
    if is_valid_page(f"{base_url}/page/{current}", full_check = False):
        while is_valid_page(f"{base_url}/page/{current + 1}", full_check = False):
            current += 1
    else:
        while not is_valid_page(f"{base_url}/page/{current}", full_check = False) and current > 0:
            current -= 1
    while is_valid_page(f"{base_url}/page/{current}", full_check = True):
        current -= 1
    return current

def extract_active_product_links(soup):
    link_list = []
    product_containers = soup.find_all('div', class_='css-1sn1xa2')
    for product_container in product_containers:
        if product_container.find('div', attrs = {'data-testid': 'divImgProductOverlay'}):
            continue 
        link_tag = product_container.find('a', class_='pcv3__info-content css-gwkf0u')
        if link_tag:
            link_list.append(link_tag.get('href'))
    return link_list

def collect_product_links_from_catalog_page(url):
    try:
        driver = driver_maker()
        driver.get(url)
        time.sleep(3)
        scroll_until_next_button(driver = driver)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        return extract_active_product_links(soup)
    except Exception as e:
        print(f"Gagal scrape {url}: {e}")
        return []

def scrape_product_detail(product_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    current_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d')
    try:
        res = requests.get(product_url, headers=headers)
        if res.status_code != 200:
            return False
        soup = BeautifulSoup(res.text, "html.parser")
        product_data = {}
        product_data['name'] = soup.find('h1', class_='css-j63za0').text.strip()
        product_data['detail'] = soup.select_one('div[data-testid="lblPDPDescriptionProduk"]').text
        product_data['price'] = int(soup.select_one('div.price[data-testid="lblPDPDetailProductPrice"]').text.replace("Rp", "").replace(".", ""))
        originalprice_element = soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]')
        if originalprice_element:
            product_data['originalprice'] = int(originalprice_element.text.replace("Rp", "").replace(".", ""))
        else:
            product_data['originalprice'] = int(product_data['price'])
        discountpercentage_element = soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]')
        if discountpercentage_element:
            product_data['discountpercentage'] = float(discountpercentage_element.text.replace("%", "")) / 100
        else:
            product_data['discountpercentage'] = float(0)
        product_data['platform'] = 'tokopedia'
        product_data['createdate'] = current_timestamp
        return product_data
    except Exception as e:
        print(f"Gagal ambil detail {product_url}: {e}")
        return None

def data_insert(connection_engine, data):
    with Session(autocommit = False, autoflush = False, bind = connection_engine) as session:
        new_data = raw_scrap_data(
            name = data['name']
            ,price = data['price']
            ,originalprice = data['originalprice']
            ,discountpercentage = data['discountpercentage']
            ,platform = data['platform']
            ,createdate = data['createdate']
        )
        session.add(new_data)
        session.commit()

def collect_active_product_links_parallel_executor(base_url, last_valid_page, connection_engine, num_processes = 5):
    catalog_urls = [base_url] + [f"{base_url}/page/{page}" for page in range(2, last_valid_page + 1)]
    # Pool 1: scraping halaman katalog
    with ProcessPoolExecutor(max_workers = num_processes) as catalog_executor:
        future_catalog = [catalog_executor.submit(collect_product_links_from_catalog_page, url) for url in catalog_urls]
        for finisihed_catalog in as_completed(future_catalog):
            try:
                product_url = finisihed_catalog.result()
                # Pool 2: scraping halaman produk (hanya pakai requests)
                with ProcessPoolExecutor(max_workers = num_processes) as product_executor:
                    future_product = [product_executor.submit(scrape_product_detail, link) for link in product_url]
                    for finished_product in as_completed(future_product):
                        product_data = finished_product.result()
                        data_insert(connection_engine, product_data)
            except Exception as exc:
                print(exc)


##################################################
# pipeline
##################################################

def run_pipeline(connection_engine):
    engine = connection_engine
    last_valid_page = find_last_valid_page("https://www.tokopedia.com/unilever/product")
    collect_active_product_links_parallel_executor("https://www.tokopedia.com/unilever/product", last_valid_page, num_processes = 5)