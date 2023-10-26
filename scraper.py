import csv
import logging
import random
import re
import time
import traceback
import threading
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from threading import Thread
import queue

scraped_data = queue.Queue()


def createDriver():
    # ser = Service(f"C:\Program Files (x86)\chromedriver.exe")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.121 Safari/537.36"
    )
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-error")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_experimental_option(
        "prefs", {"profile.managed_default_content_settings.images": 2}
    )
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-popup-blocking")
    driver = webdriver.Chrome(options=options)

    return driver


def cnbc_scrap(urls, driver, start, end, thread_index):
    now = datetime.now()
    date_str = now.date().strftime("%Y%m%d")
    logging.basicConfig(
        filename=f"logs_{date_str}.log",
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    count = start + 1

    if not check_internet_connection():
        print("No internet connection. Waiting for connection...")
        logging.error("No internet connection. Waiting for connection...")
    else:
        for url in urls[start:end]:
            try:
                driver.get(url)
                wait = WebDriverWait(driver, 20)
                try:
                    make_it = driver.find_element(
                        By.CLASS_NAME, "MakeItGlobalNav-styles-makeit-logo--sXqSs"
                    )
                    make_it_logo = make_it.get_attribute("href")
                except NoSuchElementException:
                    logging.error(f"{url} - No Make It Logo found")
                    make_it_logo = None
                    pass

                if make_it_logo is not None:
                    logging.info("This link is CNBC Make It")
                elif "select" in url:
                    logging.info("This link is CNBC Select")
                time.sleep(random.randint(1, 3))
                if "select" in url:
                    try:
                        author_element = driver.find_element(
                            By.XPATH,
                            "//div[@class='Author-styles-select-authorNameAndSocial--C51G7']",
                        )
                        html = author_element.get_attribute("innerHTML")
                        soup = BeautifulSoup(html, "html.parser")

                        author_name = soup.find(
                            "a", {"class": "Author-styles-select-authorName--IoDuN"}
                        ).text
                    except NoSuchElementException:
                        logging.error(f"{url} - No Author found")
                        author_name = "No Author Name Available"
                        pass
                    try:
                        headline = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-styles-select-headline--n2eyV"
                        ).text
                    except NoSuchElementException:
                        headline = "No Headline Available"
                        logging.error(f"{url} - No Headline found")
                        pass
                    try:
                        category = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-styles-select-eyebrow--Yjj64"
                        ).text
                    except NoSuchElementException:
                        logging.error(f"{url} - No Category found")
                        category = "No Category Available"
                        pass
                    try:
                        time_div = driver.find_element(
                            By.XPATH,
                            "//div[@class='ArticleHeader-styles-select-time--dEL7X']",
                        )
                        html = time_div.get_attribute("innerHTML")
                        soup = BeautifulSoup(html, "html.parser")
                        time_text = soup.time.get_text()

                        if "Published" in time_text:
                            time_pattern = re.compile(r"Published (.*)")
                            time_match = time_pattern.search(time_text)
                        else:
                            time_pattern = re.compile(r"Updated (.*)")
                            time_match = time_pattern.search(time_text)

                        if time_match:
                            time_str = time_match.group(1)
                            time_obj = datetime.strptime(time_str, "%a, %b %d %Y")
                            time_iso = time_obj.strftime("%Y-%m-%dT%H:%M:%S+0000")

                        published_time = time_iso
                    except NoSuchElementException:
                        logging.error(f"{url} - No Date found")
                        published_time = "No Date Available"
                        pass
                elif make_it_logo is not None:
                    try:
                        author_element = driver.find_element(
                            By.CLASS_NAME, "Author-styles-makeit-authorName--_ANaL"
                        )
                        author_soup = BeautifulSoup(
                            author_element.get_attribute("innerHTML"), "html.parser"
                        )
                        author_name = author_soup.text.strip()
                    except NoSuchElementException:
                        logging.error(f"{url} - No Author found")
                        author_name = "No Author Name Available"
                        pass
                    try:
                        headline = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-styles-makeit-headline--l_iUX"
                        ).text
                    except NoSuchElementException:
                        headline = "No Headline Available"
                        logging.error(f"{url} - No Headline found")
                        pass
                    try:
                        category = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-styles-makeit-eyebrow--Degp4"
                        ).text
                    except NoSuchElementException:
                        logging.error(f"{url} - No Category found")
                        category = "No Category Available"
                        pass
                    try:
                        time_element = driver.find_element(
                            By.CSS_SELECTOR, 'time[itemprop="datePublished"]'
                        )
                        published_time = time_element.get_attribute("datetime")
                    except NoSuchElementException:
                        logging.error(f"{url} - No Date found")
                        published_time = "No Date Available"
                        pass
                else:
                    try:
                        author_element = driver.find_element(
                            By.CLASS_NAME, "Author-authorName"
                        )
                        author_soup = BeautifulSoup(
                            author_element.get_attribute("innerHTML"), "html.parser"
                        )
                        author_name = author_soup.text.strip()
                    except NoSuchElementException:
                        try:
                            img_elem = driver.find_element(
                                By.XPATH, '//div[@class="Author-author"]//img'
                            )
                            img_alt = img_elem.get_attribute("alt")
                            author_name = img_alt
                        except NoSuchElementException:
                            logging.error(f"{url} - No Author found")
                            author_name = "No Author Name Available"
                            pass
                    try:
                        headline = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-headline"
                        ).text
                    except NoSuchElementException:
                        headline = "No Headline Available"
                        logging.error(f"{url} - No Headline found")
                        pass
                    try:
                        category = driver.find_element(
                            By.CLASS_NAME, "ArticleHeader-eyebrow"
                        ).text
                    except NoSuchElementException:
                        logging.error(f"{url} - No Category found")
                        category = "No Category Available"
                        pass
                    try:
                        time_element = driver.find_element(
                            By.CSS_SELECTOR, 'time[itemprop="datePublished"]'
                        )
                        published_time = time_element.get_attribute("datetime")
                    except NoSuchElementException:
                        logging.error(f"{url} - No Date found")
                        published_time = "No Date Available"
                        pass
                try:
                    group_div = wait.until(
                        EC.presence_of_all_elements_located(
                            (By.XPATH, "//div[@class='group']")
                        )
                    )
                    paragraphs = []
                    full_content = ""
                    for div in group_div:
                        html = div.get_attribute("innerHTML")
                        soup = BeautifulSoup(html, "html.parser")
                        paragraphs.extend([p.text.strip() for p in soup.find_all("p")])
                        full_content = " ".join(paragraphs)
                except NoSuchElementException:
                    full_content = "No Content Available"
                    logging.error(f"{url} - No Content found")
                    pass
                # Key points/summary
                try:
                    key_points_div = driver.find_element(
                        By.CLASS_NAME, "RenderKeyPoints-list"
                    )
                    key_points_html = key_points_div.get_attribute("innerHTML")
                    soup = BeautifulSoup(key_points_html, "html.parser")
                    summary_list = [li.text.strip() for li in soup.find_all("li")]
                    summary = " ".join(summary_list)
                except NoSuchElementException:
                    logging.error(f"{url} - No Summary found")
                    summary = "No Summary Available"
                    pass

                scraped_item = {
                    "URL": url,
                    "Author": author_name,
                    "Headline": headline,
                    "Category": category,
                    "Published Time": published_time,
                    "Content": full_content,
                    "Summary": summary,
                }

                # Put the dictionary into the queue
                scraped_data.put(scraped_item)

                print(f"[Thread {thread_index}] {headline} | {count} - {end} | SUCCESS")

                count += 1

            except ElementClickInterceptedException:
                print(f"Element Click Exception Error in {url}")
                logging.exception(f"Element Click Exception Error in {url}")
                continue
            except TimeoutException:
                logging.exception(f"Timeout exception in {url}")
                continue
            except Exception as e:
                print(f"Error in {url} --> {e}")
                logging.exception(f"Error in {url} --> {e}\n{traceback.format_exc()}")
                continue

    def write_to_csv():
        csv_filename = f"cnbc_news_scraper/scraped_data_{date_str}.csv"

        while True:
            try:
                scraped_item = scraped_data.get(block=False)
                with open(csv_filename, "a", newline="", encoding="utf-8") as csv_file:
                    writer = csv.writer(csv_file)

                    if csv_file.tell() == 0:
                        header = [
                            "URL",
                            "Author",
                            "Headline",
                            "Category",
                            "Published Time",
                            "Content",
                            "Summary",
                        ]
                        writer.writerow(header)

                    writer.writerow(scraped_item.values())

            except queue.Empty:
                time.sleep(1)

    csv_thread = Thread(target=write_to_csv)
    csv_thread.start()


def check_internet_connection(
    url="http://www.google.com", timeout=10, retry_interval=20
):
    while True:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            print("Internet connection is available.")
            return True
        except requests.HTTPError as exception:
            print("HTTP error occurred while checking internet connection:", exception)
        except requests.ConnectionError as exception:
            print("Error connecting to the server:", exception)
        except requests.Timeout as exception:
            print("Request timed out:", exception)
        except requests.RequestException as exception:
            print("An error occurred:", exception)

        print(f"Retrying connection check in {retry_interval} seconds...")
        time.sleep(retry_interval)


def multithread_scrap(num_threads, urls):
    total_urls = len(urls)
    urls_per_thread = total_urls // num_threads

    # Create threads and start them
    threads = []
    for i in range(num_threads):
        start = i * urls_per_thread
        end = start + urls_per_thread if i != num_threads - 1 else total_urls
        driver = createDriver()
        t = threading.Thread(target=cnbc_scrap, args=(urls, driver, start, end, i + 1))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()


def main():
    urls = [
        "https://www.cnbc.com/2023/10/14/these-top-colleges-promise-no-student-loan-debt.html",
        "https://www.cnbc.com/2023/10/14/how-snickers-maker-mars-prepares-for-halloween-and-trick-or-treat.html",
    ]
    num_threads = 2
    multithread_scrap(num_threads, urls)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    difference = end_time - start_time
    m, s = divmod(difference, 60)
    h, m = divmod(m, 60)
    print(f"Total time used: {int(h)}h {int(m)}m {int(s)}s")
