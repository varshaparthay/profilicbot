import requests
from bs4 import BeautifulSoup
import csv
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://fsastore.com/fsa-eligibility-list/a",
    "Connection": "keep-alive",
}

BASE_URL = "https://fsastore.com"

def fetch_json(letter, page=1):
    url = f"{BASE_URL}/on/demandware.store/Sites-FSASTORE-Site/default/Elist-ShowAjax?cgid=el-{letter}&page={page}"
    print(f"Fetching {url}")
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def extract_products(html):
    soup = BeautifulSoup(html, "html.parser")
    products = []
    items = soup.find_all("div", class_="c-elist__col")
    for item in items:
        data_href = item.get("data-href")
        name_div = item.find("div", class_="c-elist__card__heading__title")
        status_span = item.find("span", class_="c-elist__card__heading__type")
        if data_href and name_div and status_span:
            name = name_div.get_text(strip=True)
            url = data_href
            status = status_span.get_text(strip=True)
            products.append([name, url, status])
    return products

def scrape_letter(letter):
    page = 1
    results = []
    while True:
        data = fetch_json(letter, page)
        results.extend(extract_products(data["html"]))
        if not data.get("showLoadMore") or not data.get("loadMoreUrl"):
            break
        page += 1
        time.sleep(5)
    return results

def main():
    with open("fsa_products.csv", "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Product name", "Website", "Eligibility"])
        for letter in 'abcdefghijklmnopqrstuvwxyz':
            print(f"Scraping letter {letter} ...")
            rows = scrape_letter(letter)
            writer.writerows(rows)
            print(f"Done letter {letter}, got {len(rows)} products.")

if __name__ == "__main__":
    main()
