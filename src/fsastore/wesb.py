import os
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import openai
import json
import re

# Initialize OpenAI client
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=api_key)

# Step 1: Fetch and parse the page content
# Handles Cloudflare with cloudscraper fallback

def fetch_page_content(url: str) -> (str, str):
    headers = {"User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    )}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        html = resp.text
    except HTTPError:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(url)
        resp.raise_for_status()
        html = resp.text

    soup = BeautifulSoup(html, "html.parser")
    # Extract product name with fallbacks
    h1 = soup.find("h1")
    if h1 and h1.get_text().strip():
        name = h1.get_text().strip()
    else:
        meta = soup.find("meta", property="og:title") or soup.find(
            "meta", attrs={"name": "title"}
        )
        name = meta["content"].strip() if meta and meta.get("content") else ""

    # Gather visible text elements
    elems = soup.find_all(["h1", "h2", "h3", "p", "li"])
    text_parts = [e.get_text().strip() for e in elems if e.get_text().strip()]

    # Include meta description if present
    meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find(
        "meta", property="og:description"
    )
    if meta_desc and meta_desc.get("content"):
        text_parts.append(meta_desc["content"].strip())

    # Include table content (e.g., supplement facts)
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            if cols:
                rows.append(" | ".join(cols))
        if rows:
            text_parts.append("Table:" + "".join(rows))

    # Include any JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            text_parts.append("JSON-LD:" + json.dumps(ld))
        except Exception:
            continue

    full_text = "\n".join(text_parts)
    return name, full_text

# Step 2: Build structured prompt per teammate spec

def build_prompt(name: str, description: str) -> list[dict]:
    system_msg = (
        "You are an expert product analyst specializing in HSA/FSA eligibility. "
        "Extract structured attributes with maximal precision and flag missing data.\n"
        "Respond with JSON only, no extra text. Schema: {\n"
        "  \"name\": string,\n"
        "  \"description\": string,\n"
        "  \"ingredients\": [string],\n"
        "  \"modeOfUse\": string,\n"
        "  \"treatedConditions\": [string],    // Conditions or diseases it treats or alleviates\n"
        "  \"symptoms\": [string],             // Symptoms it claims to address\n"
        "  \"diagnosticUse\": string,          // Any diagnostic relevance or 'None'\n"
        "}\n"
        "Rules: If a field can't be determined, set to 'Missing'.\n"
    )
    user_content = (
        f"INPUT:\n• product_name: {name}\n• product_description: {description}\n"
    )
    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_content}
    ]

# Step 3: Call OpenAI and parse JSON


def extract_structure(name: str, description: str) -> dict:
    messages = build_prompt(name, description)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.0,
        max_tokens=600
    )
    content = resp.choices[0].message.content.strip()
    # Remove markdown code fences if present
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\n", "", content)
        content = re.sub(r"\n```$", "", content)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        raise ValueError(f"Failed to parse JSON: {content}")

# Wrapper to fetch page and extract JSON

def analyze_url(url: str) -> dict:
    name, description = fetch_page_content(url)
    return extract_structure(name, description)

# CLI interface
if __name__ == "__main__":
    import sys
    url = "https://www.shiseido.com/us/en/vital-perfection-uplifting-and-firming-eye-cream-0730852163799.html"
    result = analyze_url(url)
    print(json.dumps(result, indent=2))

# Requirements:
# pip install requests beautifulsoup4 openai cloudscraper
