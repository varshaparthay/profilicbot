import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
import openai
import cloudscraper
import os
import sys
import json
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import openai

# Replace with your actual OpenAI API key
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=api_key)

# Fetch and parse page content, handling Cloudflare

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
    # Extract product name
    h1 = soup.find("h1")
    name = h1.get_text().strip() if h1 else ""

    # Collect full page text for context
    elems = soup.find_all(["h1", "h2", "h3", "p", "li"])
    text = "\n".join(e.get_text().strip() for e in elems if e.get_text().strip())
    return name, text

# Build assistant prompt according to spec

def build_prompt(name: str, description: str) -> list[dict]:
    system_msg = (
        "You are an expert product analyst specializing in HSA/FSA eligibility. "
        "Extract structured attributes with maximal precision and flag missing data.\n"
        "Respond with JSON only, no extra text. Schema: {\n"
        "  \"name\": string,\n"
        "  \"description\": string,\n"
        "  \"ingredients\": [string],\n"
        "  \"modeOfUse\": string,\n"
        "  \"indications\": [string],\n"
        "  \"treatedConditions\": [string],    // Conditions or diseases it treats or alleviates\n"
        "  \"symptoms\": [string],             // Symptoms it claims to address\n"
        "  \"diagnosticUse\": string,          // Any diagnostic relevance or 'None'\n"
        "}\n"
        "Rules: If a field can't be determined, set to 'Missing'. Use exact regulatoryStatus terms: 'FDA-cleared medical device', 'OTC monograph drug', 'prescription drug', 'dietary supplement', 'cosmetic', 'other:<detail>', or 'Missing'. Distinguish approved indications from marketing claims."
    )
    user_msg = (
        f"INPUT:\n• product_name: {name}\n• product_description: {description}\n"
    )
    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg}
    ]

# Call OpenAI and parse JSON

def extract_structure(name: str, description: str) -> dict:
    messages = build_prompt(name, description)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.0,
        max_tokens=600
    )
    content = resp.choices[0].message.content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON from model: {content}")

# Main


# Requirements:
# pip install requests beautifulsoup4 openai cloudscraper


if __name__ == "__main__":
    import sys
    url = ""
    name, desc = fetch_page_content(url)
    result = extract_structure(name, desc)
    print(json.dumps(result, indent=2))

# Requirements:
# pip install requests beautifulsoup4 openai cloudscraper
