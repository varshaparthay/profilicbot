import os
import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import openai
import json
import re
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize OpenAI client
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=api_key)

# Step 1: Fetch and parse the page content
# Handles Cloudflare with cloudscraper fallback

def fetch_page_content(url: str) -> tuple[str, str]:
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

    # Gather visible text elements
    elems = soup.find_all(["h1", "h2", "h3", "p", "li"])
    text_parts = [e.get_text().strip() for e in elems if e.get_text().strip()]

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
        "  \"description\": string,            // Include ALL product information: overview, benefits, features, claims, etc.\n"
        "  \"ingredients\": [string],\n"
        "  \"modeOfUse\": string,\n"
        "  \"treatedConditions\": [string],    // Conditions or diseases it treats or alleviates\n"
        "  \"symptoms\": [string],             // Symptoms it claims to address\n"
        "  \"diagnosticUse\": string,          // Any diagnostic relevance or 'None'\n"
        "}\n"
        "Rules: If a field can't be determined, set to 'Missing'. "
        "For description, combine all relevant product details, benefits, features, and claims into a comprehensive summary.\n"
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
        max_tokens=5000
    )
    content = resp.choices[0].message.content.strip()
    # Remove markdown code fences if present
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\n", "", content)
        content = re.sub(r"\n```$", "", content)
    
    # Handle truncated JSON by attempting to fix common issues
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # Try to fix truncated JSON by finding the last complete field
        try:
            # Find the last complete field before truncation
            last_comma = content.rfind(',')
            if last_comma > 0:
                # Try to close the JSON properly
                truncated_content = content[:last_comma] + '\n}'
                return json.loads(truncated_content)
        except json.JSONDecodeError:
            pass
        
        raise ValueError(f"Failed to parse JSON after repair attempts: {content}")

# Wrapper to fetch page and extract JSON

def analyze_url(url: str) -> dict:
    # Handle null/empty URLs including pandas NaN values
    if pd.isna(url) or not url or str(url).strip() == '' or str(url).lower() in ['none', 'null', 'nan']:
        return None
    
    name, description = fetch_page_content(url)
    return extract_structure(name, description)

def process_single_product(row_data: tuple) -> dict:
    """Process a single product row - designed for parallel execution"""
    index, row_dict = row_data
    name = row_dict['name']
    url = row_dict['url']
    
    try:
        result = analyze_url(url)
        if result and isinstance(result, dict):
            # Store the entire JSON result as the description
            feligibot_description = json.dumps(result, ensure_ascii=False)
        else:
            feligibot_description = None
    except Exception as e:
        print(f"Error processing {name}: {e}")
        feligibot_description = None
    
    # Create result with all original columns plus feligibot_description containing full JSON
    result_dict = row_dict.copy()
    result_dict['index'] = index
    result_dict['feligibot_description'] = feligibot_description
    
    return result_dict


def process_csv(input_file: str, output_file: str = None, max_workers: int = 10, batch_size: int = 100) -> None:
    """Process CSV file with parallel processing for large datasets"""
    # Read input CSV
    try:
        df = pd.read_csv(input_file)
        if 'name' not in df.columns or 'url' not in df.columns:
            raise ValueError("CSV must contain 'name' and 'url' columns")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Set default output file name
    if not output_file:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_with_descriptions.csv"
    
    # Setup progress tracking files
    temp_output = f"{os.path.splitext(output_file)[0]}_temp.csv"
    progress_file = f"{os.path.splitext(output_file)[0]}_progress.txt"
    
    # Check for existing progress
    start_index = 0
    results = []
    if os.path.exists(temp_output) and os.path.exists(progress_file):
        try:
            existing_df = pd.read_csv(temp_output)
            results = existing_df.to_dict('records')
            with open(progress_file, 'r') as f:
                start_index = int(f.read().strip())
            print(f"Resuming from index {start_index} (found {len(results)} existing results)")
        except Exception as e:
            print(f"Could not resume from previous run: {e}")
            start_index = 0
            results = []
    
    print(f"Processing {len(df)} products with {max_workers} parallel workers...")
    
    # Prepare data for parallel processing - include all row data
    remaining_data = [(index, row.to_dict()) for index, row in df.iloc[start_index:].iterrows()]
    
    try:
        # Process in parallel with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_data = {executor.submit(process_single_product, row_data): row_data for row_data in remaining_data}
            
            completed_count = start_index
            batch_results = []
            
            # Process completed tasks
            for future in as_completed(future_to_data):
                row_data = future_to_data[future]
                try:
                    result = future.result()
                    batch_results.append(result)
                    completed_count += 1
                    
                    print(f"Completed [{completed_count}/{len(df)}]: {result['name']}")
                    
                    # Save progress in batches
                    if len(batch_results) >= batch_size:
                        # Sort by original index to maintain order
                        batch_results.sort(key=lambda x: x['index'])
                        # Remove index field but keep all other columns
                        for res in batch_results:
                            result_dict = res.copy()
                            result_dict.pop('index', None)
                            results.append(result_dict)
                        
                        # Save intermediate results
                        temp_df = pd.DataFrame(results)
                        temp_df.to_csv(temp_output, index=False)
                        
                        # Update progress
                        with open(progress_file, 'w') as f:
                            f.write(str(completed_count))
                        
                        print(f"Progress saved: {completed_count}/{len(df)} completed")
                        batch_results = []
                        
                except Exception as e:
                    print(f"Error processing {row_data[1]}: {e}")
                    completed_count += 1
            
            # Handle remaining batch results
            if batch_results:
                batch_results.sort(key=lambda x: x['index'])
                # Remove index field but keep all other columns
                for res in batch_results:
                    result_dict = res.copy()
                    result_dict.pop('index', None)
                    results.append(result_dict)
    
    except Exception as e:
        print(f"\nError during parallel processing: {e}")
        print(f"Progress saved in {temp_output}")
        return
    
    # Sort final results by original index to maintain order (if index exists)
    if results and 'index' in results[0]:
        results.sort(key=lambda x: x['index'])
    
    # Remove the index field before saving final output (if any remaining)
    for result in results:
        result.pop('index', None)
    
    # Save final results
    output_df = pd.DataFrame(results)
    output_df.to_csv(output_file, index=False)
    print(f"\nFinal results saved to: {output_file}")
    
    # Clean up temporary files
    for temp_file in [temp_output, progress_file]:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    # Show summary
    success_count = output_df['feligibot_description'].notna().sum()
    print(f"Successfully processed {success_count}/{len(results)} products")


# CLI interface
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 2:
        # CSV mode: python scrape_single_url.py <input_csv> [output_csv] [max_workers]
        input_csv = sys.argv[1]
        output_csv = sys.argv[2] if len(sys.argv) > 2 else None
        max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        print(f"Processing CSV file: {input_csv} with {max_workers} workers")
        process_csv(input_csv, output_csv, max_workers)
    else:
        # Single URL mode (for testing)
        url = "https://www.drunkelephant.com/collections/skincare/plump-c-tripeptide-lippe-mask-812343036086.html?cgid=products-allproducts-skincare"
        result = analyze_url(url)
        print(json.dumps(result, indent=2))

# Requirements:
# pip install requests beautifulsoup4 openai cloudscraper
