import os
import requests
from requests.exceptions import HTTPError, RequestException
from bs4 import BeautifulSoup
import openai
from urllib.parse import urljoin, urlparse, quote
import json
import re
from dotenv import load_dotenv
from typing import List, Optional, Tuple
import time
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

class ProductURLFinder:
    def __init__(self, use_ai: bool = True):
        self.use_ai = use_ai
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.0.0 Safari/537.36"
            )
        }
        
        if use_ai:
            api_key = os.getenv("OPENAI_API_KEY")
            if api_key:
                self.client = openai.OpenAI(api_key=api_key)
            else:
                self.use_ai = False
                print("Warning: OpenAI API key not found, AI matching disabled")

    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage with Cloudflare fallback"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except HTTPError:
            try:
                import cloudscraper
                scraper = cloudscraper.create_scraper()
                resp = scraper.get(url, timeout=10)
                resp.raise_for_status()
                return BeautifulSoup(resp.text, "html.parser")
            except Exception:
                return None
        except Exception:
            return None

    def _find_search_form(self, soup: BeautifulSoup) -> Optional[dict]:
        """Find search form on the page"""
        # Look for search forms
        search_forms = soup.find_all("form")
        for form in search_forms:
            # Check if form has search-related inputs
            inputs = form.find_all("input")
            for inp in inputs:
                if inp.get("type") in ["search", "text"] and any(
                    keyword in str(inp).lower() 
                    for keyword in ["search", "query", "q", "find"]
                ):
                    return {
                        "action": form.get("action", ""),
                        "method": form.get("method", "get"),
                        "input_name": inp.get("name", "q")
                    }
        return None

    def _try_site_search(self, base_url: str, product_name: str) -> List[str]:
        """Try to use the website's built-in search"""
        soup = self._fetch_page(base_url)
        if not soup:
            return []

        search_form = self._find_search_form(soup)
        candidate_urls = []

        # Try common search URL patterns
        search_patterns = [
            f"/search?q={quote(product_name)}",
            f"/search?query={quote(product_name)}",
            f"/products/search?q={quote(product_name)}",
            f"/?s={quote(product_name)}",
            f"/search/{quote(product_name.replace(' ', '-'))}",
        ]

        # Add form-based search if found
        if search_form:
            action = search_form["action"] or "/search"
            param = search_form["input_name"]
            search_patterns.append(f"{action}?{param}={quote(product_name)}")

        for pattern in search_patterns:
            search_url = urljoin(base_url, pattern)
            soup = self._fetch_page(search_url)
            if soup:
                candidate_urls.extend(self._extract_product_links(soup, base_url))
                time.sleep(0.5)  # Rate limiting

        return list(set(candidate_urls))

    def _is_category_page(self, url: str) -> bool:
        """Check if URL looks like a category/listing page rather than individual product"""
        url_lower = url.lower()
        
        # Strong indicators of category pages
        category_indicators = [
            "/collections/", "/category/", "/categories/", "/shop/", "/store/",
            "/catalog/", "/all-products", "/products/", "/skincare/", "/makeup/",
            "/hair/", "/body/", "/treatments/", "/brands/", "/sale/"
        ]
        
        # Strong indicators of individual product pages  
        product_indicators = [
            ".html", "/product-", "-product", "/item/", "/p/",
            "/sku/", "/model/", "/detail/", "/view/"
        ]
        
        has_category_indicator = any(indicator in url_lower for indicator in category_indicators)
        has_product_indicator = any(indicator in url_lower for indicator in product_indicators)
        
        # If it has product indicators, it's likely a product page
        if has_product_indicator:
            return False
            
        # If it has category indicators and no product indicators, it's likely a category
        if has_category_indicator:
            return True
            
        # Additional heuristics: long URLs with multiple segments often indicate products
        path_segments = [seg for seg in urlparse(url).path.split('/') if seg]
        if len(path_segments) >= 3 and not has_category_indicator:
            return False  # Likely a specific product
            
        return has_category_indicator

    def _extract_product_links(self, soup: BeautifulSoup, base_url: str, deep_crawl: bool = False) -> List[str]:
        """Extract potential product links from a page"""
        product_links = []
        category_links = []
        
        # Look for links that might be products
        for link in soup.find_all("a", href=True):
            href = link.get("href")
            if not href:
                continue
                
            # Convert to absolute URL
            full_url = urljoin(base_url, href)
            
            # Skip non-product links
            if any(skip in href.lower() for skip in [
                "javascript:", "mailto:", "#", "tel:", "sms:",
                "/cart", "/checkout", "/account", "/login", "/register",
                "/about", "/contact", "/faq", "/support", "/blog",
                ".pdf", ".jpg", ".png", ".gif", ".css", ".js"
            ]):
                continue
            
            # Separate category pages from product pages
            if self._is_category_page(full_url):
                if deep_crawl:
                    category_links.append(full_url)
            elif any(indicator in href.lower() for indicator in [
                "/product", "/item", "/p/", ".html", "-product"
            ]):
                product_links.append(full_url)
            
            # Also check if link has product-like structure or text
            link_text = link.get_text(strip=True)
            if link_text and len(link_text) > 5 and not self._is_category_page(full_url):
                # Check if it's a specific product link
                if any(char in href for char in ["-", "_"]) and len(href.split("/")[-1]) > 10:
                    product_links.append(full_url)
        
        # If we're doing deep crawl and found few direct products, explore categories
        if deep_crawl and len(product_links) < 5 and category_links:
            print(f"Crawling {len(category_links)} category pages for products...")
            for category_url in category_links[:3]:  # Limit to avoid too much crawling
                cat_soup = self._fetch_page(category_url)
                if cat_soup:
                    product_links.extend(self._extract_product_links(cat_soup, base_url, deep_crawl=False))
                    time.sleep(0.5)
        
        return list(set(product_links[:50]))  # Limit but allow more candidates

    def _crawl_product_pages(self, base_url: str, max_depth: int = 2, max_pages: int = 50) -> List[str]:
        """Dynamically discover and crawl product pages using breadth-first exploration"""
        product_urls = set()
        visited_urls = set()
        to_visit = [(base_url, 0)]  # (url, depth)
        
        print(f"Starting dynamic exploration of {base_url}...")
        
        while to_visit and len(visited_urls) < max_pages:
            current_url, depth = to_visit.pop(0)
            
            if current_url in visited_urls or depth > max_depth:
                continue
                
            visited_urls.add(current_url)
            soup = self._fetch_page(current_url)
            if not soup:
                continue
                
            print(f"Exploring [{len(visited_urls)}/{max_pages}] depth {depth}: {current_url}")
            
            # Extract product links from current page
            page_products = self._extract_product_links(soup, base_url, deep_crawl=False)
            product_urls.update(page_products)
            
            # Find navigation/category links to explore further
            if depth < max_depth:
                nav_links = self._find_navigation_links(soup, base_url)
                for link in nav_links:
                    if link not in visited_urls:
                        to_visit.append((link, depth + 1))
            
            time.sleep(0.3)  # Rate limiting
        
        print(f"Exploration complete: visited {len(visited_urls)} pages, found {len(product_urls)} products")
        return list(product_urls)
    
    def _find_navigation_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find navigation and category links that likely lead to more products"""
        nav_links = set()
        
        # Look for navigation elements
        nav_selectors = [
            "nav a", "header a", ".navigation a", ".nav a", ".menu a",
            ".category a", ".categories a", ".collection a", ".collections a",
            "[class*='nav'] a", "[class*='menu'] a", "[class*='category'] a",
            "[id*='nav'] a", "[id*='menu'] a", "[href*='product']", "[href*='collection']",
            "[href*='category']", "[href*='shop']", "[href*='store']"
        ]
        
        for selector in nav_selectors:
            try:
                for link in soup.select(selector):
                    href = link.get('href')
                    if not href:
                        continue
                    
                    full_url = urljoin(base_url, href)
                    
                    # Filter for potentially useful navigation links
                    if self._is_useful_navigation_link(full_url, base_url):
                        nav_links.add(full_url)
            except Exception:
                continue
        
        return list(nav_links)[:20]  # Limit to prevent explosion
    
    def _is_useful_navigation_link(self, url: str, base_url: str) -> bool:
        """Check if a link is worth exploring for products"""
        # Must be same domain
        from urllib.parse import urlparse
        if urlparse(url).netloc != urlparse(base_url).netloc:
            return False
        
        url_lower = url.lower()
        
        # Skip obviously non-product links
        skip_patterns = [
            '/cart', '/checkout', '/account', '/login', '/register', '/contact',
            '/about', '/faq', '/support', '/blog', '/news', '/press',
            '.pdf', '.jpg', '.png', '.gif', '.css', '.js', '.xml',
            'javascript:', 'mailto:', 'tel:', '#', '/search',
            '/privacy', '/terms', '/shipping', '/return'
        ]
        
        if any(skip in url_lower for skip in skip_patterns):
            return False
        
        # Prioritize product-related paths
        useful_patterns = [
            'product', 'collection', 'category', 'shop', 'store',
            'catalog', 'item', 'skin', 'care', 'beauty', 'cosmetic'
        ]
        
        return any(pattern in url_lower for pattern in useful_patterns)

    def _ai_match_product(self, product_name: str, candidate_urls: List[str]) -> Optional[str]:
        """Use AI to find the best matching product URL"""
        if not self.use_ai or not candidate_urls:
            return None

        # Get page titles/content for each URL
        url_info = []
        for url in candidate_urls[:10]:  # Limit to avoid token limits
            soup = self._fetch_page(url)
            if soup:
                title = soup.find("title")
                title_text = title.get_text().strip() if title else ""
                
                h1 = soup.find("h1")
                h1_text = h1.get_text().strip() if h1 else ""
                
                url_info.append({
                    "url": url,
                    "title": title_text,
                    "h1": h1_text
                })

        if not url_info:
            return None

        # Create AI prompt
        system_msg = (
            "You are a product matching expert. Given a product name and a list of URLs "
            "with their titles and main headings, identify which URL best matches the product. "
            "Respond with just the URL, or 'None' if no good match exists."
        )
        
        user_content = f"Product name: {product_name}\n\nCandidate URLs:\n"
        for info in url_info:
            user_content += f"URL: {info['url']}\nTitle: {info['title']}\nHeading: {info['h1']}\n\n"

        try:
            resp = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_content}
                ],
                temperature=0.0,
                max_tokens=2000
            )
            result = resp.choices[0].message.content.strip()
            return result if result != "None" and result.startswith("http") else None
        except Exception:
            return None

    def _simple_text_match(self, product_name: str, candidate_urls: List[str]) -> Optional[str]:
        """Simple text-based matching fallback"""
        # Clean and normalize product name
        import re
        cleaned_name = re.sub(r'[™®©]', '', product_name)  # Remove trademark symbols
        # Split on both spaces and hyphens to handle "Co-Cleanser" -> "co", "cleanser"
        cleaned_name = re.sub(r'[^a-zA-Z0-9\s-]', ' ', cleaned_name)
        words = re.split(r'[\s-]+', cleaned_name)
        product_words = set([word.lower().strip() for word in words if len(word.strip()) > 1])
        
        # Debug logging for specific product
        debug_product = "laini latherless conditioning co-cleanser midi"
        is_debug = debug_product in product_name.lower()
        if is_debug:
            print(f"\n🔍 DEBUG: Matching '{product_name}'")
            print(f"   Cleaned words: {product_words}")
        best_match = None
        best_score = 0

        for url in candidate_urls:
            soup = self._fetch_page(url)
            if not soup:
                continue

            # Get page text content including URL path
            title = soup.find("title")
            title_text = title.get_text().strip().lower() if title else ""
            
            h1 = soup.find("h1") 
            h1_text = h1.get_text().strip().lower() if h1 else ""
            
            # Extract product name from URL path (e.g., ceramighty-af-eye-balm)
            from urllib.parse import urlparse
            url_path = urlparse(url).path.lower()
            url_words = re.sub(r'[^a-zA-Z0-9\s-]', ' ', url_path).split()
            url_text = ' '.join(url_words)
            
            combined_text = title_text + " " + h1_text + " " + url_text
            text_words = set([word.strip() for word in combined_text.split() if len(word.strip()) > 1])
            
            # Calculate overlap score
            overlap = len(product_words.intersection(text_words))
            if is_debug:
                print(f"   Candidate: {url}")
                print(f"   Text words: {list(text_words)[:10]}...")  # Show first 10
                print(f"   Overlap: {overlap} words: {product_words.intersection(text_words)}")
            
            if overlap > best_score:
                best_score = overlap
                best_match = url

        # More flexible matching: require at least 1 significant word match
        # Skip very common words for better matching
        significant_words = [word for word in product_words 
                           if len(word) > 2 and word.lower() not in ['the', 'and', 'for', 'with', 'from', 'midi', 'mini', 'full', 'size']]
        
        if len(significant_words) >= 3:
            # For longer product names, require at least 1/3 of significant words
            min_required_matches = max(1, len(significant_words) // 3)
        else:
            # For shorter names, require at least 1 match
            min_required_matches = 1
        
        if is_debug:
            print(f"   Significant words: {significant_words}")
            print(f"   Best match: {best_match} (score: {best_score}, required: {min_required_matches})")
            
        return best_match if best_score >= min_required_matches else None

    def find_product_url(self, product_name: str, website_url: str) -> Optional[str]:
        """Main method to find product URL"""
        print(f"Searching for '{product_name}' on {website_url}")
        
        # Step 1: Try site search
        search_candidates = self._try_site_search(website_url, product_name)
        print(f"Found {len(search_candidates)} candidates from site search")
        
        # Step 2: Crawl product pages if search didn't work
        if len(search_candidates) < 3:
            crawl_candidates = self._crawl_product_pages(website_url)
            search_candidates.extend(crawl_candidates)
            print(f"Found {len(search_candidates)} total candidates after crawling")
        
        if not search_candidates:
            print("No candidate URLs found")
            return None
        
        # Step 3: Use AI to match if available
        if self.use_ai:
            ai_match = self._ai_match_product(product_name, search_candidates)
            if ai_match:
                print(f"AI matched: {ai_match}")
                return ai_match
        
        # Step 4: Fallback to simple text matching
        text_match = self._simple_text_match(product_name, search_candidates)
        if text_match:
            print(f"Text matched: {text_match}")
            return text_match
        
        print("No good matches found")
        return None

    def _process_single_product(self, product_data: tuple, website_url: str, all_candidates: List[str]) -> dict:
        """Process a single product - designed for parallel execution"""
        index, row_dict = product_data
        product_name = row_dict['name']
        existing_url = row_dict.get('url', '')
        
        # Skip if URL already exists
        if existing_url and pd.notna(existing_url) and str(existing_url).strip() != '':
            print(f"Skipping '{product_name}' - URL already exists: {existing_url}")
            result = row_dict.copy()
            result['index'] = index
            return result
        
        try:
            # Try site search for specific product
            search_candidates = self._try_site_search(website_url, product_name)
            combined_candidates = list(set(search_candidates + all_candidates))
            
            found_url = None
            
            if combined_candidates:
                # Try AI matching first
                if self.use_ai:
                    ai_match = self._ai_match_product(product_name, combined_candidates)
                    if ai_match:
                        found_url = ai_match
                
                # Fallback to text matching if AI didn't work
                if not found_url:
                    text_match = self._simple_text_match(product_name, combined_candidates)
                    if text_match:
                        found_url = text_match
            
            # Create result with all original columns plus found URL
            result = row_dict.copy()
            result['index'] = index
            result['url'] = found_url  # This will overwrite existing URL column or add new one
            
            return result
        
        except Exception as e:
            print(f"Error processing {product_name}: {e}")
            result = row_dict.copy()
            result['index'] = index
            result['url'] = None
            return result

    def process_csv_file(self, csv_file_path: str, website_url: str, output_file_path: str = None, batch_size: int = 10, max_workers: int = 8) -> None:
        """Process CSV file with product names using parallel processing and incremental saving"""
        # Read CSV file
        try:
            df = pd.read_csv(csv_file_path)
            if 'name' not in df.columns:
                raise ValueError("CSV must contain 'name' column")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return
        
        # Set up output files with folder name pattern
        input_dir = os.path.dirname(csv_file_path)
        folder_name = os.path.basename(input_dir)
        
        if output_file_path:
            final_output = output_file_path
        else:
            final_output = os.path.join(input_dir, f"{folder_name}_results.csv")
            
        temp_output = os.path.join(input_dir, f"{folder_name}_temp.csv")
        progress_file = os.path.join(input_dir, f"{folder_name}_progress.txt")
        
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
        
        products_needing_urls = 0  # Initialize products_needing_urls
        # Get all candidates once to avoid repeated crawling (only if there are products needing URLs)
        if start_index == 0 and products_needing_urls > 0:
            print(f"Crawling {website_url} for product candidates...")
            all_candidates = self._crawl_product_pages(website_url)
            print(f"Found {len(all_candidates)} total candidate URLs")
            # Save candidates for resume functionality
            candidates_file = os.path.join(input_dir, f"{folder_name}_candidates.json")
            with open(candidates_file, 'w') as f:
                json.dump(all_candidates, f)
        elif products_needing_urls > 0:
            # Load saved candidates
            candidates_file = os.path.join(input_dir, f"{folder_name}_candidates.json")
            try:
                with open(candidates_file, 'r') as f:
                    all_candidates = json.load(f)
                print(f"Loaded {len(all_candidates)} candidate URLs from previous run")
            except Exception as e:
                print(f"Could not load candidates, re-crawling: {e}")
                all_candidates = self._crawl_product_pages(website_url)
        else:
            all_candidates = []
            # Load saved candidates
            candidates_file = os.path.join(input_dir, f"{folder_name}_candidates.json")
            try:
                with open(candidates_file, 'r') as f:
                    all_candidates = json.load(f)
                print(f"Loaded {len(all_candidates)} candidate URLs from previous run")
            except Exception as e:
                print(f"Could not load candidates, re-crawling: {e}")
                all_candidates = self._crawl_product_pages(website_url)
        
        print(f"Processing {len(df)} products with {max_workers} parallel workers...")
        
        # Count products that need URL finding
        products_needing_urls = 0
        for index, row in df.iterrows():
            existing_url = row.get('url', '')
            if not existing_url or pd.isna(existing_url) or str(existing_url).strip() == '':
                products_needing_urls += 1
        
        print(f"Found {products_needing_urls} products without URLs (out of {len(df)} total)")
        
        if products_needing_urls == 0:
            print("All products already have URLs, skipping crawling phase")
            # Still need to return results with all original data
            results = []
            for index, row in df.iterrows():
                result_dict = row.to_dict()
                result_dict.pop('index', None)  # Remove any existing index
                results.append(result_dict)
            
            # Save final results
            output_df = pd.DataFrame(results)
            output_df.to_csv(final_output, index=False)
            print(f"Results saved to: {final_output}")
            print(f"Summary: All {len(results)} products already had URLs")
            return
        
        # Prepare data for parallel processing - include all row data
        remaining_data = [(index, row.to_dict()) for index, row in df.iloc[start_index:].iterrows()]
        
        try:
            # Process in parallel with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_data = {
                    executor.submit(self._process_single_product, product_data, website_url, all_candidates): product_data 
                    for product_data in remaining_data
                }
                
                completed_count = start_index
                batch_results = []
                
                # Process completed tasks
                for future in as_completed(future_to_data):
                    product_data = future_to_data[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        completed_count += 1
                        
                        # Log result
                        if result['url']:
                            print(f"[{completed_count}/{len(df)}] Found: {result['name']} -> {result['url']}")
                        else:
                            print(f"[{completed_count}/{len(df)}] No match: {result['name']}")
                        
                        # Save progress in batches
                        if len(batch_results) >= batch_size:
                            # Sort by original index to maintain order
                            batch_results.sort(key=lambda x: x['index'])
                            
                            # Add to results (remove index field but keep all other columns)
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
                        print(f"Error processing {product_data[1]}: {e}")
                        completed_count += 1
                
                # Handle remaining batch results
                if batch_results:
                    batch_results.sort(key=lambda x: x['index'])
                    for res in batch_results:
                        result_dict = res.copy()
                        result_dict.pop('index', None)
                        results.append(result_dict)
        
        except Exception as e:
            print(f"\nError during processing: {e}")
            print(f"Progress saved in {temp_output}")
            print(f"Resume by running the script again with the same parameters")
            return
        
        # Create final output DataFrame
        output_df = pd.DataFrame(results)
        
        # Save final results
        output_df.to_csv(final_output, index=False)
        print(f"\nFinal results saved to: {final_output}")
        
        # Clean up temporary files
        candidates_file = os.path.join(input_dir, f"{folder_name}_candidates.json")
        for temp_file in [temp_output, progress_file, candidates_file]:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        # Display summary
        found_count = output_df['url'].notna().sum()
        print(f"\nSummary: Found URLs for {found_count}/{len(results)} products")


def find_product_url(product_name: str, website_url: str, use_ai: bool = True) -> Optional[str]:
    """Convenience function to find a product URL"""
    finder = ProductURLFinder(use_ai=use_ai)
    return finder.find_product_url(product_name, website_url)


def process_products_csv(csv_file_path: str, website_url: str, output_file_path: str = None, use_ai: bool = True, max_workers: int = 8) -> None:
    """Convenience function to process CSV file with product names"""
    finder = ProductURLFinder(use_ai=use_ai)
    finder.process_csv_file(csv_file_path, website_url, output_file_path, max_workers=max_workers)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 3:
        # CSV mode: python find_product_url.py <csv_file> <website_url> [max_workers]
        csv_file = sys.argv[1]
        website = sys.argv[2]
        max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 8
        print(f"Processing CSV file: {csv_file}")
        print(f"Website: {website}")
        print(f"Workers: {max_workers}")
        process_products_csv(csv_file, website, max_workers=max_workers)
    else:
        # Single product mode (original example)
        product = "Limited-Edition Power Infusing Concentrate"
        website = "https://www.shiseido.com/"
        
        result = find_product_url(product, website)
        if result:
            print(f"\nFound product URL: {result}")
        else:
            print("\nProduct URL not found")