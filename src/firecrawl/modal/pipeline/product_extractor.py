#!/usr/bin/env python3
"""
Product Extractor for Modal Pipeline
Extracts comprehensive product data using Firecrawl structured extraction
"""

import time
import os
from typing import Dict, Any

from .config import app, image, secrets, url_queue, product_queue
from .schemas import ProductURL, ExtractedProduct, ProductExtractionSchema

try:
    from firecrawl import FirecrawlApp
except ImportError:
    FirecrawlApp = None

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=300,  # 5 minutes per product
    concurrency_limit=30  # 30 parallel workers
)
def product_extractor_worker():
    """
    Product Extractor Worker
    Continuously processes URLs from url_queue and extracts product data
    """
    # Initialize Firecrawl
    firecrawl = None
    if FirecrawlApp:
        api_key = os.environ.get('FIRECRAWL_API_KEY')
        if api_key:
            firecrawl = FirecrawlApp(api_key=api_key)
    
    if not firecrawl:
        print("❌ Firecrawl not available - cannot extract products")
        return
    
    print("🔍 Product Extractor Worker started - waiting for URLs...")
    
    # Process URLs from queue
    while True:
        try:
            # Get URL from queue (blocks until available)
            product_url: ProductURL = url_queue.get()
            
            if product_url is None:  # Poison pill to stop worker
                print("💊 Received stop signal - shutting down worker")
                break
            
            print(f"📥 Processing: {product_url.url}")
            
            # Extract product data
            extracted_product = _extract_single_product(firecrawl, product_url)
            
            if extracted_product:
                # Queue for categorization
                product_queue.put(extracted_product)
                print(f"✅ Extracted and queued: {extracted_product.name}")
            else:
                print(f"❌ Failed to extract: {product_url.url}")
                
        except Exception as e:
            print(f"❌ Error processing URL: {str(e)}")
            continue

def _extract_single_product(firecrawl, product_url: ProductURL) -> ExtractedProduct:
    """Extract comprehensive product data from a single URL"""
    start_time = time.time()
    
    try:
        print(f"   🤖 Firecrawl scraping: {product_url.url}")
        
        # Firecrawl structured extraction
        scrape_result = firecrawl.scrape_url(
            product_url.url,
            formats=['extract'],
            extract={
                'schema': ProductExtractionSchema.model_json_schema()
            }
        )
        
        # Check if extraction was successful
        success = getattr(scrape_result, 'success', None)
        if not success:
            error_msg = getattr(scrape_result, 'error', 'Unknown error')
            print(f"   ❌ Firecrawl extraction failed: {error_msg}")
            return None
        
        # Get extracted data
        extract_data = getattr(scrape_result, 'extract', {})
        if not extract_data:
            print(f"   ❌ No structured data extracted")
            return None
        
        # Get markdown content for comprehensive description building
        markdown = getattr(scrape_result, 'markdown', '')
        
        # Build comprehensive description
        comprehensive_description = _build_comprehensive_description(extract_data, markdown)
        
        extraction_time = time.time() - start_time
        
        # Create ExtractedProduct object
        extracted_product = ExtractedProduct(
            url=product_url.url,
            batch_id=product_url.batch_id,
            name=extract_data.get('name', product_url.estimated_name),
            description=comprehensive_description,
            structured_data=extract_data,
            extraction_time=extraction_time
        )
        
        print(f"   ✅ Extracted {len(comprehensive_description)} char description in {extraction_time:.1f}s")
        return extracted_product
        
    except Exception as e:
        extraction_time = time.time() - start_time
        print(f"   ❌ Extraction error: {str(e)} (took {extraction_time:.1f}s)")
        return None

def _build_comprehensive_description(extract_data: Dict[str, Any], markdown: str) -> str:
    """
    Build comprehensive 2000+ character product description
    Combines structured data with markdown content for maximum detail
    """
    description_parts = []
    
    # Start with main description
    main_desc = extract_data.get('description', '').strip()
    if main_desc:
        description_parts.append(main_desc)
    
    # Add features
    features = extract_data.get('features', '').strip()
    if features:
        description_parts.append(f"Key Features: {features}")
    
    # Add benefits
    benefits = extract_data.get('benefits', '').strip()
    if benefits:
        description_parts.append(f"Benefits: {benefits}")
    
    # Add ingredients/components
    ingredients = extract_data.get('ingredients', '').strip()
    if ingredients:
        description_parts.append(f"Ingredients/Components: {ingredients}")
    
    # Add usage instructions
    usage = extract_data.get('usage', '').strip()
    if usage:
        description_parts.append(f"Usage Instructions: {usage}")
    
    # Add specifications
    specs = extract_data.get('specifications', '').strip()
    if specs:
        description_parts.append(f"Specifications: {specs}")
    
    # Add medical claims
    medical_claims = extract_data.get('medical_claims', '').strip()
    if medical_claims:
        description_parts.append(f"Health & Medical Claims: {medical_claims}")
    
    # Add warranty/support info
    warranty = extract_data.get('warranty_support', '').strip()
    if warranty:
        description_parts.append(f"Warranty & Support: {warranty}")
    
    # Add additional info
    additional = extract_data.get('additional_info', '').strip()
    if additional:
        description_parts.append(f"Additional Information: {additional}")
    
    # Join all parts
    comprehensive_desc = ' | '.join(description_parts)
    
    # If still too short, supplement with markdown content
    if len(comprehensive_desc) < 1500 and markdown:
        # Extract additional context from markdown
        markdown_lines = [line.strip() for line in markdown.split('\n') if line.strip()]
        
        # Filter out navigation and non-product content
        product_lines = []
        skip_patterns = [
            'navigation', 'menu', 'header', 'footer', 'cart', 'checkout',
            'login', 'account', 'shipping', 'returns', 'privacy', 'terms',
            'subscribe', 'newsletter', 'follow us', 'social', 'contact'
        ]
        
        for line in markdown_lines:
            line_lower = line.lower()
            if len(line) > 20 and not any(skip in line_lower for skip in skip_patterns):
                if any(keyword in line_lower for keyword in [
                    'product', 'benefit', 'feature', 'ingredient', 'use', 'apply',
                    'helps', 'support', 'improve', 'reduce', 'enhance', 'provide',
                    'formula', 'blend', 'extract', 'vitamin', 'mineral', 'supplement'
                ]):
                    product_lines.append(line)
        
        # Add most relevant markdown content
        if product_lines:
            markdown_supplement = ' | '.join(product_lines[:10])  # Top 10 relevant lines
            comprehensive_desc += f" | Additional Details: {markdown_supplement}"
    
    # Ensure minimum length and quality
    if len(comprehensive_desc) < 200:
        # Fallback: use main product name and basic info
        name = extract_data.get('name', 'Product')
        brand = extract_data.get('brand', '')
        category = extract_data.get('category', '')
        price = extract_data.get('price', '')
        
        fallback_desc = f"{name}"
        if brand:
            fallback_desc += f" by {brand}"
        if category:
            fallback_desc += f" - {category}"
        if price:
            fallback_desc += f" - Price: {price}"
        if main_desc:
            fallback_desc += f" - {main_desc}"
        
        comprehensive_desc = fallback_desc
    
    # Clean up and return
    comprehensive_desc = comprehensive_desc.replace('  ', ' ').replace(' | |', ' |').strip()
    
    return comprehensive_desc

# Alternative function for batch processing
@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1800  # 30 minutes for batch
)
def extract_products_batch(product_urls: list) -> list:
    """
    Batch extract multiple products at once
    Alternative to worker-based processing for smaller batches
    """
    # Initialize Firecrawl
    firecrawl = None
    if FirecrawlApp:
        api_key = os.environ.get('FIRECRAWL_API_KEY')
        if api_key:
            firecrawl = FirecrawlApp(api_key=api_key)
    
    if not firecrawl:
        print("❌ Firecrawl not available - cannot extract products")
        return []
    
    print(f"🔍 Batch extracting {len(product_urls)} products...")
    
    extracted_products = []
    
    for i, product_url in enumerate(product_urls, 1):
        print(f"📥 Processing {i}/{len(product_urls)}: {product_url.url}")
        
        extracted_product = _extract_single_product(firecrawl, product_url)
        
        if extracted_product:
            extracted_products.append(extracted_product)
            print(f"✅ Extracted: {extracted_product.name}")
        else:
            print(f"❌ Failed: {product_url.url}")
        
        # Small delay to respect rate limits
        time.sleep(0.1)
    
    print(f"✅ Batch extraction complete: {len(extracted_products)}/{len(product_urls)} successful")
    return extracted_products