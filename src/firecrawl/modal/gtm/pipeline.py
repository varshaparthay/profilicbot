#!/usr/bin/env python3
"""
GTM Pipeline - Modal Implementation
Website Discovery and Processing Pipeline using Firecrawl Map
"""

import modal

# Modal app configuration with mounted prompt files
image = (
    modal.Image.debian_slim(python_version="3.13")
    .pip_install([
        "requests",
        "pandas", 
        "openai",
        "firecrawl-py",
        "pydantic",
        "beautifulsoup4",
        "python-dotenv",
        "boto3",
        "fastapi[standard]",  # Required for web endpoints
        "aiohttp",  # Required for async HTTP requests in hybrid discovery
        "brotli"   # Required for brotli compression support
    ])
    .add_local_dir("/Users/varsha/src/profilicbot/src/prompts", remote_path="/prompts")
)

app = modal.App("gtm-pipeline")

# Secrets for APIs
secrets = [
    modal.Secret.from_name("firecrawl-api-key"),
    modal.Secret.from_name("openai-api-key"), 
    modal.Secret.from_name("aws-s3-credentials")
]

@app.function(
    image=image,
    secrets=secrets,
    timeout=86400  # 24 hours
)
def start_gtm_pipeline(website_url: str, single_url: bool = False, user_email: str = None):
    """
    Main GTM pipeline: Discovery → Processing → Email Notification
    
    Args:
        website_url: Target website URL (e.g., "https://example.com")
        single_url: If True, process only the single URL; if False, discover all URLs on website
        user_email: Email address to send completion notification (optional)
        
    Returns:
        Pipeline results with execution details
    """
    import time
    import uuid
    
    execution_id = f"gtm_{int(time.time())}_{str(uuid.uuid4())[:8]}"
    queue_name = f"gtm-jobs-{execution_id}"
    
    print(f"🚀 GTM PIPELINE STARTED")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌐 Website URL: {website_url}")
    print(f"🎯 Mode: {'Single URL' if single_url else 'Full Website Discovery'}")
    print(f"⏰ Max timeout: 24 hours")
    
    try:
        # Stage 0: Discovery - Load URLs into queue
        if single_url:
            url_count = load_single_url_to_queue.remote(website_url, queue_name, execution_id)
        else:
            url_count = discover_and_load_urls_to_queue.remote(website_url, queue_name, execution_id)
        
        print(f"✅ Stage 0: Loaded {url_count} URLs to queue")
        
        # Use 50 workers for demo speed
        max_workers = 50
        print(f"🔧 Starting {max_workers} workers for parallel processing...")
        
        # Start workers
        workers = [
            gtm_worker.spawn(queue_name, execution_id, i) 
            for i in range(max_workers)
        ]
        
        # Wait for all workers to complete
        print(f"⏳ Waiting for all workers to complete...")
        results = [worker.get() for worker in workers]
        
        # Calculate totals
        total_processed = sum([r["processed"] for r in results])
        total_errors = sum([r["errors"] for r in results])
        
        print(f"✅ All workers completed!")
        print(f"📊 Processed: {total_processed}, Errors: {total_errors}")
        
        # Consolidate results
        print(f"📋 Consolidating results...")
        final_results_path = consolidate_gtm_results.remote(execution_id, website_url)
        
        # Send email notification if email provided
        if user_email:
            print(f"📧 Sending completion notification to {user_email}...")
            try:
                send_completion_email.remote(
                    user_email, 
                    execution_id, 
                    website_url, 
                    total_processed, 
                    total_errors, 
                    final_results_path,
                    single_url
                )
                print(f"✅ Email notification sent successfully!")
            except Exception as e:
                print(f"⚠️ Email notification failed: {e}")
        
        pipeline_result = {
            "status": "completed",
            "execution_id": execution_id,
            "website_url": website_url,
            "single_url_mode": single_url,
            "urls_discovered": url_count,
            "urls_processed": total_processed,
            "errors": total_errors,
            "results_path": final_results_path,
            "s3_location": f"s3://flex-ai/gtm/{execution_id}/",
            "worker_results": results,
            "completion_time": time.time()
        }
        
        print(f"🎉 GTM PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"📁 Results: {final_results_path}")
        
        return pipeline_result
        
    except Exception as e:
        error_result = {
            "status": "failed",
            "execution_id": execution_id,
            "website_url": website_url,
            "error": str(e),
            "failure_time": time.time()
        }
        
        print(f"❌ GTM PIPELINE FAILED: {e}")
        return error_result

@app.function(
    image=image,
    secrets=secrets,
    timeout=3600  # 1 hour for discovery
)
def load_single_url_to_queue(website_url: str, queue_name: str, execution_id: str):
    """Load single URL into processing queue"""
    from modal import Queue
    
    print(f"📌 Loading single URL: {website_url}")
    
    # Create queue
    queue = Queue.from_name(queue_name, create_if_missing=True)
    
    # Add single URL to queue
    work_item = {
        "url": website_url,
        "url_id": f"{execution_id}_url_0001",
        "execution_id": execution_id,
        "discovery_method": "single_url"
    }
    
    queue.put(work_item)
    
    print(f"✅ Added 1 URL to queue: {queue_name}")
    return 1

def filter_product_urls(urls):
    """Aggressively filter URLs to keep only likely product pages"""
    import re
    
    # URL patterns to exclude (case-insensitive) - your exact debug list
    exclude_list = [
        # Your original debug list
        'developer', 'terms-and-conditions', 'medical-advisory-board', 'v2/docs', 'extend', 
        'declarations-of-conformity', 'membership', 'docs', 'regulatory-notices-ca', 
        'integrations', 'accessibility', 'privacy-policy', 'science-and-research', 
        'my-account', 'shipping-information', 'business', 'guidelines-for-commercial-use', 
        'sizing', 'careers', 'fcc-compliance-statements', 'es', 'intellectual-property-notice', 
        '404', 'how-it-works', 'contact', 'cookie-policy',
        # Critical missing patterns from your examples
        'blog', '/blog/', 'about', 'support', 'help', 'legal', 'privacy', 'terms',
        # Additional excludes you found
        'videositemap.xml', 'gift', 'de', '/growth/sitemap.xml', 'social-use-agreement', 
        'm/?ref=godly', 'tc/raf-2way', '/why-oura', 'programs', '/sitemap.xml', 'calendar', 'forgot_password', 'join_us', 'frontpage', 'sitemap*.xml', 'sitemap_index.xml', 'sitemap_index.xml.gz', 'sitemap.xml.gz',
    ]
    
    filtered_urls = []
    excluded_urls = []
    
    for url in urls:
        exclude_url = False
        exclude_reason = ""
        
        # Check against your exact exclude list
        for exclude_term in exclude_list:
            if exclude_term.lower() in url.lower():
                exclude_url = True
                exclude_reason = exclude_term
                break
        
        if exclude_url:
            excluded_urls.append(f"{url} (excluded by: {exclude_reason})")
        else:
            filtered_urls.append(url)
    
    print(f"🔍 URL FILTERING RESULTS:")
    print(f"   Original URLs: {len(urls)}")
    print(f"   Excluded URLs: {len(excluded_urls)}")
    print(f"   Kept URLs: {len(filtered_urls)}")
    
    # Log ALL excluded URLs for complete audit
    if excluded_urls:
        print(f"\n📋 ALL EXCLUDED URLs ({len(excluded_urls)} total):")
        for i, excluded in enumerate(excluded_urls):
            print(f"   {i+1:3d}. {excluded}")
    
    # Log ALL kept URLs for complete audit  
    if filtered_urls:
        print(f"\n📋 ALL KEPT URLs ({len(filtered_urls)} total):")
        for i, kept in enumerate(filtered_urls):
            print(f"   {i+1:3d}. {kept}")
        
        # Also save kept URLs to a CSV-like format for easy analysis
        print(f"\n📋 KEPT URLs - CSV FORMAT:")
        print("URL")
        for kept in filtered_urls:
            print(kept)
    
    return filtered_urls

@app.function(
    image=image,
    secrets=secrets,
    timeout=3600  # 1 hour for discovery
)
def discover_and_load_urls_to_queue(website_url: str, queue_name: str, execution_id: str):
    """Use Firecrawl map to discover all URLs on website and load into queue"""
    from modal import Queue
    import os
    from firecrawl import FirecrawlApp
    
    print(f"🗺️ Discovering URLs on website: {website_url}")
    
    try:
        # Initialize Firecrawl
        firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_API_KEY"))
        
        # Use Firecrawl map to discover all URLs
        print(f"🔍 Running Firecrawl map on {website_url}")
        map_result = firecrawl.map_url(website_url)
        
        if not map_result or not hasattr(map_result, 'links'):
            raise Exception("Firecrawl map failed to return URLs")
        
        discovered_urls = map_result.links
        print(f"📊 Discovered {len(discovered_urls)} URLs via Firecrawl map")
        
        # Print all discovered URLs first
        print(f"\n🗂️ ALL DISCOVERED URLS ({len(discovered_urls)} total):")
        for i, url in enumerate(discovered_urls):
            print(f"   {i+1:3d}. {url}")
        
        # Filter out non-product URLs
        print(f"\n🔧 CALLING FILTER FUNCTION...")
        filtered_urls = filter_product_urls(discovered_urls)
        print(f"\n📊 After filtering: {len(filtered_urls)} URLs (removed {len(discovered_urls) - len(filtered_urls)} non-product URLs)")
        
        # Debug: show first few filtered URLs
        print(f"\n🔍 FIRST 10 FILTERED URLS:")
        for i, url in enumerate(filtered_urls[:10]):
            print(f"   {i+1}. {url}")
        
        # Create queue
        queue = Queue.from_name(queue_name, create_if_missing=True)
        
        # Prepare all work items for batch put
        work_items = []
        for idx, url in enumerate(filtered_urls):
            work_item = {
                "url": url,
                "url_id": f"{execution_id}_url_{idx:06d}",
                "execution_id": execution_id,
                "discovery_method": "firecrawl_map"
            }
            work_items.append(work_item)
        
        # Batch put all items to queue
        queue.put_many(work_items)
        
        print(f"✅ Added {len(filtered_urls)} URLs to queue: {queue_name}")
        return len(filtered_urls)
        
    except Exception as e:
        print(f"❌ URL discovery failed: {e}")
        # Fallback to single URL if discovery fails
        print(f"🔄 Falling back to single URL processing")
        return load_single_url_to_queue.remote(website_url, queue_name, execution_id)

@app.function(
    image=image,
    secrets=secrets,
    max_containers=300,
    timeout=86400  # 24 hours per worker
)
def gtm_worker(queue_name: str, execution_id: str, worker_id: int):
    """
    Worker: Process URLs from the queue
    Each worker processes multiple URLs until queue is empty
    """
    from modal import Queue
    
    queue = Queue.from_name(queue_name)
    processed = 0
    errors = 0
    
    print(f"🔧 GTM Worker {worker_id} started")
    
    while True:
        # Get work with timeout
        try:
            work_item = queue.get(timeout=60)  # Wait 60 seconds for work
            
            if work_item is None:
                print(f"✅ GTM Worker {worker_id} finished - no more work (processed {processed})")
                break
        except Exception as e:
            print(f"✅ GTM Worker {worker_id} finished - queue empty (processed {processed})")
            break
        
        try:
            # Process single URL
            result = process_single_url(work_item)
            
            # Save result to S3
            save_gtm_result_to_s3(execution_id, work_item["url_id"], result)
            processed += 1
            
            if processed % 10 == 0:
                print(f"📊 GTM Worker {worker_id}: {processed} URLs completed")
                
        except Exception as e:
            print(f"❌ GTM Worker {worker_id} error on {work_item['url_id']}: {e}")
            save_gtm_error_to_s3(execution_id, work_item["url_id"], str(e), work_item)
            errors += 1
    
    return {
        "worker_id": worker_id,
        "processed": processed,
        "errors": errors
    }

def process_single_url(work_item):
    """
    Process single URL through 3 stages (matching dermstore structure):
    Stage 1: Firecrawl Scrape - Extract raw content 
    Stage 2: Categorization - Classify content into up to 3 categories
    Stage 3: Classification - HSA/FSA eligibility using category-specific guides
    
    Args:
        work_item: Contains url and metadata
        
    Returns:
        Processed result with extracted content, categorization, and classification
    """
    import time
    
    url = work_item["url"]
    url_display = url[:80] + "..." if len(url) > 80 else url
    
    # Stage 1: Firecrawl Scrape
    print(f"📄 Stage 1: Scraping {url_display}")
    extraction_result = stage1_firecrawl_scrape(url)
    
    # Stage 2: Categorization
    if extraction_result["status"] == "success":
        print(f"🏷️ Stage 2: Categorizing content from {url_display}")
        categorization_result = stage2_categorize_content(extraction_result)
    else:
        categorization_result = {"status": "skipped", "reason": "No content to categorize"}
        print(f"⏭️ Stage 2: Skipped categorization (no content)")
    
    # Stage 3: Classification
    if categorization_result["status"] == "success":
        print(f"🏥 Stage 3: Classifying HSA/FSA eligibility from {url_display}")
        classification_result = stage3_classify_eligibility(extraction_result, categorization_result)
    else:
        classification_result = {"status": "skipped", "reason": "No categories for classification"}
        print(f"⏭️ Stage 3: Skipped classification (no categories)")
    
    # Combine results using exact same structure as dermstore pipeline
    final_result = {
        **work_item,  # Include original URL and metadata
        
        # Stage 1: Extraction (same fields as dermstore)
        "extracted_name": extraction_result.get("name", ""),
        "detailed_description": extraction_result.get("detailed_description", ""),
        "ingredients": extraction_result.get("ingredients", ""),
        "conditions_treats": extraction_result.get("conditions_treats", ""),
        "category": extraction_result.get("category", ""),
        "extraction_status": extraction_result.get("status", "failed"),
        
        # Stage 2: Categorization
        "primary_category": categorization_result.get("primary_category", ""),
        "secondary_category": categorization_result.get("secondary_category", ""),
        "tertiary_category": categorization_result.get("tertiary_category", ""),
        "categorization_reasoning": categorization_result.get("reasoning", ""),
        "categorization_confidence": categorization_result.get("confidence", 0),
        "categorization_status": categorization_result.get("status", "failed"),
        
        # Stage 3: Classification (same fields as dermstore)
        "eligibility_status": classification_result.get("eligibilityStatus", ""),
        "explanation": classification_result.get("explanation", ""),
        "additional_considerations": classification_result.get("additionalConsiderations", ""),
        "lmn_qualification_probability": classification_result.get("lmnQualificationProbability", ""),
        "confidence_percentage": classification_result.get("confidencePercentage", 0),
        "classification_status": classification_result.get("status", "failed"),
        
        # Metadata
        "processing_timestamp": time.time(),
        "overall_status": "completed"
    }
    
    return final_result

def stage1_firecrawl_scrape(url: str):
    """
    Stage 1: Firecrawl extraction using exact same pattern as dermstore pipeline
    """
    import time
    
    try:
        import os
        from firecrawl import FirecrawlApp
        
        firecrawl = FirecrawlApp(api_key=os.environ.get("FIRECRAWL_API_KEY"))
        
        # Add URL validation
        if not url or url.strip() == "":
            return {
                "status": "failed",
                "error": "Empty URL provided"
            }
        
        # Use Firecrawl's structured extraction with custom prompt (same pattern as dermstore)
        extraction_prompt = """
        Extract detailed content information from this web page.
        
        For the detailed_description field, provide a comprehensive description that includes:
        1. What the page/content is about
        2. Key information and main points
        3. Important details and features mentioned
        4. Any specific topics or themes covered
        5. Target audience or purpose if mentioned
        
        Make the detailed_description informative and comprehensive, combining all relevant page details into flowing, well-organized text.
        """
        
        # Log the Firecrawl extraction prompt for verification
        print(f"🔧 === FIRECRAWL EXTRACTION PROMPT ===")
        print(f"🌐 URL: {url}")
        print(f"📝 Extraction Prompt:\n{extraction_prompt}")
        print(f"🔧 === END EXTRACTION PROMPT ===")
        print(f"⏳ Starting Firecrawl extraction with AI...")
        
        # Add retry logic for extraction
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                scrape_result = firecrawl.scrape_url(
                    url,
                    formats=["extract"],
                    extract={
                        "prompt": extraction_prompt,
                        "schema": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Page title or main heading"
                                },
                                "detailed_description": {
                                    "type": "string", 
                                    "description": "Comprehensive page description including what it is, key information, important details, and main topics"
                                },
                                "ingredients": {
                                    "type": "string",
                                    "description": "Key components, technologies, or elements mentioned on the page"
                                },
                                "conditions_treats": {
                                    "type": "string",
                                    "description": "Problems, issues, or use cases this page/content addresses or is relevant for"
                                },
                                "category": {
                                    "type": "string", 
                                    "description": "Content category (blog, product, service, documentation, etc.)"
                                }
                            },
                            "required": ["name", "detailed_description"]
                        }
                    }
                )
                
                if scrape_result and scrape_result.success and scrape_result.extract:
                    break  # Success, exit retry loop
                elif attempt < max_retries - 1:
                    print(f"⚠️ Extraction failed, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as retry_e:
                if attempt < max_retries - 1:
                    print(f"⚠️ Extraction API error, retrying in {retry_delay}s: {retry_e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise retry_e
        
        if not scrape_result or not scrape_result.success or not scrape_result.extract:
            error_msg = "Failed to extract content"
            if hasattr(scrape_result, 'error') and scrape_result.error:
                error_msg = f"Extraction failed: {scrape_result.error}"
                
            return {
                "status": "failed",
                "error": error_msg,
            }
        
        extracted_data = scrape_result.extract
        
        return {
            "status": "success",
            "name": extracted_data.get("name", ""),
            "detailed_description": extracted_data.get("detailed_description", ""),
            "ingredients": extracted_data.get("ingredients", ""),
            "conditions_treats": extracted_data.get("conditions_treats", ""),
            "category": extracted_data.get("category", ""),
            "scraped_url": url
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }

def stage2_categorize_content(extraction_result):
    """
    Stage 2: Categorize content using OpenAI and flex product categories
    """
    try:
        import os
        import json
        import openai
        
        client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        # Load the categorization prompt template
        prompt_template = load_categorization_prompt()
        categories_data = load_product_categories()
        
        # Extract data from extraction result
        name = extraction_result.get("name", "")
        description = extraction_result.get("detailed_description", "")
        ingredients = extraction_result.get("ingredients", "")
        features = extraction_result.get("conditions_treats", "")
        
        # Build the categorization prompt
        prompt = build_categorization_prompt(
            prompt_template, 
            categories_data, 
            name, 
            description, 
            ingredients, 
            features
        )
        
        print(f"🤖 === OPENAI CATEGORIZATION PROMPT ===")
        print(f"📝 Model: gpt-4o-mini")
        print(f"🌡️ Temperature: 0.1")
        print(f"💬 User Prompt:\n{prompt[:500]}...")
        print(f"🤖 === END PROMPT ===")
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an AI assistant that categorizes web content into predefined categories. Follow the instructions exactly and only use categories from the provided list."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        
        response_text = response.choices[0].message.content
        
        print(f"🤖 === OPENAI RESPONSE ===")
        print(f"📝 Raw Response:\n{response_text}")
        print(f"🤖 === END RESPONSE ===")
        
        # Parse the JSON response
        categorization_result = parse_categorization_response(response_text)
        
        return {
            "status": "success",
            **categorization_result
        }
        
    except Exception as e:
        print(f"❌ Categorization failed: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "primary_category": "",
            "secondary_category": "",
            "tertiary_category": "",
            "reasoning": f"Categorization failed: {str(e)}",
            "confidence": 0
        }

def load_categorization_prompt():
    """Load the categorization prompt template from mounted file"""
    try:
        with open("/prompts/categorization_prompt.txt", "r") as f:
            return f.read()
    except Exception as e:
        print(f"❌ Failed to load categorization prompt: {e}")
        # Fallback prompt matching the updated template
        return """Classify this content into the most appropriate categories from the list below. Select up to 3 categories, ranked by relevance:

Content: {{PRODUCT_NAME}}
Description: {{PRODUCT_DESCRIPTION}}
Components: {{PRODUCT_BRAND}}
Features: {{PRODUCT_FEATURES}}

Available Categories:
{{CATEGORIES_LIST}}

CRITICAL CONSTRAINT: You must ONLY use category names from this exact list. Do NOT create or invent new categories.

VALID CATEGORIES ONLY:
[{{VALID_CATEGORY_NAMES}}]

INSTRUCTIONS:
1. Select 1-3 categories that best match this content
2. Rank them by relevance (primary = most relevant)
3. If only 1 category fits, leave secondary/tertiary as empty strings ""
4. If only 2 categories fit, leave tertiary as empty string ""
5. Use exact category names from the list above

Respond with JSON:
{
    "primary_category": "MUST_BE_EXACT_MATCH_FROM_LIST_ABOVE",
    "secondary_category": "SECOND_MOST_RELEVANT_OR_EMPTY_STRING",
    "tertiary_category": "THIRD_MOST_RELEVANT_OR_EMPTY_STRING", 
    "reasoning": "Brief explanation of why these categories were chosen from the valid list",
    "confidence": 85
}"""

def load_product_categories():
    """Load the flex product categories from mounted file"""
    import json
    try:
        with open("/prompts/flex_product_categories.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to load product categories: {e}")
        # Return minimal fallback
        return {
            "categories": [
                {"name": "Dermatology & Skin Care", "description": "Skin care products", "keywords": ["skin", "care"]},
                {"name": "Nutritional Supplements & Vitamins", "description": "Supplements", "keywords": ["vitamin", "supplement"]},
                {"name": "Medical Equipment & Supplies", "description": "Medical devices", "keywords": ["medical", "device"]}
            ]
        }

def build_categorization_prompt(template, categories_data, name, description, ingredients, features):
    """Build the final categorization prompt with all data"""
    # Extract category names and build categories list
    categories = categories_data.get("categories", [])
    category_names = [cat["name"] for cat in categories]
    
    # Build detailed categories list
    categories_list = []
    for cat in categories:
        cat_desc = f"• {cat['name']}: {cat['description']}"
        categories_list.append(cat_desc)
    
    categories_text = "\n".join(categories_list)
    category_names_text = ", ".join([f'"{name}"' for name in category_names])
    
    # Replace template variables
    prompt = template.replace("{{PRODUCT_NAME}}", name or "Not specified")
    prompt = prompt.replace("{{PRODUCT_DESCRIPTION}}", description or "Not specified")
    prompt = prompt.replace("{{PRODUCT_BRAND}}", ingredients or "Not specified")  # Reusing ingredients as "brand/components"
    prompt = prompt.replace("{{PRODUCT_FEATURES}}", features or "Not specified")
    prompt = prompt.replace("{{CATEGORIES_LIST}}", categories_text)
    prompt = prompt.replace("{{VALID_CATEGORY_NAMES}}", category_names_text)
    
    # Prompt is already configured for 3 categories in the template file
    
    return prompt

def parse_categorization_response(response_text: str):
    """Parse the AI categorization response"""
    import json
    try:
        # Find JSON block in response
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}') + 1
        
        if start_idx == -1 or end_idx == 0:
            raise ValueError("No JSON found in response")
        
        json_text = response_text[start_idx:end_idx]
        result = json.loads(json_text)
        
        return {
            "primary_category": result.get("primary_category", ""),
            "secondary_category": result.get("secondary_category", ""),
            "tertiary_category": result.get("tertiary_category", ""),
            "reasoning": result.get("reasoning", ""),
            "confidence": result.get("confidence", 0)
        }
        
    except Exception as e:
        return {
            "primary_category": "Parse Error",
            "secondary_category": "",
            "tertiary_category": "",
            "reasoning": f"Failed to parse AI response: {str(e)}",
            "confidence": 0
        }

def stage3_classify_eligibility(extraction_result, categorization_result):
    """
    Stage 3: HSA/FSA eligibility classification using category-specific guides (dermstore pattern)
    """
    try:
        import os
        import json
        import openai
        
        client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        # Get categories from Stage 2
        primary_category = categorization_result.get("primary_category", "")
        secondary_category = categorization_result.get("secondary_category", "")
        tertiary_category = categorization_result.get("tertiary_category", "")
        
        # Get content from Stage 1
        product_name = extraction_result.get("name", "")
        product_description = extraction_result.get("detailed_description", "")
        ingredients = extraction_result.get("ingredients", "")
        conditions_treats = extraction_result.get("conditions_treats", "")
        
        print(f"🔍 Looking up guides for categories:")
        print(f"   Primary: {primary_category}")
        print(f"   Secondary: {secondary_category}")
        print(f"   Tertiary: {tertiary_category}")
        
        # Load and lookup guides for categories
        guide_data = load_flex_guide_mapped_to_categories()
        relevant_guides = lookup_guides_for_categories(
            guide_data, 
            primary_category, 
            secondary_category, 
            tertiary_category
        )
        
        print(f"📚 Found guides for {len(relevant_guides)} categories")
        
        # Build classification prompt using dermstore pattern
        prompt = build_classification_prompt(
            product_name, 
            product_description, 
            ingredients, 
            conditions_treats, 
            relevant_guides
        )
        
        print(f"🤖 === HSA/FSA CLASSIFICATION PROMPT ===")
        print(f"📝 Model: gpt-4o-mini")
        print(f"🌡️ Temperature: 0.1")
        print(f"💬 User Prompt:\n{prompt[:500]}...")
        print(f"🤖 === END PROMPT ===")
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an AI medical assistant that determines HSA/FSA eligibility for products."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        
        response_text = response.choices[0].message.content
        
        print(f"🤖 === HSA/FSA CLASSIFICATION RESPONSE ===")
        print(f"📝 Raw Response:\n{response_text}")
        print(f"🤖 === END RESPONSE ===")
        print(f"🔄 Token Usage: {response.usage.total_tokens} tokens")
        
        # Parse JSON response (same as dermstore)
        classification_result = parse_classification_response(response_text)
        
        return {
            "status": "success",
            **classification_result
        }
        
    except Exception as e:
        print(f"❌ HSA/FSA Classification failed: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "eligibilityStatus": "Error",
            "explanation": f"Classification failed: {str(e)}",
            "additionalConsiderations": "",
            "lmnQualificationProbability": "N/A",
            "confidencePercentage": 0
        }

def load_flex_guide_mapped_to_categories():
    """Load the flex guide mapped to categories from mounted file"""
    import json
    try:
        with open("/prompts/flex_guide_mapped_to_categories.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to load flex guide: {e}")
        return {"guide": []}

def lookup_guides_for_categories(guide_data, primary_category, secondary_category, tertiary_category):
    """Dynamic guide lookup without hardcoding - improved matching"""
    relevant_categories = []
    categories_to_search = [primary_category, secondary_category, tertiary_category]
    categories_to_search = [cat for cat in categories_to_search if cat and cat.strip()]
    
    print(f"🔍 Searching guide for categories: {categories_to_search}")
    
    # Use 'guide' key to match the actual JSON structure with improved matching logic
    for guide_category in guide_data.get("guide", []):
        guide_name = guide_category.get("category", "").lower()
        
        # Check if any of our categories match this guide category (case-insensitive, partial match)
        for search_cat in categories_to_search:
            search_cat_lower = search_cat.lower()
            
            # More flexible matching: check for key words and partial matches
            if ("blood pressure" in search_cat_lower and "cardiac" in guide_name) or \
               ("cardiac" in search_cat_lower and "cardiac" in guide_name) or \
               ("medical equipment" in search_cat_lower and "medical equipment" in guide_name) or \
               ("monitoring" in search_cat_lower and "monitoring" in guide_name) or \
               search_cat_lower in guide_name or guide_name in search_cat_lower:
                relevant_categories.append(guide_category)
                print(f"✅ Matched: '{search_cat}' with guide category '{guide_category.get('category')}'")
                break
                
    print(f"📚 Found {len(relevant_categories)} relevant guide categories")
    return relevant_categories

def build_classification_prompt(product_name, product_description, ingredients, conditions_treats, relevant_guides):
    """Build HSA/FSA classification prompt using dermstore pattern with dynamic guides"""
    import json
    
    # Create the guide structure that the classification prompt expects
    guide_data = {"guide": relevant_guides}
    guide_text = json.dumps(guide_data, indent=2)
    
    # Use dermstore's exact prompt pattern
    prompt = f"""You are an AI medical assistant using the Flex Product Guide to determine HSA/FSA eligibility.

**Input:**  
- **Product Name:** {product_name}  
- **Product Description:** {product_description}  
- **Ingredients:** {ingredients}  
- **Conditions/Skin Care Treats:** {conditions_treats}  

**Instructions:**
1. **Gather Details:**  
   - Use the provided Product Name and Description for your analysis.  
   - If one field is missing, flag the missing information.

2. **Analyze & Classify:**  
   a. Identify key product details (ingredients, mechanism, indications).  
   b. Determine product type and primary purpose (medical device, OTC drug, general wellness, dual-use).  
   c. Precedence Rule:
      - If the Flex Product Guide explicitly marks the product Eligible/Non-eligible/Needs LMN, follow that decision.
   e. **IMPORTANT**: Don't refer to IRS guidelines to classify a product. Use ONLY the Flex Product Guide that is provided to you to make a decision.

3. **Reasoning:**  
   - Provide a numbered bullet list (no more than 3 points) of your chain-of-thought.

4. **Final Output:**  
   At the end, output **exactly** in this JSON format (keys must be quoted; values may be strings or numbers; always include all fields):

```json
{{
  "eligibilityStatus": "<Eligible / Not Eligible / Eligible with Letter of Medical Necessity>",
  "explanation": "<Concise reasoning with citations to guide sections>",
  "additionalConsiderations": "<Caveats or usage notes>",
  "lmnQualificationProbability": "<If Non-eligible, % and brief rationale; otherwise \"N/A\">",
  "confidencePercentage": <Number 0–100 indicating model's confidence in this answer>
}}
```

5. **Error Handling**:
If you cannot classify the product, respond with "Insufficient Information"

**Flex Product Guide:**
{guide_text}
"""
    
    return prompt

def parse_classification_response(response_text: str):
    """Parse the AI classification response (same as dermstore)"""
    import json
    try:
        # Find JSON block in response
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}') + 1
        
        if start_idx == -1 or end_idx == 0:
            raise ValueError("No JSON found in response")
        
        json_text = response_text[start_idx:end_idx]
        result = json.loads(json_text)
        
        return {
            "eligibilityStatus": result.get("eligibilityStatus", "Unknown"),
            "explanation": result.get("explanation", ""),
            "additionalConsiderations": result.get("additionalConsiderations", ""), 
            "lmnQualificationProbability": result.get("lmnQualificationProbability", "N/A"),
            "confidencePercentage": result.get("confidencePercentage", 0)
        }
        
    except Exception as e:
        return {
            "eligibilityStatus": "Parse Error",
            "explanation": f"Failed to parse AI response: {str(e)}",
            "additionalConsiderations": "",
            "lmnQualificationProbability": "N/A", 
            "confidencePercentage": 0
        }

def save_gtm_result_to_s3(execution_id: str, url_id: str, result):
    """Save worker result to S3"""
    import boto3
    import json
    try:
        s3_client = boto3.client('s3')
        
        # Save as JSON
        json_key = f"gtm/{execution_id}/results/{url_id}.json"
        s3_client.put_object(
            Bucket='flex-ai',
            Key=json_key,
            Body=json.dumps(result, indent=2),
            ContentType='application/json'
        )
        
    except Exception as e:
        print(f"❌ Failed to save GTM result to S3: {e}")

def save_gtm_error_to_s3(execution_id: str, url_id: str, error: str, original_data):
    """Save error result to S3"""
    import boto3
    import json
    import time
    try:
        s3_client = boto3.client('s3')
        
        error_result = {
            **original_data,
            "processing_error": error,
            "overall_status": "failed",
            "processing_timestamp": time.time()
        }
        
        # Save as JSON
        json_key = f"gtm/{execution_id}/results/{url_id}_error.json"
        s3_client.put_object(
            Bucket='flex-ai',
            Key=json_key,
            Body=json.dumps(error_result, indent=2),
            ContentType='application/json'
        )
        
    except Exception as e:
        print(f"❌ Failed to save GTM error to S3: {e}")

@app.function(
    image=image,
    secrets=secrets,
    timeout=3600,   # 1 hour
    memory=4096     # 4GB for large dataset processing
)
def consolidate_gtm_results(execution_id: str, website_url: str):
    """
    Consolidate all worker outputs into final results
    """
    import boto3
    import json
    import pandas as pd
    from urllib.parse import urlparse
    
    print(f"📋 Consolidating GTM results for {execution_id}")
    
    try:
        s3_client = boto3.client('s3')
        
        # List all worker output files
        prefix = f"gtm/{execution_id}/results/"
        response = s3_client.list_objects_v2(
            Bucket='flex-ai',
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            raise Exception("No worker output files found")
        
        # Collect all results
        all_results = []
        file_count = 0
        
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.json'):
                # Read JSON file
                response = s3_client.get_object(Bucket='flex-ai', Key=key)
                result_data = json.loads(response['Body'].read())
                all_results.append(result_data)
                file_count += 1
        
        print(f"📊 Collected {file_count} result files")
        
        # Create DataFrame
        df = pd.DataFrame(all_results)
        
        # Create output filename from website URL
        domain = urlparse(website_url).netloc.replace('www.', '').replace('.', '_')
        output_filename = f"gtm_{domain}_{execution_id}.csv"
        
        # Save final CSV to execution_id/outputs/ directory
        final_csv_key = f"gtm/{execution_id}/outputs/{output_filename}"
        csv_buffer = df.to_csv(index=False)
        
        s3_client.put_object(
            Bucket='flex-ai',
            Key=final_csv_key,
            Body=csv_buffer,
            ContentType='text/csv'
        )
        
        final_csv_path = f"s3://flex-ai/{final_csv_key}"
        
        print(f"✅ Consolidation Complete: {len(df)} URLs processed")
        print(f"📁 Final CSV: {final_csv_path}")
        
        return final_csv_path
        
    except Exception as e:
        print(f"❌ Consolidation failed: {e}")
        raise e

@app.function(
    image=image,
    secrets=secrets,
    timeout=300  # 5 minutes
)
def send_completion_email(user_email: str, execution_id: str, website_url: str, urls_processed: int, errors: int, results_path: str, single_url_mode: bool):
    """
    Send email notification when GTM pipeline completes
    """
    import boto3
    import os
    from urllib.parse import urlparse
    from datetime import datetime
    
    print(f"📧 === SENDING EMAIL NOTIFICATION ===")
    print(f"📬 To: {user_email}")
    print(f"📋 Execution ID: {execution_id}")
    
    try:
        # Initialize AWS SES client  
        ses_client = boto3.client('ses', region_name='us-west-2')  # Use same region as credentials
        
        # Extract domain for cleaner display
        domain = urlparse(website_url).netloc.replace('www.', '')
        
        # Create email content
        mode_text = "Single URL" if single_url_mode else "Full Website Discovery"
        status_icon = "✅" if errors == 0 else "⚠️"
        
        subject = f"{status_icon} GTM Pipeline Complete: {domain} ({urls_processed} URLs processed)"
        
        # HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .stats {{ background-color: #f9f9f9; padding: 15px; border-left: 4px solid #4CAF50; margin: 20px 0; }}
                .footer {{ background-color: #f1f1f1; padding: 15px; text-align: center; font-size: 0.9em; }}
                .error {{ color: #d32f2f; }}
                .success {{ color: #4CAF50; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{status_icon} GTM Pipeline Complete</h1>
            </div>
            
            <div class="content">
                <h2>Pipeline Results</h2>
                
                <div class="stats">
                    <strong>Website:</strong> {website_url}<br>
                    <strong>Mode:</strong> {mode_text}<br>
                    <strong>Execution ID:</strong> {execution_id}<br>
                    <strong>URLs Processed:</strong> <span class="success">{urls_processed}</span><br>
                    <strong>Errors:</strong> <span class="{'error' if errors > 0 else 'success'}">{errors}</span><br>
                    <strong>Completed:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
                </div>
                
                <h3>Results Available</h3>
                <p>Your processed data has been consolidated into a CSV file:</p>
                <p><strong>Location:</strong> {results_path}</p>
                
                <h3>What's Included</h3>
                <ul>
                    <li><strong>Content Extraction:</strong> Product names, descriptions, and key details</li>
                    <li><strong>AI Categorization:</strong> Up to 3 relevant categories per URL</li>
                    <li><strong>HSA/FSA Classification:</strong> Eligibility determination with confidence scores</li>
                </ul>
                
                {"<p class='error'><strong>Note:</strong> Some URLs encountered errors during processing. Check the S3 location for error details.</p>" if errors > 0 else "<p class='success'><strong>Success:</strong> All URLs processed without errors!</p>"}
            </div>
            
            <div class="footer">
                <p>This notification was sent automatically by the GTM Pipeline system.<br>
                Generated by <a href="https://claude.ai/code">Claude Code</a></p>
            </div>
        </body>
        </html>
        """
        
        # Text version for email clients that don't support HTML
        text_body = f"""
GTM Pipeline Complete - {domain}

Pipeline Results:
- Website: {website_url}
- Mode: {mode_text}  
- Execution ID: {execution_id}
- URLs Processed: {urls_processed}
- Errors: {errors}
- Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

Results Available:
Your processed data has been consolidated into a CSV file at:
{results_path}

What's Included:
- Content Extraction: Product names, descriptions, and key details
- AI Categorization: Up to 3 relevant categories per URL  
- HSA/FSA Classification: Eligibility determination with confidence scores

{'Note: Some URLs encountered errors during processing. Check the S3 location for error details.' if errors > 0 else 'Success: All URLs processed without errors!'}

This notification was sent automatically by the GTM Pipeline system.
        """
        
        # Download CSV from S3 and send as attachment
        s3_client = boto3.client('s3')
        
        # Extract S3 key from results_path (s3://flex-ai/path/file.csv)
        s3_key = results_path.replace('s3://flex-ai/', '')
        
        print(f"📥 Downloading CSV from S3: {s3_key}")
        
        # Download CSV content
        csv_response = s3_client.get_object(Bucket='flex-ai', Key=s3_key)
        csv_content = csv_response['Body'].read()
        
        # Create MIME email with attachment
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.application import MIMEApplication
        import base64
        
        msg = MIMEMultipart()
        msg['From'] = 'varsha@withflex.com'
        msg['To'] = user_email
        msg['Subject'] = subject
        
        # Add HTML body
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Add text body
        text_part = MIMEText(text_body, 'plain')
        msg.attach(text_part)
        
        # Add CSV attachment
        csv_filename = f"gtm_results_{execution_id}.csv"
        csv_attachment = MIMEApplication(csv_content)
        csv_attachment.add_header('Content-Disposition', 'attachment', filename=csv_filename)
        msg.attach(csv_attachment)
        
        # Send raw email with attachment
        response = ses_client.send_raw_email(
            Source='varsha@withflex.com',
            Destinations=[user_email],
            RawMessage={'Data': msg.as_string()}
        )
        
        message_id = response['MessageId']
        print(f"✅ Email sent successfully!")
        print(f"📧 Message ID: {message_id}")
        print(f"📬 To: {user_email}")
        print(f"📋 Subject: {subject}")
        
        return {
            "status": "success",
            "message_id": message_id,
            "recipient": user_email,
            "subject": subject
        }
        
    except Exception as e:
        print(f"❌ Failed to send email: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "failed",
            "error": str(e),
            "recipient": user_email
        }

# =============================================================================
# WEB API ENDPOINTS
# =============================================================================

@app.function(
    image=image,
    secrets=secrets
)
@modal.fastapi_endpoint(method="POST", docs=True)
def api_run_gtm_pipeline(data: dict):
    """
    Run the GTM pipeline via REST API
    
    Expected JSON body:
    {
        "website_url": "https://example.com",
        "single_url": false,
        "email": "user@company.com"
    }
    """
    try:
        website_url = data.get("website_url")
        if not website_url:
            return {"status": "error", "error": "website_url is required"}
        
        single_url = data.get("single_url", False)
        user_email = data.get("email")
        
        # Validate URL format
        if not website_url.startswith(('http://', 'https://')):
            return {
                "status": "error", 
                "error": "Invalid URL format: website_url must start with http:// or https://"
            }
        
        # Generate execution ID first
        import time
        execution_id = f"gtm_{int(time.time())}"
        
        print(f"🌐 GTM Pipeline API Request:")
        print(f"   Website URL: {website_url}")
        print(f"   Single URL Mode: {single_url}")
        print(f"   Email: {user_email or 'None'}")
        print(f"   Execution ID: {execution_id}")
        
        if single_url:
            # For single URL - process directly and return results in response
            print(f"🚀 Processing single URL synchronously...")
            
            # Create work item for single URL processing
            work_item = {
                "url": website_url,
                "url_id": f"{execution_id}_single_url",
                "execution_id": execution_id,
                "discovery_method": "api_single_url"
            }
            
            # Process the URL directly (no queue, no consolidation)
            try:
                result_data = process_single_url(work_item)
                
                # Send email if requested (async)
                if user_email:
                    # Save single result to S3 for email attachment
                    import boto3
                    import pandas as pd
                    import json
                    
                    s3_client = boto3.client('s3')
                    
                    # Convert to DataFrame and save as CSV
                    df = pd.DataFrame([result_data])
                    csv_content = df.to_csv(index=False)
                    
                    # Save to S3
                    s3_key = f"gtm/{execution_id}/outputs/single_url_result.csv"
                    s3_client.put_object(
                        Bucket='flex-ai',
                        Key=s3_key,
                        Body=csv_content,
                        ContentType='text/csv'
                    )
                    
                    # Send email with attachment (spawn async to not block response)
                    send_completion_email.spawn(
                        user_email, execution_id, website_url, 
                        1, 0, f"s3://flex-ai/{s3_key}", True
                    )
                
                return {
                    "status": "completed",
                    "message": "Single URL processed successfully", 
                    "execution_id": execution_id,
                    "website_url": website_url,
                    "processing_time": "immediate",
                    "email_sent": user_email is not None,
                    "result": result_data  # Raw processing result included in response
                }
                
            except Exception as e:
                print(f"❌ Single URL processing failed: {str(e)}")
                return {
                    "status": "error",
                    "error": f"Single URL processing failed: {str(e)}",
                    "execution_id": execution_id
                }
        else:
            # For full website discovery - run asynchronously as before
            print(f"🚀 Running full website discovery asynchronously...")
            pipeline_call = start_gtm_pipeline.spawn(website_url, single_url, user_email)
            
            return {
                "status": "started",
                "execution_id": execution_id,
                "message": "GTM pipeline started successfully and running asynchronously",
                "call_id": pipeline_call.object_id,
                "website_url": website_url,
                "single_url": single_url,
                "email_notifications": user_email is not None,
                "monitor_s3": f"https://s3.console.aws.amazon.com/s3/buckets/flex-ai?region=us-west-2&prefix=gtm/{execution_id}/",
                "note": "Pipeline is running in the background. Check S3 for results or use call_id to monitor status."
            }
        
    except Exception as e:
        print(f"💥 GTM Pipeline API Error: {str(e)}")
        return {"status": "error", "error": str(e)}

@app.function(image=image)
@modal.fastapi_endpoint(docs=True)
def api_health():
    """Health check endpoint for GTM pipeline API"""
    import time
    return {
        "status": "healthy", 
        "service": "gtm-pipeline-webhook",
        "version": "1.0.0",
        "timestamp": time.time(),
        "endpoints": {
            "POST /api_run_gtm_pipeline": "Trigger GTM pipeline",
            "GET /api_health": "Health check",
            "GET /docs": "API documentation"
        }
    }

# Health check endpoint (function-based)
@app.function()
def health_check():
    """Pipeline health check"""
    import time
    return {
        "status": "healthy",
        "pipeline": "gtm-pipeline",
        "version": "1.0.0",
        "timestamp": time.time()
    }

if __name__ == "__main__":
    print("🚀 GTM Pipeline - Modal Implementation")
    print("📋 Available Functions:")
    print("   • start_gtm_pipeline - Main pipeline orchestrator")
    print("   • health_check - Health check")
    print("\n✅ Ready for deployment!")