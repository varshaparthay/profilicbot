#!/usr/bin/env python3
"""
Stage 2: Extraction Dispatcher for Modal Pipeline
Reads discovery CSV from S3, creates dynamic batches, and manages extraction workers
"""

import time
import pandas as pd
from typing import List

from .config import app, image, secrets, url_queue
from .s3_utils import S3Manager, create_dynamic_batches, combine_batch_results, BatchReference

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=3600  # 1 hour for dispatching and coordination
)
def stage2_extraction_dispatcher(
    execution_id: str,
    environment: str = "dev",
    max_products: int = None
) -> dict:
    """
    Stage 2: Extraction Dispatcher
    Reads discovery CSV, creates dynamic batches, manages extraction workers
    
    Args:
        execution_id: Unique execution ID from discovery stage
        environment: dev or prod environment
        max_products: Optional limit on number of products to process
        
    Returns:
        Extraction results and statistics
    """
    
    start_time = time.time()
    
    print(f"🚀 STAGE 2: Extraction Dispatcher")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌍 Environment: {environment}")
    
    try:
        # Read discovery CSV from S3
        s3_manager = S3Manager()
        discovery_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "discovery", "discovered_urls.csv"
        )
        
        print(f"📥 Reading discovery CSV from: {discovery_csv_path}")
        df = s3_manager.download_dataframe(discovery_csv_path)
        
        if len(df) == 0:
            raise Exception("No URLs found in discovery CSV")
        
        # Apply max_products limit if specified
        original_count = len(df)
        if max_products and len(df) > max_products:
            df = df.head(max_products)
            print(f"⚠️  Limited to {max_products:,} products (from {original_count:,})")
        
        print(f"📊 Processing {len(df):,} URLs for extraction")
        
        # Create dynamic batches for extraction
        # Each URL takes ~1 minute to extract
        batch_references = create_dynamic_batches(
            df=df,
            execution_id=execution_id,
            stage="extraction",
            environment=environment,
            processing_time_per_item=1.0  # 1 minute per URL
        )
        
        print(f"\n🚀 Starting {len(batch_references)} extraction workers...")
        
        # Queue batch references for workers
        for batch_ref in batch_references:
            url_queue.put(batch_ref)
        
        # Start extraction workers
        extraction_futures = []
        batch_size, max_workers = _calculate_worker_count(len(batch_references))
        
        for i in range(min(max_workers, len(batch_references))):
            future = extraction_worker.spawn()
            extraction_futures.append(future)
        
        print(f"✅ Started {len(extraction_futures)} extraction workers")
        
        # Wait for all workers to complete
        print(f"⏳ Waiting for extraction workers to complete...")
        completed_workers = 0
        
        for future in extraction_futures:
            try:
                result = future.get()  # This blocks until worker completes
                completed_workers += 1
                print(f"   ✅ Worker {completed_workers}/{len(extraction_futures)} completed")
            except Exception as e:
                print(f"   ❌ Worker failed: {str(e)}")
        
        # Combine all batch results into final CSV
        print(f"\n📋 Combining extraction results...")
        
        extraction_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "extraction", "extracted_products.csv"
        )
        
        final_df = combine_batch_results(batch_references, extraction_csv_path)
        
        dispatch_time = time.time() - start_time
        
        if len(final_df) > 0:
            print(f"\n✅ EXTRACTION DISPATCH COMPLETE!")
            print(f"📁 Results saved to: {extraction_csv_path}")
            print(f"📊 Successfully extracted: {len(final_df):,} products")
            print(f"⏱️  Total dispatch time: {dispatch_time/60:.1f} minutes")
            
            return {
                'execution_id': execution_id,
                'environment': environment,
                'input_urls': len(df),
                'extracted_products': len(final_df),
                'extraction_csv_path': extraction_csv_path,
                'batch_count': len(batch_references),
                'worker_count': len(extraction_futures),
                'dispatch_time': dispatch_time,
                'status': 'completed'
            }
        else:
            raise Exception("No products were successfully extracted")
            
    except Exception as e:
        dispatch_time = time.time() - start_time
        print(f"❌ EXTRACTION DISPATCH FAILED: {str(e)}")
        
        return {
            'execution_id': execution_id,
            'environment': environment,
            'input_urls': 0,
            'extracted_products': 0,
            'extraction_csv_path': None,
            'dispatch_time': dispatch_time,
            'status': 'failed',
            'error': str(e)
        }

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1800,  # 30 minutes per worker
    max_containers=100  # Auto-scale up to 100 workers
)
def extraction_worker():
    """
    Extraction Worker - Processes one batch of URLs from S3
    """
    import os
    
    try:
        # Get batch reference from queue
        batch_ref: BatchReference = url_queue.get()
        
        if batch_ref is None:
            return  # Poison pill to stop worker
        
        print(f"🔍 Processing batch {batch_ref.batch_number}: {batch_ref.item_count} URLs")
        
        # Initialize Firecrawl
        firecrawl = None
        try:
            from firecrawl import FirecrawlApp
            api_key = os.environ.get('FIRECRAWL_API_KEY')
            if api_key:
                firecrawl = FirecrawlApp(api_key=api_key)
        except ImportError:
            firecrawl = None
        
        if not firecrawl:
            raise Exception("Firecrawl not available - cannot extract products")
        
        # Load URLs from S3
        s3_manager = S3Manager()
        input_df = s3_manager.download_dataframe(batch_ref.s3_input_path)
        
        if len(input_df) == 0:
            print(f"⚠️  Empty batch {batch_ref.batch_number}")
            return
        
        # Process each URL in the batch
        results = []
        for idx, row in input_df.iterrows():
            url = row['url']
            estimated_name = row.get('estimated_name', 'Unknown Product')
            
            print(f"   🤖 Extracting: {url}")
            
            try:
                # Extract product data using Firecrawl
                extracted_data = _extract_single_product(firecrawl, url, estimated_name)
                if extracted_data:
                    results.append(extracted_data)
                    print(f"   ✅ Extracted: {extracted_data['name']}")
                else:
                    print(f"   ❌ Failed to extract: {url}")
                    
            except Exception as e:
                print(f"   ❌ Error extracting {url}: {str(e)}")
                continue
        
        # Save results to S3
        if results:
            results_df = pd.DataFrame(results)
            success = s3_manager.upload_dataframe(results_df, batch_ref.s3_output_path)
            
            if success:
                print(f"✅ Batch {batch_ref.batch_number} complete: {len(results)}/{batch_ref.item_count} successful")
            else:
                print(f"❌ Failed to save batch {batch_ref.batch_number} results")
        else:
            print(f"❌ Batch {batch_ref.batch_number}: No successful extractions")
            
    except Exception as e:
        print(f"❌ Worker error: {str(e)}")

def _extract_single_product(firecrawl, url: str, estimated_name: str) -> dict:
    """Extract comprehensive product data from a single URL"""
    
    try:
        # Import schemas
        from .schemas import ProductExtractionSchema
        
        # Firecrawl structured extraction
        scrape_result = firecrawl.scrape_url(
            url,
            formats=['extract'],
            extract={
                'schema': ProductExtractionSchema.model_json_schema()
            }
        )
        
        # Check if extraction was successful
        success = getattr(scrape_result, 'success', None)
        if not success:
            return None
        
        # Get extracted data
        extract_data = getattr(scrape_result, 'extract', {})
        if not extract_data:
            return None
        
        # Get markdown content for comprehensive description building
        markdown = getattr(scrape_result, 'markdown', '')
        
        # Build comprehensive description
        comprehensive_description = _build_comprehensive_description(extract_data, markdown)
        
        # Return standardized product data
        return {
            'url': url,
            'name': extract_data.get('name', estimated_name),
            'description': comprehensive_description,
            'price': extract_data.get('price', ''),
            'brand': extract_data.get('brand', ''),
            'ingredients': extract_data.get('ingredients', ''),
            'features': extract_data.get('features', ''),
            'usage': extract_data.get('usage', ''),
            'specifications': extract_data.get('specifications', ''),
            'medical_claims': extract_data.get('medical_claims', ''),
            'category': extract_data.get('category', ''),
            'benefits': extract_data.get('benefits', ''),
            'warranty_support': extract_data.get('warranty_support', ''),
            'additional_info': extract_data.get('additional_info', ''),
            'extraction_timestamp': time.time()
        }
        
    except Exception as e:
        print(f"   ❌ Extraction error for {url}: {str(e)}")
        return None

def _build_comprehensive_description(extract_data: dict, markdown: str) -> str:
    """Build comprehensive 2000+ character product description"""
    
    description_parts = []
    
    # Start with main description
    main_desc = extract_data.get('description', '').strip()
    if main_desc:
        description_parts.append(main_desc)
    
    # Add structured data fields
    fields = ['features', 'benefits', 'ingredients', 'usage', 'specifications', 
              'medical_claims', 'warranty_support', 'additional_info']
    
    for field in fields:
        value = extract_data.get(field, '').strip()
        if value:
            field_name = field.replace('_', ' ').title()
            description_parts.append(f"{field_name}: {value}")
    
    # Join all parts
    comprehensive_desc = ' | '.join(description_parts)
    
    # If still too short, supplement with relevant markdown content
    if len(comprehensive_desc) < 1500 and markdown:
        markdown_lines = [line.strip() for line in markdown.split('\n') if line.strip()]
        
        # Filter for product-relevant content
        product_lines = []
        skip_patterns = ['navigation', 'menu', 'header', 'footer', 'cart', 'checkout']
        
        for line in markdown_lines:
            line_lower = line.lower()
            if (len(line) > 20 and 
                not any(skip in line_lower for skip in skip_patterns) and
                any(keyword in line_lower for keyword in [
                    'product', 'benefit', 'feature', 'ingredient', 'use', 'apply',
                    'helps', 'support', 'improve', 'reduce', 'enhance', 'provide'
                ])):
                product_lines.append(line)
        
        if product_lines:
            markdown_supplement = ' | '.join(product_lines[:10])
            comprehensive_desc += f" | Additional Details: {markdown_supplement}"
    
    # Ensure minimum quality
    if len(comprehensive_desc) < 200:
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
    
    # Clean up
    comprehensive_desc = comprehensive_desc.replace('  ', ' ').replace(' | |', ' |').strip()
    
    return comprehensive_desc

def _calculate_worker_count(batch_count: int) -> tuple:
    """Calculate optimal worker count based on batch count"""
    if batch_count <= 10:
        return 25, min(10, batch_count)
    elif batch_count <= 100:
        return 25, min(30, batch_count)
    elif batch_count <= 500:
        return 50, min(50, batch_count)
    else:
        return 100, min(100, batch_count)