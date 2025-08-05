#!/usr/bin/env python3
"""
Stage 3: Categorization Dispatcher for Modal Pipeline
Reads extraction CSV from S3, uses GPT-4o-mini to categorize products, writes to S3
"""

import time
import json
import pandas as pd
from typing import List, Dict, Any

from .config import app, image, secrets, categorization_queue
from .s3_utils import S3Manager, create_dynamic_batches, combine_batch_results, BatchReference

try:
    import openai
except ImportError:
    openai = None

OPENAI_MODEL = "gpt-4o-mini"

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=3600  # 1 hour for dispatching and coordination
)
def stage3_categorization_dispatcher(
    execution_id: str,
    environment: str = "dev"
) -> dict:
    """
    Stage 3: Categorization Dispatcher
    Reads extraction CSV, creates dynamic batches, manages categorization workers
    
    Args:
        execution_id: Unique execution ID from previous stages
        environment: dev or prod environment
        
    Returns:
        Categorization results and statistics
    """
    
    start_time = time.time()
    
    print(f"🚀 STAGE 3: Categorization Dispatcher")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌍 Environment: {environment}")
    
    try:
        # Read extraction CSV from S3
        s3_manager = S3Manager()
        extraction_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "extraction", "extracted_products.csv"
        )
        
        print(f"📥 Reading extraction CSV from: {extraction_csv_path}")
        df = s3_manager.download_dataframe(extraction_csv_path)
        
        if len(df) == 0:
            raise Exception("No products found in extraction CSV")
        
        print(f"📊 Processing {len(df):,} products for categorization")
        
        # Create dynamic batches for categorization
        # Categorization is faster than extraction (~10 seconds per product)
        batch_references = create_dynamic_batches(
            df=df,
            execution_id=execution_id,
            stage="categorization",
            environment=environment,
            processing_time_per_item=0.17  # ~10 seconds per product
        )
        
        print(f"\n🚀 Starting {len(batch_references)} categorization workers...")
        
        # Queue batch references for workers
        for batch_ref in batch_references:
            categorization_queue.put(batch_ref)
        
        # Start categorization workers
        categorization_futures = []
        batch_size, max_workers = _calculate_worker_count(len(batch_references))
        
        for i in range(min(max_workers, len(batch_references))):
            future = categorization_worker.spawn()
            categorization_futures.append(future)
        
        print(f"✅ Started {len(categorization_futures)} categorization workers")
        
        # Wait for all workers to complete
        print(f"⏳ Waiting for categorization workers to complete...")
        completed_workers = 0
        
        for future in categorization_futures:
            try:
                result = future.get()  # This blocks until worker completes
                completed_workers += 1
                print(f"   ✅ Worker {completed_workers}/{len(categorization_futures)} completed")
            except Exception as e:
                print(f"   ❌ Worker failed: {str(e)}")
        
        # Combine all batch results into final CSV
        print(f"\n📋 Combining categorization results...")
        
        categorization_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "categorization", "categorized_products.csv"
        )
        
        final_df = combine_batch_results(batch_references, categorization_csv_path)
        
        dispatch_time = time.time() - start_time
        
        if len(final_df) > 0:
            # Calculate category distribution
            category_distribution = final_df['category'].value_counts().to_dict()
            
            print(f"\n✅ CATEGORIZATION DISPATCH COMPLETE!")
            print(f"📁 Results saved to: {categorization_csv_path}")
            print(f"📊 Successfully categorized: {len(final_df):,} products")
            print(f"🏷️  Category distribution: {len(category_distribution)} unique categories")
            print(f"⏱️  Total dispatch time: {dispatch_time/60:.1f} minutes")
            
            return {
                'execution_id': execution_id,
                'environment': environment,
                'input_products': len(df),
                'categorized_products': len(final_df),
                'categorization_csv_path': categorization_csv_path,
                'category_distribution': category_distribution,
                'batch_count': len(batch_references),
                'worker_count': len(categorization_futures),
                'dispatch_time': dispatch_time,
                'status': 'completed'
            }
        else:
            raise Exception("No products were successfully categorized")
            
    except Exception as e:
        dispatch_time = time.time() - start_time
        print(f"❌ CATEGORIZATION DISPATCH FAILED: {str(e)}")
        
        return {
            'execution_id': execution_id,
            'environment': environment,
            'input_products': 0,
            'categorized_products': 0,
            'categorization_csv_path': None,
            'dispatch_time': dispatch_time,
            'status': 'failed',
            'error': str(e)
        }

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=900,  # 15 minutes per worker
    max_containers=50  # Auto-scale up to 50 workers (faster than extraction)
)
def categorization_worker():
    """
    Categorization Worker - Processes one batch of products from S3
    """
    import os
    
    try:
        # Get batch reference from queue
        batch_ref: BatchReference = categorization_queue.get()
        
        if batch_ref is None:
            return  # Poison pill to stop worker
        
        print(f"🏷️  Processing batch {batch_ref.batch_number}: {batch_ref.item_count} products")
        
        # Initialize OpenAI
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key or not openai:
            raise Exception("OpenAI not available - cannot categorize products")
        
        openai.api_key = api_key
        
        # Load categories from mounted prompts
        categories = _load_categories()
        if not categories:
            raise Exception("Categories not loaded - cannot categorize products")
        
        # Load products from S3
        s3_manager = S3Manager()
        input_df = s3_manager.download_dataframe(batch_ref.s3_input_path)
        
        if len(input_df) == 0:
            print(f"⚠️  Empty batch {batch_ref.batch_number}")
            return
        
        # Process each product in the batch
        results = []
        for idx, row in input_df.iterrows():
            product_name = row.get('name', 'Unknown Product')
            product_description = row.get('description', '')
            
            print(f"   🤖 Categorizing: {product_name}")
            
            try:
                # Categorize product using OpenAI
                category_result = _categorize_single_product(
                    categories, product_name, product_description
                )
                
                if category_result:
                    # Add categorization fields to existing product data
                    product_data = row.to_dict()
                    product_data.update({
                        'category': category_result['category'],
                        'category_confidence': category_result['confidence'],
                        'category_reasoning': category_result['reasoning'],
                        'categorization_timestamp': time.time()
                    })
                    
                    results.append(product_data)
                    print(f"   ✅ Categorized as: {category_result['category']} ({category_result['confidence']:.2f})")
                else:
                    print(f"   ❌ Failed to categorize: {product_name}")
                    
            except Exception as e:
                print(f"   ❌ Error categorizing {product_name}: {str(e)}")
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
            print(f"❌ Batch {batch_ref.batch_number}: No successful categorizations")
            
    except Exception as e:
        print(f"❌ Worker error: {str(e)}")

def _load_categories() -> List[Dict[str, Any]]:
    """Load categories - simplified for Modal deployment"""
    
    try:
        # Basic categories based on your categories.txt file
        categories = [
            {"name": "Acne & Blemish Control", "description": "Skincare products for acne treatment"},
            {"name": "Allergy & Respiratory", "description": "Allergy medications and respiratory aids"},
            {"name": "Cold & Flu", "description": "Cold and flu medications and remedies"},
            {"name": "Dental & Oral Care", "description": "Dental care products and oral hygiene"},
            {"name": "Diabetes Care", "description": "Blood glucose monitors and diabetic supplies"},
            {"name": "Digestive Health", "description": "Digestive aids and stomach medications"},
            {"name": "Eye & Vision Care", "description": "Eye drops, reading glasses, contact supplies"},
            {"name": "First Aid & Wound Care", "description": "Bandages, antiseptics, wound care"},
            {"name": "Medical Equipment & Supplies", "description": "Medical devices and diagnostic equipment"},
            {"name": "Pain Relief & Anti-inflammatory", "description": "Pain medications and anti-inflammatory drugs"},
            {"name": "Skin Care & Dermatology", "description": "Therapeutic skin care products"},
            {"name": "Other / Miscellaneous", "description": "Products that don't fit other categories"}
        ]
        
        print(f"✅ Loaded {len(categories)} categories")
        return categories
        
    except Exception as e:
        print(f"❌ Error loading categories: {str(e)}")
        return []

def _categorize_single_product(
    categories: List[Dict[str, Any]], 
    product_name: str, 
    product_description: str
) -> Dict[str, Any]:
    """Categorize a single product using OpenAI GPT-4o-mini"""
    
    try:
        # Build categories list for prompt
        categories_text = ""
        for i, cat in enumerate(categories, 1):
            categories_text += f"{i}. {cat['name']}\n"
            if cat.get('description'):
                categories_text += f"   Description: {cat['description']}\n"
            if cat.get('keywords'):
                keywords_str = ', '.join(cat['keywords'][:10])  # Limit keywords for prompt length
                categories_text += f"   Keywords: {keywords_str}\n"
        
        # Create categorization prompt
        prompt = f"""You are a product categorization expert. Classify the following product into one of these categories:

{categories_text}

Product Name: {product_name}
Product Description: {product_description[:1000]}

Analyze the product and return your classification in this exact JSON format:
{{
  "category": "exact category name from the list above",
  "confidence": 0.85,
  "reasoning": "brief explanation of why this category fits best"
}}

Choose the most specific and accurate category. If unsure, pick the closest match."""
        
        # Make OpenAI API call
        if openai:
            response = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a precise product categorizer. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Validate result
            if 'category' in result and 'confidence' in result:
                return {
                    'category': result.get('category', 'Other / Miscellaneous'),
                    'confidence': float(result.get('confidence', 0.5)),
                    'reasoning': result.get('reasoning', 'No reasoning provided')
                }
            else:
                print(f"   ⚠️  Invalid response format: {content}")
                return None
                
        else:
            print(f"   ❌ OpenAI not available")
            return None
            
    except json.JSONDecodeError as e:
        print(f"   ❌ JSON parsing error: {str(e)}")
        return None
        
    except Exception as e:
        print(f"   ❌ Categorization error: {str(e)}")
        return None

def _calculate_worker_count(batch_count: int) -> tuple:
    """Calculate optimal worker count based on batch count"""
    if batch_count <= 10:
        return 25, min(10, batch_count)
    elif batch_count <= 50:
        return 50, min(20, batch_count)
    elif batch_count <= 200:
        return 100, min(30, batch_count)
    else:
        return 200, min(50, batch_count)