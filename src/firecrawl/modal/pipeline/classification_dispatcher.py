#!/usr/bin/env python3
"""
Stage 4: HSA/FSA Classification Dispatcher for Modal Pipeline
Reads categorized CSV from S3, uses category-specific guides for targeted classification
"""

import time
import json
import pandas as pd
from typing import List, Dict, Any

from .config import app, image, secrets, classification_queue
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
def stage4_classification_dispatcher(
    execution_id: str,
    environment: str = "dev"
) -> dict:
    """
    Stage 4: HSA/FSA Classification Dispatcher
    Reads categorized CSV, creates dynamic batches, manages classification workers
    
    Args:
        execution_id: Unique execution ID from previous stages
        environment: dev or prod environment
        
    Returns:
        Classification results and statistics
    """
    
    start_time = time.time()
    
    print(f"🚀 STAGE 4: HSA/FSA Classification Dispatcher")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌍 Environment: {environment}")
    
    try:
        # Read categorized CSV from S3
        s3_manager = S3Manager()
        categorization_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "categorization", "categorized_products.csv"
        )
        
        print(f"📥 Reading categorized CSV from: {categorization_csv_path}")
        df = s3_manager.download_dataframe(categorization_csv_path)
        
        if len(df) == 0:
            raise Exception("No products found in categorization CSV")
        
        print(f"📊 Processing {len(df):,} products for HSA/FSA classification")
        
        # Show category distribution
        category_counts = df['category'].value_counts()
        print(f"🏷️  Category distribution:")
        for category, count in category_counts.head(10).items():
            print(f"   {category}: {count} products")
        
        # Create dynamic batches for classification
        # Classification takes ~15 seconds per product (longer than categorization)
        batch_references = create_dynamic_batches(
            df=df,
            execution_id=execution_id,
            stage="classification",
            environment=environment,
            processing_time_per_item=0.25  # ~15 seconds per product
        )
        
        print(f"\n🚀 Starting {len(batch_references)} classification workers...")
        
        # Queue batch references for workers
        for batch_ref in batch_references:
            classification_queue.put(batch_ref)
        
        # Start classification workers
        classification_futures = []
        batch_size, max_workers = _calculate_worker_count(len(batch_references))
        
        for i in range(min(max_workers, len(batch_references))):
            future = classification_worker.spawn()
            classification_futures.append(future)
        
        print(f"✅ Started {len(classification_futures)} classification workers")
        
        # Wait for all workers to complete
        print(f"⏳ Waiting for classification workers to complete...")
        completed_workers = 0
        
        for future in classification_futures:
            try:
                result = future.get()  # This blocks until worker completes
                completed_workers += 1
                print(f"   ✅ Worker {completed_workers}/{len(classification_futures)} completed")
            except Exception as e:
                print(f"   ❌ Worker failed: {str(e)}")
        
        # Combine all batch results into final CSV
        print(f"\n📋 Combining classification results...")
        
        classification_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "classification", "classified_products.csv"
        )
        
        final_df = combine_batch_results(batch_references, classification_csv_path)
        
        dispatch_time = time.time() - start_time
        
        if len(final_df) > 0:
            # Calculate eligibility distribution
            eligibility_distribution = final_df['hsa_fsa_status'].value_counts().to_dict()
            
            print(f"\n✅ CLASSIFICATION DISPATCH COMPLETE!")
            print(f"📁 Results saved to: {classification_csv_path}")
            print(f"📊 Successfully classified: {len(final_df):,} products")
            print(f"🏥 HSA/FSA eligibility distribution:")
            for status, count in eligibility_distribution.items():
                print(f"   {status}: {count} products")
            print(f"⏱️  Total dispatch time: {dispatch_time/60:.1f} minutes")
            
            return {
                'execution_id': execution_id,
                'environment': environment,
                'input_products': len(df),
                'classified_products': len(final_df),
                'classification_csv_path': classification_csv_path,
                'eligibility_distribution': eligibility_distribution,
                'batch_count': len(batch_references),
                'worker_count': len(classification_futures),
                'dispatch_time': dispatch_time,
                'status': 'completed'
            }
        else:
            raise Exception("No products were successfully classified")
            
    except Exception as e:
        dispatch_time = time.time() - start_time
        print(f"❌ CLASSIFICATION DISPATCH FAILED: {str(e)}")
        
        return {
            'execution_id': execution_id,
            'environment': environment,
            'input_products': 0,
            'classified_products': 0,
            'classification_csv_path': None,
            'dispatch_time': dispatch_time,
            'status': 'failed',
            'error': str(e)
        }

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1200,  # 20 minutes per worker
    max_containers=15  # Limited by OpenAI rate limits
)
def classification_worker():
    """
    Classification Worker - Processes one batch of categorized products from S3
    """
    import os
    
    try:
        # Get batch reference from queue
        batch_ref: BatchReference = classification_queue.get()
        
        if batch_ref is None:
            return  # Poison pill to stop worker
        
        print(f"🧠 Processing batch {batch_ref.batch_number}: {batch_ref.item_count} products")
        
        # Initialize OpenAI
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key or not openai:
            raise Exception("OpenAI not available - cannot classify products")
        
        openai.api_key = api_key
        
        # Load classification guides
        eligibility_prompt = _load_eligibility_prompt()
        category_guides = _load_category_guides()
        
        if not eligibility_prompt:
            raise Exception("Eligibility prompt not loaded - cannot classify products")
        
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
            product_category = row.get('category', 'Other / Miscellaneous')
            product_description = row.get('description', '')
            
            print(f"   🤖 Classifying: {product_name} (category: {product_category})")
            
            try:
                # Get category-specific guide
                category_guide = _get_category_guide(category_guides, product_category)
                
                # Classify product using OpenAI with targeted prompt
                classification_result = _classify_single_product(
                    eligibility_prompt, category_guide, product_name, 
                    product_description, product_category
                )
                
                if classification_result:
                    # Add classification fields to existing product data
                    product_data = row.to_dict()
                    product_data.update({
                        'hsa_fsa_status': classification_result['status'],
                        'hsa_fsa_reasoning': classification_result['reasoning'],
                        'hsa_fsa_confidence': classification_result['confidence'],
                        'classification_timestamp': time.time()
                    })
                    
                    results.append(product_data)
                    print(f"   ✅ Classified as: {classification_result['status']} ({classification_result['confidence']:.2f})")
                else:
                    print(f"   ❌ Failed to classify: {product_name}")
                    
            except Exception as e:
                print(f"   ❌ Error classifying {product_name}: {str(e)}")
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
            print(f"❌ Batch {batch_ref.batch_number}: No successful classifications")
            
    except Exception as e:
        print(f"❌ Worker error: {str(e)}")

def _load_eligibility_prompt() -> str:
    """Load main HSA/FSA eligibility prompt"""
    
    try:
        # For now, return a basic HSA/FSA prompt since we can't mount files
        eligibility_content = """
You are an HSA/FSA eligibility expert. Determine if products are eligible for Health Savings Account (HSA) or Flexible Spending Account (FSA) reimbursement.

HSA/FSA Eligible Products:
- Medical equipment and supplies (blood pressure monitors, thermometers, etc.)
- Prescription medications and over-the-counter drugs with prescription
- First aid supplies and wound care
- Dental and oral care products
- Eye care products (reading glasses, contact solution, etc.)
- Allergy and respiratory products
- Pain relief medications
- Medical devices for specific conditions

NOT Eligible:
- General wellness products without medical purpose
- Cosmetics and beauty products
- General fitness equipment
- Household cleaning products
- Food and supplements (unless prescribed)

Return your assessment with confidence level and reasoning.
        """.strip()
        
        print(f"✅ Loaded eligibility prompt ({len(eligibility_content)} chars)")
        return eligibility_content
        
    except Exception as e:
        print(f"❌ Error loading eligibility prompt: {str(e)}")
        return None

def _load_category_guides() -> Dict[str, str]:
    """Load category-specific guides - simplified for Modal deployment"""
    
    try:
        # Basic category-specific guides for HSA/FSA classification
        category_guides = {
            "Medical Equipment & Supplies": "Focus on diagnostic and therapeutic medical devices. Most are HSA/FSA eligible.",
            "First Aid & Wound Care": "Generally HSA/FSA eligible - includes bandages, antiseptics, wound care supplies.",
            "Dental & Oral Care": "HSA/FSA eligible - includes dental tools, fluoride products, oral care devices.",
            "Eye & Vision Care": "HSA/FSA eligible - includes reading glasses, contact supplies, eye drops.",
            "Pain Relief & Anti-inflammatory": "Over-the-counter pain medications typically HSA/FSA eligible.",
            "Allergy & Respiratory": "HSA/FSA eligible - includes allergy medications, breathing aids, air purifiers for medical use.",
            "Diabetes Care": "HSA/FSA eligible - includes glucose monitors, test strips, diabetic supplies.",
            "General": "Evaluate based on medical necessity and therapeutic purpose."
        }
        
        print(f"✅ Loaded {len(category_guides)} category-specific guides")
        return category_guides
        
    except Exception as e:
        print(f"❌ Error loading category guides: {str(e)}")
        return {}

def _get_category_guide(category_guides: Dict[str, str], product_category: str) -> str:
    """Get the specific guide for a product's category"""
    
    # Try exact match first
    if product_category in category_guides:
        return category_guides[product_category]
    
    # Try partial matches (for variations in category names)
    for guide_category, guide_text in category_guides.items():
        if guide_category.lower() in product_category.lower() or product_category.lower() in guide_category.lower():
            return guide_text
    
    # Return general guide or empty string if no match
    general_guide = category_guides.get('General', category_guides.get('Other', ''))
    
    if general_guide:
        print(f"   ℹ️  Using general guide for category: {product_category}")
    else:
        print(f"   ⚠️  No specific guide found for category: {product_category}")
    
    return general_guide

def _classify_single_product(
    eligibility_prompt: str,
    category_guide: str,
    product_name: str,
    product_description: str,
    product_category: str
) -> Dict[str, Any]:
    """Classify a single product's HSA/FSA eligibility using targeted prompts"""
    
    try:
        # Build targeted classification prompt
        prompt_parts = [eligibility_prompt]
        
        # Add category-specific guide if available
        if category_guide:
            prompt_parts.append(f"\nCategory-Specific Guidelines for {product_category}:")
            prompt_parts.append(category_guide)
        
        # Add product information
        prompt_parts.append(f"\nProduct to Classify:")
        prompt_parts.append(f"Name: {product_name}")
        prompt_parts.append(f"Category: {product_category}")
        prompt_parts.append(f"Description: {product_description[:1500]}")  # Limit length
        
        prompt_parts.append(f"""
Analyze this product and determine its HSA/FSA eligibility. Return your response in this exact JSON format:
{{
  "status": "eligible|not_eligible|prescription_required|unclear",
  "reasoning": "detailed explanation of your decision based on HSA/FSA rules and category guidelines",
  "confidence": 0.85
}}

Focus on the specific category guidelines and HSA/FSA eligibility criteria.""")
        
        full_prompt = "\n".join(prompt_parts)
        
        # Make OpenAI API call
        if openai:
            response = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are an HSA/FSA eligibility expert. Always respond with valid JSON."},
                    {"role": "user", "content": full_prompt}
                ],
                temperature=0.1,
                max_tokens=500,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Validate result
            if 'status' in result and 'reasoning' in result:
                return {
                    'status': result.get('status', 'unclear'),
                    'reasoning': result.get('reasoning', 'No reasoning provided'),
                    'confidence': float(result.get('confidence', 0.5))
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
        print(f"   ❌ Classification error: {str(e)}")
        return None

def _calculate_worker_count(batch_count: int) -> tuple:
    """Calculate optimal worker count based on batch count (limited by OpenAI rate limits)"""
    if batch_count <= 5:
        return 25, min(5, batch_count)
    elif batch_count <= 20:
        return 50, min(10, batch_count)
    elif batch_count <= 50:
        return 100, min(15, batch_count)
    else:
        return 200, min(15, batch_count)  # Max 15 workers due to OpenAI limits