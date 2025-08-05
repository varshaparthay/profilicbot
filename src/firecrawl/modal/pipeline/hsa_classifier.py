#!/usr/bin/env python3
"""
HSA/FSA Classifier for Modal Pipeline
Performs targeted HSA/FSA classification using OpenAI with custom prompts
Optimized with category-specific prompts for token efficiency
"""

import time
import json
import os
from typing import Dict, Any, Optional

from .config import app, image, secrets, categorization_queue, classification_queue
from .schemas import CategorizedProduct, ClassifiedProduct

try:
    import openai
except ImportError:
    openai = None

OPENAI_MODEL = "gpt-4o-mini"

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=180,  # 3 minutes per classification
    concurrency_limit=15  # 15 parallel workers (respects OpenAI rate limits)
)
def hsa_classifier_worker():
    """
    HSA/FSA Classifier Worker
    Continuously processes categorized products and classifies HSA/FSA eligibility
    """
    # Initialize OpenAI
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("❌ OpenAI API key not available - cannot classify products")
        return
    
    if openai:
        openai.api_key = api_key
    
    # Load custom prompts
    eligibility_prompt = _load_custom_prompts()
    if not eligibility_prompt:
        print("❌ Custom prompts not loaded - cannot classify products")
        return
    
    print("🧠 HSA/FSA Classifier Worker started - waiting for products...")
    
    # Process products from queue
    while True:
        try:
            # Get categorized product from queue (blocks until available)
            categorized_product: CategorizedProduct = categorization_queue.get()
            
            if categorized_product is None:  # Poison pill to stop worker
                print("💊 Received stop signal - shutting down worker")
                break
            
            print(f"📥 Classifying: {categorized_product.name} (category: {categorized_product.primary_category})")
            
            # Skip classification for excluded products
            if categorized_product.classification_priority >= 5:
                print(f"⏭️  Skipping excluded product: {categorized_product.primary_category}")
                continue
            
            # Classify product
            classified_product = _classify_single_product(categorized_product, eligibility_prompt)
            
            if classified_product:
                # Queue for Turbopuffer upload
                classification_queue.put(classified_product)
                print(f"✅ Classified as {classified_product.eligibility_status}")
            else:
                print(f"❌ Failed to classify: {categorized_product.name}")
                
        except Exception as e:
            print(f"❌ Error classifying product: {str(e)}")
            continue

def _load_custom_prompts() -> Optional[str]:
    """Load custom prompts from mounted files"""
    print("🔍 Loading custom prompt files from /app/prompts/...")
    
    eligibility_path = "/app/prompts/feligibity.txt"
    guide_path = "/app/prompts/flex_product_guide.txt"
    
    try:
        # Load eligibility prompt
        if not os.path.exists(eligibility_path):
            print(f"❌ Eligibility prompt not found at: {eligibility_path}")
            return None
            
        with open(eligibility_path, 'r', encoding='utf-8') as f:
            eligibility_content = f.read().strip()
            
        if not eligibility_content:
            print("❌ Eligibility prompt file is empty")
            return None
            
        print(f"✅ Loaded eligibility prompt ({len(eligibility_content)} chars)")
        
        # Load flex guide if available
        flex_guide_content = ""
        if os.path.exists(guide_path):
            with open(guide_path, 'r', encoding='utf-8') as f:
                flex_guide_content = f.read().strip()
                print(f"✅ Loaded flex guide ({len(flex_guide_content)} chars)")
        else:
            print(f"⚠️  Flex guide not found at: {guide_path} (optional)")
        
        # Combine prompts
        if flex_guide_content:
            combined_prompt = f"{eligibility_content}\n\nAdditional Context:\n{flex_guide_content}"
        else:
            combined_prompt = eligibility_content
            
        print(f"📋 Total prompt length: {len(combined_prompt)} chars")
        return combined_prompt
        
    except Exception as e:
        print(f"❌ Error loading custom prompts: {str(e)}")
        return None

def _classify_single_product(categorized_product: CategorizedProduct, eligibility_prompt: str) -> ClassifiedProduct:
    """Classify a single product's HSA/FSA eligibility using OpenAI"""
    start_time = time.time()
    
    try:
        # Build category-optimized prompt
        optimized_prompt = _build_category_optimized_prompt(
            eligibility_prompt, 
            categorized_product
        )
        
        # Build product context for classification
        product_context = _build_product_context(categorized_product)
        
        print(f"   🤖 OpenAI classification (category: {categorized_product.primary_category}, priority: {categorized_product.classification_priority})")
        
        # Make OpenAI API call
        if openai:
            response = openai.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": optimized_prompt},
                    {"role": "user", "content": product_context}
                ],
                temperature=0.1,
                max_tokens=800,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            content = response.choices[0].message.content
            result = json.loads(content)
            
        else:
            print("   ❌ OpenAI not available")
            return None
        
        # Extract classification results
        eligibility_status = result.get('eligibilityStatus', 'unknown')
        eligibility_rationale = result.get('explanation', '') + ' | ' + result.get('additionalConsiderations', '')
        
        classification_time = time.time() - start_time
        total_processing_time = categorized_product.extraction_time + categorized_product.categorization_time + classification_time
        
        print(f"   ✅ Classification: {eligibility_status} ({classification_time:.1f}s)")
        
        # Create ClassifiedProduct object
        classified_product = ClassifiedProduct(
            url=categorized_product.url,
            batch_id=categorized_product.batch_id,
            name=categorized_product.name,
            description=categorized_product.description,
            structured_data=categorized_product.structured_data,
            extraction_time=categorized_product.extraction_time,
            primary_category=categorized_product.primary_category,
            secondary_category=categorized_product.secondary_category,
            hsa_fsa_likelihood=categorized_product.hsa_fsa_likelihood,
            category_confidence=categorized_product.category_confidence,
            # Classification fields
            eligibility_status=eligibility_status,
            eligibility_rationale=eligibility_rationale,
            classification_time=classification_time,
            total_processing_time=total_processing_time
        )
        
        return classified_product
        
    except json.JSONDecodeError as e:
        classification_time = time.time() - start_time
        print(f"   ❌ JSON parsing error: {str(e)} (took {classification_time:.1f}s)")
        return None
        
    except Exception as e:
        classification_time = time.time() - start_time
        print(f"   ❌ Classification error: {str(e)} (took {classification_time:.1f}s)")
        return None

def _build_category_optimized_prompt(base_prompt: str, categorized_product: CategorizedProduct) -> str:
    """Build category-specific optimized prompt to reduce token usage"""
    
    category = categorized_product.primary_category
    likelihood = categorized_product.hsa_fsa_likelihood
    priority = categorized_product.classification_priority
    
    # For high-priority, high-likelihood products - use full prompt
    if priority == 1 and likelihood == 'high':
        return base_prompt
    
    # For medium priority products - use abbreviated prompt
    elif priority == 2:
        abbreviated_prompt = f"""
You are an HSA/FSA eligibility expert. Classify this {category} product for HSA/FSA eligibility.

Key Rules:
- HSA/FSA eligible: Medical devices, treatments, preventive care, prescribed items
- NOT eligible: General wellness, cosmetic, fitness items without medical purpose
- Consider FDA approval, medical claims, therapeutic benefits

Respond in JSON format:
{{
  "eligibilityStatus": "eligible|not_eligible|requires_prescription|unclear",
  "explanation": "Brief reasoning",
  "additionalConsiderations": "Any important notes"
}}
"""
        return abbreviated_prompt.strip()
    
    # For low priority products - use basic prompt
    elif priority >= 3:
        basic_prompt = f"""
Determine if this {category} product is HSA/FSA eligible.

Rules: Medical purpose = eligible, Cosmetic/wellness = not eligible

JSON response:
{{
  "eligibilityStatus": "eligible|not_eligible|unclear", 
  "explanation": "Reason"
}}
"""
        return basic_prompt.strip()
    
    # Default to full prompt
    return base_prompt

def _build_product_context(categorized_product: CategorizedProduct) -> str:
    """Build concise product context for classification"""
    
    # Base product info
    context_parts = [
        f"Product: {categorized_product.name}",
        f"Category: {categorized_product.primary_category}",
        f"HSA/FSA Likelihood: {categorized_product.hsa_fsa_likelihood}"
    ]
    
    # Add description (truncated for token efficiency)
    description = categorized_product.description
    if len(description) > 1000:
        description = description[:1000] + "..."
    context_parts.append(f"Description: {description}")
    
    # Add key structured data
    if categorized_product.structured_data:
        structured = categorized_product.structured_data
        
        # Add most relevant fields for classification
        relevant_fields = ['medical_claims', 'features', 'benefits', 'ingredients', 'specifications']
        for field in relevant_fields:
            value = structured.get(field)
            if value and isinstance(value, str) and len(value) > 10:
                # Truncate long fields
                if len(value) > 300:
                    value = value[:300] + "..."
                context_parts.append(f"{field.title()}: {value}")
    
    return "\n".join(context_parts)

# Alternative function for batch processing
@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1800  # 30 minutes for batch
)
def classify_products_batch(categorized_products: list) -> list:
    """
    Batch classify multiple products at once
    Alternative to worker-based processing for smaller batches
    """
    # Initialize OpenAI
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        print("❌ OpenAI API key not available")
        return []
    
    if openai:
        openai.api_key = api_key
    
    # Load custom prompts
    eligibility_prompt = _load_custom_prompts()
    if not eligibility_prompt:
        print("❌ Custom prompts not loaded")
        return []
    
    print(f"🧠 Batch classifying {len(categorized_products)} products...")
    
    classified_products = []
    
    for i, categorized_product in enumerate(categorized_products, 1):
        print(f"📥 Classifying {i}/{len(categorized_products)}: {categorized_product.name}")
        
        # Skip excluded products
        if categorized_product.classification_priority >= 5:
            print(f"⏭️  Skipped excluded: {categorized_product.primary_category}")
            continue
        
        classified_product = _classify_single_product(categorized_product, eligibility_prompt)
        
        if classified_product:
            classified_products.append(classified_product)
            print(f"✅ Classified as {classified_product.eligibility_status}")
        else:
            print(f"❌ Failed: {categorized_product.name}")
        
        # Small delay to respect rate limits
        time.sleep(0.1)
    
    print(f"✅ Batch classification complete: {len(classified_products)} successful")
    return classified_products