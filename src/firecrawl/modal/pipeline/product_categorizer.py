#!/usr/bin/env python3
"""
Product Categorizer for Modal Pipeline
Intelligently categorizes products to optimize HSA/FSA classification
Reduces token usage by filtering irrelevant products early
"""

import time
import re
from typing import Dict, List, Tuple

from .config import app, image, secrets, product_queue, categorization_queue
from .schemas import ExtractedProduct, CategorizedProduct, PRODUCT_CATEGORIES, MEDICAL_INDICATORS, EXCLUSION_INDICATORS

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=60,  # 1 minute per product (fast categorization)
    concurrency_limit=50  # 50 parallel workers for fast processing
)
def product_categorizer_worker():
    """
    Product Categorizer Worker
    Continuously processes extracted products and categorizes them
    """
    print("🏷️  Product Categorizer Worker started - waiting for products...")
    
    # Process products from queue
    while True:
        try:
            # Get product from queue (blocks until available)
            extracted_product: ExtractedProduct = product_queue.get()
            
            if extracted_product is None:  # Poison pill to stop worker
                print("💊 Received stop signal - shutting down worker")
                break
            
            print(f"📥 Categorizing: {extracted_product.name}")
            
            # Categorize product
            categorized_product = _categorize_single_product(extracted_product)
            
            if categorized_product:
                # Queue for HSA/FSA classification (or skip if excluded)
                if categorized_product.classification_priority < 5:  # Don't queue excluded products
                    categorization_queue.put(categorized_product)
                    print(f"✅ Categorized as {categorized_product.primary_category} (priority {categorized_product.classification_priority})")
                else:
                    print(f"⏭️  Skipped - excluded category: {categorized_product.primary_category}")
            else:
                print(f"❌ Failed to categorize: {extracted_product.name}")
                
        except Exception as e:
            print(f"❌ Error categorizing product: {str(e)}")
            continue

def _categorize_single_product(extracted_product: ExtractedProduct) -> CategorizedProduct:
    """Categorize a single product using keyword analysis and rule-based logic"""
    start_time = time.time()
    
    try:
        # Combine all text data for analysis
        text_data = _combine_product_text(extracted_product)
        text_lower = text_data.lower()
        
        print(f"   🔍 Analyzing product text ({len(text_data)} chars)")
        
        # Step 1: Check for exclusion indicators first
        exclusion_score = _calculate_exclusion_score(text_lower)
        if exclusion_score > 0.7:
            category_info = _get_exclusion_category(text_lower)
            return _create_categorized_product(
                extracted_product, 
                category_info['category'], 
                'excluded', 
                'excluded', 
                exclusion_score, 
                5,  # Skip classification
                time.time() - start_time
            )
        
        # Step 2: Analyze for medical/HSA/FSA indicators
        medical_score = _calculate_medical_score(text_lower)
        
        # Step 3: Find best matching category
        primary_category, secondary_category, confidence = _find_best_category_match(text_lower)
        
        # Step 4: Determine HSA/FSA likelihood and priority
        hsa_fsa_likelihood, priority = _determine_likelihood_and_priority(
            primary_category, medical_score, confidence
        )
        
        categorization_time = time.time() - start_time
        
        print(f"   📊 Category: {primary_category}, HSA/FSA: {hsa_fsa_likelihood}, Priority: {priority} ({categorization_time:.2f}s)")
        
        return _create_categorized_product(
            extracted_product,
            primary_category,
            secondary_category,
            hsa_fsa_likelihood,
            confidence,
            priority,
            categorization_time
        )
        
    except Exception as e:
        categorization_time = time.time() - start_time
        print(f"   ❌ Categorization error: {str(e)} (took {categorization_time:.2f}s)")
        
        # Return uncategorized product with medium priority
        return _create_categorized_product(
            extracted_product,
            'uncategorized',
            'unknown',
            'medium',
            0.5,
            3,  # Medium priority for manual review
            categorization_time
        )

def _combine_product_text(product: ExtractedProduct) -> str:
    """Combine all available product text for analysis"""
    text_parts = [product.name, product.description]
    
    if product.structured_data:
        # Add structured data fields
        for field in ['features', 'benefits', 'ingredients', 'medical_claims', 
                     'usage', 'specifications', 'additional_info', 'category']:
            value = product.structured_data.get(field)
            if value and isinstance(value, str):
                text_parts.append(value)
    
    # Clean and combine
    combined_text = ' '.join([part for part in text_parts if part])
    return combined_text

def _calculate_exclusion_score(text_lower: str) -> float:
    """Calculate how likely the product is to be excluded from HSA/FSA eligibility"""
    exclusion_matches = 0
    total_indicators = len(EXCLUSION_INDICATORS)
    
    for indicator in EXCLUSION_INDICATORS:
        if indicator in text_lower:
            exclusion_matches += 1
    
    return exclusion_matches / total_indicators if total_indicators > 0 else 0

def _calculate_medical_score(text_lower: str) -> float:
    """Calculate how medical/therapeutic the product appears to be"""
    medical_matches = 0
    total_indicators = len(MEDICAL_INDICATORS)
    
    for indicator in MEDICAL_INDICATORS:
        if indicator in text_lower:
            medical_matches += 1
    
    return medical_matches / total_indicators if total_indicators > 0 else 0

def _find_best_category_match(text_lower: str) -> Tuple[str, str, float]:
    """Find the best matching product category"""
    best_category = 'uncategorized'
    best_subcategory = 'unknown'
    best_score = 0.0
    
    # Check all categories
    for category_level in ['PRIMARY', 'SECONDARY', 'EXCLUDED']:
        categories = PRODUCT_CATEGORIES[category_level]
        
        for category_name, category_data in categories.items():
            keywords = category_data['keywords']
            matches = sum(1 for keyword in keywords if keyword in text_lower)
            
            if matches > 0:
                # Calculate score based on matches and category priority
                score = matches / len(keywords)
                
                # Boost score for primary categories
                if category_level == 'PRIMARY':
                    score *= 1.5
                elif category_level == 'SECONDARY':
                    score *= 1.2
                
                if score > best_score:
                    best_score = score
                    best_category = category_name
                    best_subcategory = category_level.lower()
    
    return best_category, best_subcategory, best_score

def _get_exclusion_category(text_lower: str) -> Dict[str, str]:
    """Determine which exclusion category the product belongs to"""
    excluded_categories = PRODUCT_CATEGORIES['EXCLUDED']
    
    for category_name, category_data in excluded_categories.items():
        keywords = category_data['keywords']
        if any(keyword in text_lower for keyword in keywords):
            return {'category': category_name, 'level': 'excluded'}
    
    return {'category': 'general_excluded', 'level': 'excluded'}

def _determine_likelihood_and_priority(category: str, medical_score: float, confidence: float) -> Tuple[str, int]:
    """Determine HSA/FSA likelihood and classification priority"""
    
    # Get category info from PRODUCT_CATEGORIES
    category_info = None
    for level in ['PRIMARY', 'SECONDARY', 'EXCLUDED']:
        if category in PRODUCT_CATEGORIES[level]:
            category_info = PRODUCT_CATEGORIES[level][category]
            break
    
    if not category_info:
        # Uncategorized product
        if medical_score > 0.3:
            return 'medium', 2
        else:
            return 'low', 3
    
    # Use category's predefined likelihood and priority
    base_likelihood = category_info['hsa_fsa_likelihood']
    base_priority = category_info['priority']
    
    # Adjust based on medical score
    if medical_score > 0.5:
        # Strong medical indicators - boost likelihood
        if base_likelihood == 'medium':
            return 'high', max(1, base_priority - 1)
        elif base_likelihood == 'low':
            return 'medium', max(2, base_priority - 1)
    elif medical_score < 0.1 and base_likelihood == 'high':
        # Weak medical indicators for supposedly high-likelihood category
        return 'medium', min(3, base_priority + 1)
    
    return base_likelihood, base_priority

def _create_categorized_product(
    extracted_product: ExtractedProduct,
    primary_category: str,
    secondary_category: str, 
    hsa_fsa_likelihood: str,
    confidence: float,
    priority: int,
    categorization_time: float
) -> CategorizedProduct:
    """Create a CategorizedProduct object with all fields"""
    
    return CategorizedProduct(
        url=extracted_product.url,
        batch_id=extracted_product.batch_id,
        name=extracted_product.name,
        description=extracted_product.description,
        structured_data=extracted_product.structured_data,
        extraction_time=extracted_product.extraction_time,
        # Categorization fields
        primary_category=primary_category,
        secondary_category=secondary_category,
        hsa_fsa_likelihood=hsa_fsa_likelihood,
        category_confidence=confidence,
        classification_priority=priority,
        categorization_time=categorization_time
    )

# Alternative function for batch processing
@app.function(
    image=image,
     
    secrets=secrets,
    timeout=900  # 15 minutes for batch
)
def categorize_products_batch(extracted_products: list) -> list:
    """
    Batch categorize multiple products at once
    Alternative to worker-based processing for smaller batches
    """
    print(f"🏷️  Batch categorizing {len(extracted_products)} products...")
    
    categorized_products = []
    
    for i, extracted_product in enumerate(extracted_products, 1):
        print(f"📥 Categorizing {i}/{len(extracted_products)}: {extracted_product.name}")
        
        categorized_product = _categorize_single_product(extracted_product)
        
        if categorized_product:
            categorized_products.append(categorized_product)
            print(f"✅ Categorized as {categorized_product.primary_category}")
        else:
            print(f"❌ Failed: {extracted_product.name}")
    
    print(f"✅ Batch categorization complete: {len(categorized_products)}/{len(extracted_products)} successful")
    return categorized_products