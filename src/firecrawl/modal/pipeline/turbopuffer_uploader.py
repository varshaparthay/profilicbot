#!/usr/bin/env python3
"""
Turbopuffer Uploader for Modal Pipeline
Uploads classified products to Turbopuffer vector database with embeddings
Enables semantic search and retrieval capabilities
"""

import time
import os
from typing import List, Dict, Any

from .config import app, image, secrets, classification_queue, turbopuffer_queue
from .schemas import ClassifiedProduct, TurbopufferProduct

try:
    import turbopuffer as tpuf
except ImportError:
    tpuf = None

try:
    import openai
except ImportError:
    openai = None

EMBEDDING_MODEL = "text-embedding-ada-002"
BATCH_SIZE = 100  # Upload in batches for efficiency

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=600,  # 10 minutes per batch upload
    concurrency_limit=10  # 10 parallel workers for batch uploads
)
def turbopuffer_uploader_worker():
    """
    Turbopuffer Uploader Worker
    Continuously processes classified products and uploads to Turbopuffer
    """
    # Initialize Turbopuffer
    api_key = os.environ.get('TURBOPUFFER_API_KEY')
    if not api_key or not tpuf:
        print("❌ Turbopuffer not available - skipping uploads")
        return
    
    # Initialize OpenAI for embeddings
    openai_key = os.environ.get('OPENAI_API_KEY')
    if not openai_key or not openai:
        print("❌ OpenAI not available for embeddings - skipping uploads")
        return
    
    openai.api_key = openai_key
    
    # Get namespace from environment or use default
    namespace = os.environ.get('TURBOPUFFER_NAMESPACE', 'ecommerce-products')
    
    print(f"🚀 Turbopuffer Uploader Worker started - namespace: {namespace}")
    
    # Process products from queue in batches
    batch = []
    
    while True:
        try:
            # Get classified product from queue (blocks until available)
            classified_product: ClassifiedProduct = classification_queue.get()
            
            if classified_product is None:  # Poison pill to stop worker
                print("💊 Received stop signal - processing final batch and shutting down")
                if batch:
                    _upload_batch_to_turbopuffer(batch, namespace)
                break
            
            print(f"📥 Queuing for upload: {classified_product.name}")
            batch.append(classified_product)
            
            # Upload when batch is full
            if len(batch) >= BATCH_SIZE:
                print(f"📤 Uploading batch of {len(batch)} products...")
                uploaded_products = _upload_batch_to_turbopuffer(batch, namespace)
                
                # Queue uploaded products for results collection
                for uploaded_product in uploaded_products:
                    if uploaded_product:
                        turbopuffer_queue.put(uploaded_product)
                
                batch = []  # Reset batch
                
        except Exception as e:
            print(f"❌ Error processing upload queue: {str(e)}")
            continue

def _upload_batch_to_turbopuffer(batch: List[ClassifiedProduct], namespace: str) -> List[TurbopufferProduct]:
    """Upload a batch of classified products to Turbopuffer"""
    start_time = time.time()
    uploaded_products = []
    
    try:
        print(f"   🧠 Generating embeddings for {len(batch)} products...")
        
        # Generate embeddings for all products in batch
        embedding_texts = []
        for product in batch:
            # Create text for embedding (description + key features)
            embedding_text = _create_embedding_text(product)
            embedding_texts.append(embedding_text)
        
        # Get embeddings from OpenAI
        if openai:
            response = openai.embeddings.create(
                model=EMBEDDING_MODEL,
                input=embedding_texts
            )
            embeddings = [item.embedding for item in response.data]
        else:
            print("   ❌ OpenAI not available for embeddings")
            return []
        
        print(f"   ✅ Generated {len(embeddings)} embeddings")
        
        # Prepare Turbopuffer records
        print(f"   📦 Preparing Turbopuffer records...")
        records = []
        
        for i, (product, embedding) in enumerate(zip(batch, embeddings)):
            upload_timestamp = time.time()
            
            # Create unique ID for Turbopuffer
            turbopuffer_id = f"{product.batch_id}-{hash(product.url) % 1000000}"
            
            record = {
                "id": turbopuffer_id,
                "values": embedding,
                "attributes": {
                    "name": product.name,
                    "url": product.url,
                    "batch_id": product.batch_id,
                    "primary_category": product.primary_category,
                    "secondary_category": product.secondary_category,
                    "eligibility_status": product.eligibility_status,
                    "hsa_fsa_likelihood": product.hsa_fsa_likelihood,
                    "upload_date": upload_timestamp,
                    "description_preview": product.description[:200] if product.description else "",
                    # Processing metadata
                    "extraction_time": product.extraction_time,
                    "classification_time": product.classification_time,
                    "total_processing_time": product.total_processing_time
                }
            }
            records.append(record)
            
            # Create TurbopufferProduct object
            turbopuffer_product = TurbopufferProduct(
                url=product.url,
                batch_id=product.batch_id,
                name=product.name,
                description=product.description,
                structured_data=product.structured_data,
                extraction_time=product.extraction_time,
                primary_category=product.primary_category,
                secondary_category=product.secondary_category,
                eligibility_status=product.eligibility_status,
                eligibility_rationale=product.eligibility_rationale,
                classification_time=product.classification_time,
                # Turbopuffer fields
                turbopuffer_id=turbopuffer_id,
                embedding_vector=embedding,
                namespace=namespace,
                upload_timestamp=upload_timestamp,
                upload_success=False,  # Will be updated after upload
                total_processing_time=product.total_processing_time
            )
            uploaded_products.append(turbopuffer_product)
        
        # Upload to Turbopuffer
        print(f"   🚀 Uploading to Turbopuffer namespace: {namespace}")
        
        if tpuf:
            # Initialize Turbopuffer client
            client = tpuf.Turbopuffer()
            
            # Upload records
            client.upsert(namespace, records)
            
            print(f"   ✅ Successfully uploaded {len(records)} records to Turbopuffer")
            
            # Mark all products as successfully uploaded
            for product in uploaded_products:
                product.upload_success = True
        else:
            print("   ❌ Turbopuffer client not available")
            return []
        
        upload_time = time.time() - start_time
        print(f"   ⏱️  Batch upload completed in {upload_time:.1f}s")
        
        return uploaded_products
        
    except Exception as e:
        upload_time = time.time() - start_time
        print(f"   ❌ Batch upload failed: {str(e)} (took {upload_time:.1f}s)")
        
        # Mark all products as failed uploads
        for product in uploaded_products:
            product.upload_success = False
        
        return uploaded_products

def _create_embedding_text(product: ClassifiedProduct) -> str:
    """Create optimized text for embedding generation"""
    
    # Start with core product info
    text_parts = [
        product.name,
        f"Category: {product.primary_category}",
        f"HSA/FSA Status: {product.eligibility_status}"
    ]
    
    # Add description (truncated to control embedding size)
    description = product.description
    if len(description) > 800:
        description = description[:800] + "..."
    text_parts.append(description)
    
    # Add key structured data
    if product.structured_data:
        structured = product.structured_data
        
        # Add most important fields for semantic search
        important_fields = ['features', 'benefits', 'medical_claims', 'ingredients']
        for field in important_fields:
            value = structured.get(field)
            if value and isinstance(value, str) and len(value) > 10:
                # Truncate to control token usage
                if len(value) > 200:
                    value = value[:200] + "..."
                text_parts.append(f"{field}: {value}")
    
    # Add eligibility rationale for searchability
    if product.eligibility_rationale:
        rationale = product.eligibility_rationale
        if len(rationale) > 200:
            rationale = rationale[:200] + "..."
        text_parts.append(f"Classification: {rationale}")
    
    # Combine and return
    embedding_text = " | ".join(text_parts)
    
    # Ensure reasonable length for embedding API
    if len(embedding_text) > 2000:
        embedding_text = embedding_text[:2000] + "..."
    
    return embedding_text

# Alternative function for batch processing
@app.function(
    image=image,
    
    secrets=secrets,
    timeout=3600  # 1 hour for large batch
)
def upload_products_batch(classified_products: list, namespace: str = None) -> list:
    """
    Batch upload multiple products at once
    Alternative to worker-based processing for smaller batches
    """
    # Initialize services
    api_key = os.environ.get('TURBOPUFFER_API_KEY')
    openai_key = os.environ.get('OPENAI_API_KEY')
    
    if not api_key or not tpuf:
        print("❌ Turbopuffer not available")
        return []
    
    if not openai_key or not openai:
        print("❌ OpenAI not available for embeddings")
        return []
    
    openai.api_key = openai_key
    
    if not namespace:
        namespace = os.environ.get('TURBOPUFFER_NAMESPACE', 'ecommerce-products')
    
    print(f"🚀 Batch uploading {len(classified_products)} products to namespace: {namespace}")
    
    # Process in batches
    all_uploaded_products = []
    
    for i in range(0, len(classified_products), BATCH_SIZE):
        batch = classified_products[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(classified_products) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} products)")
        
        uploaded_products = _upload_batch_to_turbopuffer(batch, namespace)
        all_uploaded_products.extend(uploaded_products)
        
        # Small delay between batches
        time.sleep(1)
    
    successful_uploads = sum(1 for p in all_uploaded_products if p and p.upload_success)
    print(f"✅ Batch upload complete: {successful_uploads}/{len(classified_products)} successful")
    
    return all_uploaded_products

# Utility function for querying Turbopuffer
@app.function(
    image=image,
    secrets=secrets,
    timeout=60
)
def search_turbopuffer(query: str, namespace: str = None, limit: int = 10) -> list:
    """
    Search Turbopuffer for similar products
    Useful for testing and validation
    """
    # Initialize services
    api_key = os.environ.get('TURBOPUFFER_API_KEY')
    openai_key = os.environ.get('OPENAI_API_KEY')
    
    if not api_key or not tpuf:
        print("❌ Turbopuffer not available")
        return []
    
    if not openai_key or not openai:
        print("❌ OpenAI not available for query embedding")
        return []
    
    openai.api_key = openai_key
    
    if not namespace:
        namespace = os.environ.get('TURBOPUFFER_NAMESPACE', 'ecommerce-products')
    
    try:
        # Generate embedding for query
        response = openai.embeddings.create(
            model=EMBEDDING_MODEL,
            input=[query]
        )
        query_embedding = response.data[0].embedding
        
        # Search Turbopuffer
        client = tpuf.Turbopuffer()
        results = client.query(
            namespace=namespace,
            vector=query_embedding,
            top_k=limit
        )
        
        # Format results
        formatted_results = []
        for result in results:
            formatted_results.append({
                'id': result.id,
                'score': result.score,
                'attributes': result.attributes
            })
        
        print(f"🔍 Found {len(formatted_results)} results for query: {query}")
        return formatted_results
        
    except Exception as e:
        print(f"❌ Search error: {str(e)}")
        return []