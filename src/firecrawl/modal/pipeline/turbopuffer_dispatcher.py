#!/usr/bin/env python3
"""
Stage 5: Turbopuffer Upload Dispatcher for Modal Pipeline
Reads classified CSV from S3, uploads to Turbopuffer with embeddings
"""

import time
import pandas as pd
from typing import List, Dict, Any

from .config import app, image, secrets, turbopuffer_queue
from .s3_utils import S3Manager, create_dynamic_batches, combine_batch_results, BatchReference

try:
    import turbopuffer as tpuf
except ImportError:
    tpuf = None

try:
    import openai
except ImportError:
    openai = None

EMBEDDING_MODEL = "text-embedding-ada-002"
TURBOPUFFER_BATCH_SIZE = 100  # Upload in batches for efficiency

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=3600  # 1 hour for dispatching and coordination
)
def stage5_turbopuffer_dispatcher(
    execution_id: str,
    environment: str = "dev",
    turbopuffer_namespace: str = None
) -> dict:
    """
    Stage 5: Turbopuffer Upload Dispatcher
    Reads classified CSV, creates dynamic batches, manages Turbopuffer upload workers
    
    Args:
        execution_id: Unique execution ID from previous stages
        environment: dev or prod environment
        turbopuffer_namespace: Turbopuffer namespace (defaults to environment-based)
        
    Returns:
        Turbopuffer upload results and statistics
    """
    
    start_time = time.time()
    
    # Set default namespace based on environment
    if not turbopuffer_namespace:
        turbopuffer_namespace = f"ecommerce-products-{environment}"
    
    print(f"🚀 STAGE 5: Turbopuffer Upload Dispatcher")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌍 Environment: {environment}")
    print(f"🗄️  Namespace: {turbopuffer_namespace}")
    
    try:
        # Read classified CSV from S3
        s3_manager = S3Manager()
        classification_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "classification", "classified_products.csv"
        )
        
        print(f"📥 Reading classified CSV from: {classification_csv_path}")
        df = s3_manager.download_dataframe(classification_csv_path)
        
        if len(df) == 0:
            raise Exception("No products found in classification CSV")
        
        print(f"📊 Processing {len(df):,} products for Turbopuffer upload")
        
        # Show HSA/FSA distribution
        status_counts = df['hsa_fsa_status'].value_counts()
        print(f"🏥 HSA/FSA status distribution:")
        for status, count in status_counts.items():
            print(f"   {status}: {count} products")
        
        # Create dynamic batches for Turbopuffer upload
        # Turbopuffer upload takes ~30 seconds per batch (including embedding generation)
        batch_references = create_dynamic_batches(
            df=df,
            execution_id=execution_id,
            stage="turbopuffer",
            environment=environment,
            processing_time_per_item=0.3  # ~30 seconds per 100 products (batched)
        )
        
        print(f"\n🚀 Starting {len(batch_references)} Turbopuffer upload workers...")
        
        # Queue batch references for workers
        for batch_ref in batch_references:
            turbopuffer_queue.put(batch_ref)
        
        # Start Turbopuffer upload workers
        turbopuffer_futures = []
        batch_size, max_workers = _calculate_worker_count(len(batch_references))
        
        for i in range(min(max_workers, len(batch_references))):
            future = turbopuffer_worker.spawn(turbopuffer_namespace)
            turbopuffer_futures.append(future)
        
        print(f"✅ Started {len(turbopuffer_futures)} Turbopuffer upload workers")
        
        # Wait for all workers to complete
        print(f"⏳ Waiting for Turbopuffer upload workers to complete...")
        completed_workers = 0
        
        for future in turbopuffer_futures:
            try:
                result = future.get()  # This blocks until worker completes
                completed_workers += 1
                print(f"   ✅ Worker {completed_workers}/{len(turbopuffer_futures)} completed")
            except Exception as e:
                print(f"   ❌ Worker failed: {str(e)}")
        
        # Combine all batch results into final CSV
        print(f"\n📋 Combining Turbopuffer upload results...")
        
        turbopuffer_csv_path = s3_manager.build_s3_path(
            environment, execution_id, "turbopuffer", "uploaded_products.csv"
        )
        
        final_df = combine_batch_results(batch_references, turbopuffer_csv_path)
        
        dispatch_time = time.time() - start_time
        
        if len(final_df) > 0:
            # Calculate upload statistics
            successful_uploads = len(final_df[final_df['upload_success'] == True])
            failed_uploads = len(final_df[final_df['upload_success'] == False])
            
            print(f"\n✅ TURBOPUFFER DISPATCH COMPLETE!")
            print(f"📁 Results saved to: {turbopuffer_csv_path}")
            print(f"📊 Upload results: {successful_uploads:,} successful, {failed_uploads:,} failed")
            print(f"🗄️  Turbopuffer namespace: {turbopuffer_namespace}")
            print(f"⏱️  Total dispatch time: {dispatch_time/60:.1f} minutes")
            
            return {
                'execution_id': execution_id,
                'environment': environment,
                'turbopuffer_namespace': turbopuffer_namespace,
                'input_products': len(df),
                'successful_uploads': successful_uploads,
                'failed_uploads': failed_uploads,
                'turbopuffer_csv_path': turbopuffer_csv_path,
                'batch_count': len(batch_references),
                'worker_count': len(turbopuffer_futures),
                'dispatch_time': dispatch_time,
                'status': 'completed'
            }
        else:
            raise Exception("No products were successfully uploaded to Turbopuffer")
            
    except Exception as e:
        dispatch_time = time.time() - start_time
        print(f"❌ TURBOPUFFER DISPATCH FAILED: {str(e)}")
        
        return {
            'execution_id': execution_id,
            'environment': environment,
            'turbopuffer_namespace': turbopuffer_namespace,
            'input_products': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'turbopuffer_csv_path': None,
            'dispatch_time': dispatch_time,
            'status': 'failed',
            'error': str(e)
        }

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1800,  # 30 minutes per worker
    max_containers=10  # Limited concurrency for Turbopuffer uploads
)
def turbopuffer_worker(turbopuffer_namespace: str):
    """
    Turbopuffer Upload Worker - Processes one batch of classified products from S3
    """
    import os
    
    try:
        # Get batch reference from queue
        batch_ref: BatchReference = turbopuffer_queue.get()
        
        if batch_ref is None:
            return  # Poison pill to stop worker
        
        print(f"🗄️  Processing batch {batch_ref.batch_number}: {batch_ref.item_count} products")
        
        # Initialize services
        api_key = os.environ.get('TURBOPUFFER_API_KEY')
        openai_key = os.environ.get('OPENAI_API_KEY')
        
        if not api_key or not tpuf:
            raise Exception("Turbopuffer not available - cannot upload products")
        
        if not openai_key or not openai:
            raise Exception("OpenAI not available for embeddings - cannot upload products")
        
        openai.api_key = openai_key
        
        # Load products from S3
        s3_manager = S3Manager()
        input_df = s3_manager.download_dataframe(batch_ref.s3_input_path)
        
        if len(input_df) == 0:
            print(f"⚠️  Empty batch {batch_ref.batch_number}")
            return
        
        # Process batch upload
        upload_results = _upload_batch_to_turbopuffer(input_df, turbopuffer_namespace, batch_ref.batch_number)
        
        # Save results to S3
        if upload_results:
            results_df = pd.DataFrame(upload_results)
            success = s3_manager.upload_dataframe(results_df, batch_ref.s3_output_path)
            
            successful_count = sum(1 for result in upload_results if result.get('upload_success', False))
            
            if success:
                print(f"✅ Batch {batch_ref.batch_number} complete: {successful_count}/{batch_ref.item_count} successful uploads")
            else:
                print(f"❌ Failed to save batch {batch_ref.batch_number} results")
        else:
            print(f"❌ Batch {batch_ref.batch_number}: No upload results")
            
    except Exception as e:
        print(f"❌ Worker error: {str(e)}")

def _upload_batch_to_turbopuffer(df: pd.DataFrame, namespace: str, batch_number: int) -> List[Dict[str, Any]]:
    """Upload a batch of classified products to Turbopuffer with embeddings"""
    
    try:
        print(f"   🧠 Generating embeddings for {len(df)} products...")
        
        # Create embedding texts for all products
        embedding_texts = []
        for idx, row in df.iterrows():
            embedding_text = _create_embedding_text(row)
            embedding_texts.append(embedding_text)
        
        # Generate embeddings from OpenAI
        if openai:
            response = openai.embeddings.create(
                model=EMBEDDING_MODEL,
                input=embedding_texts
            )
            embeddings = [item.embedding for item in response.data]
        else:
            raise Exception("OpenAI not available for embeddings")
        
        print(f"   ✅ Generated {len(embeddings)} embeddings")
        
        # Prepare Turbopuffer records
        print(f"   📦 Preparing Turbopuffer records...")
        records = []
        upload_results = []
        
        for idx, (row_idx, row) in enumerate(df.iterrows()):
            upload_timestamp = time.time()
            
            # Create unique ID for Turbopuffer
            turbopuffer_id = f"{row['url']}_{batch_number}_{idx}"
            
            record = {
                "id": turbopuffer_id,
                "values": embeddings[idx],
                "attributes": {
                    "name": row.get('name', ''),
                    "url": row.get('url', ''),
                    "category": row.get('category', ''),
                    "hsa_fsa_status": row.get('hsa_fsa_status', ''),
                    "hsa_fsa_confidence": float(row.get('hsa_fsa_confidence', 0.0)),
                    "upload_date": upload_timestamp,
                    "description_preview": row.get('description', '')[:200],
                    "brand": row.get('brand', ''),
                    "price": row.get('price', ''),
                    "batch_number": batch_number
                }
            }
            records.append(record)
            
            # Prepare result record for CSV
            result_row = row.to_dict()
            result_row.update({
                'turbopuffer_id': turbopuffer_id,
                'turbopuffer_namespace': namespace,
                'upload_timestamp': upload_timestamp,
                'upload_success': False,  # Will be updated after upload
                'embedding_dimensions': len(embeddings[idx])
            })
            upload_results.append(result_row)
        
        # Upload to Turbopuffer
        print(f"   🚀 Uploading {len(records)} records to Turbopuffer namespace: {namespace}")
        
        if tpuf:
            # Initialize Turbopuffer client
            client = tpuf.Turbopuffer()
            
            # Upload records
            client.upsert(namespace, records)
            
            print(f"   ✅ Successfully uploaded {len(records)} records to Turbopuffer")
            
            # Mark all products as successfully uploaded
            for result in upload_results:
                result['upload_success'] = True
        else:
            raise Exception("Turbopuffer client not available")
        
        return upload_results
        
    except Exception as e:
        print(f"   ❌ Batch upload failed: {str(e)}")
        
        # Mark all products as failed uploads
        for result in upload_results:
            result['upload_success'] = False
            result['upload_error'] = str(e)
        
        return upload_results

def _create_embedding_text(row: pd.Series) -> str:
    """Create optimized text for embedding generation"""
    
    # Start with core product info
    text_parts = [
        row.get('name', 'Unknown Product'),
        f"Category: {row.get('category', 'Unknown')}",
        f"HSA/FSA Status: {row.get('hsa_fsa_status', 'Unknown')}"
    ]
    
    # Add description (truncated to control embedding size)
    description = row.get('description', '')
    if len(description) > 800:
        description = description[:800] + "..."
    text_parts.append(description)
    
    # Add structured data for better searchability
    structured_fields = ['brand', 'features', 'benefits', 'medical_claims', 'ingredients']
    for field in structured_fields:
        value = row.get(field, '')
        if value and isinstance(value, str) and len(value) > 10:
            # Truncate to control token usage
            if len(value) > 200:
                value = value[:200] + "..."
            text_parts.append(f"{field.title()}: {value}")
    
    # Add classification reasoning for searchability
    reasoning = row.get('hsa_fsa_reasoning', '')
    if reasoning:
        if len(reasoning) > 200:
            reasoning = reasoning[:200] + "..."
        text_parts.append(f"Classification: {reasoning}")
    
    # Combine and return
    embedding_text = " | ".join(text_parts)
    
    # Ensure reasonable length for embedding API
    if len(embedding_text) > 2000:
        embedding_text = embedding_text[:2000] + "..."
    
    return embedding_text

def _calculate_worker_count(batch_count: int) -> tuple:
    """Calculate optimal worker count based on batch count (limited by Turbopuffer rate limits)"""
    if batch_count <= 5:
        return 50, min(5, batch_count)
    elif batch_count <= 20:
        return 100, min(8, batch_count)
    elif batch_count <= 50:
        return 200, min(10, batch_count)
    else:
        return 500, min(10, batch_count)  # Max 10 workers for Turbopuffer

# Utility function for searching Turbopuffer (for testing/validation)
@app.function(
    image=image,
    secrets=secrets,
    timeout=60
)
def search_turbopuffer_products(
    query: str, 
    namespace: str,
    limit: int = 10,
    filter_hsa_fsa: str = None
) -> List[Dict[str, Any]]:
    """
    Search Turbopuffer for similar products
    
    Args:
        query: Search query text
        namespace: Turbopuffer namespace to search
        limit: Number of results to return
        filter_hsa_fsa: Optional HSA/FSA status filter (eligible, not_eligible, etc.)
    
    Returns:
        List of matching products with similarity scores
    """
    
    try:
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
        
        # Generate embedding for query
        response = openai.embeddings.create(
            model=EMBEDDING_MODEL,
            input=[query]
        )
        query_embedding = response.data[0].embedding
        
        # Search Turbopuffer
        client = tpuf.Turbopuffer()
        
        # Build filters if specified
        filters = {}
        if filter_hsa_fsa:
            filters['hsa_fsa_status'] = filter_hsa_fsa
        
        results = client.query(
            namespace=namespace,
            vector=query_embedding,
            top_k=limit,
            filters=filters if filters else None
        )
        
        # Format results
        formatted_results = []
        for result in results:
            formatted_results.append({
                'id': result.id,
                'similarity_score': result.score,
                'name': result.attributes.get('name', ''),
                'category': result.attributes.get('category', ''),
                'hsa_fsa_status': result.attributes.get('hsa_fsa_status', ''),
                'url': result.attributes.get('url', ''),
                'description_preview': result.attributes.get('description_preview', '')
            })
        
        print(f"🔍 Found {len(formatted_results)} results for query: {query}")
        return formatted_results
        
    except Exception as e:
        print(f"❌ Search error: {str(e)}")
        return []