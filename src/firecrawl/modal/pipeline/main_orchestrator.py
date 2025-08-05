#!/usr/bin/env python3
"""
Main Pipeline Orchestrator for Modal S3-based Product Processing
Coordinates all 5 stages: Discovery → Extraction → Categorization → Classification → Turbopuffer
"""

import time
import uuid
from typing import Dict, Any, List

from .config import app, image, secrets
from .url_discovery import stage1_discovery_orchestrator
from .extraction_dispatcher import stage2_extraction_dispatcher
from .categorization_dispatcher import stage3_categorization_dispatcher
from .classification_dispatcher import stage4_classification_dispatcher
from .turbopuffer_dispatcher import stage5_turbopuffer_dispatcher

@app.function(
    image=image,
    secrets=secrets,
    timeout=14400  # 4 hours for full pipeline
)
def run_complete_pipeline(
    base_urls: List[str],
    environment: str = "dev",
    max_products: int = None,
    turbopuffer_namespace: str = None
) -> Dict[str, Any]:
    """
    Run the complete 5-stage S3-based product processing pipeline
    
    Args:
        base_urls: List of e-commerce site URLs to discover products from
        environment: dev or prod environment
        max_products: Optional limit on number of products to process
        turbopuffer_namespace: Optional Turbopuffer namespace override
        
    Returns:
        Complete pipeline results with stage-by-stage statistics
    """
    
    pipeline_start = time.time()
    execution_id = str(uuid.uuid4())[:8]
    
    print(f"🚀 STARTING COMPLETE PIPELINE")
    print(f"📋 Execution ID: {execution_id}")
    print(f"🌍 Environment: {environment}")
    print(f"🔗 Base URLs: {len(base_urls)} sites")
    if max_products:
        print(f"📊 Max products: {max_products:,}")
    
    results = {
        'execution_id': execution_id,
        'environment': environment,
        'base_urls': base_urls,
        'max_products': max_products,
        'pipeline_start': pipeline_start,
        'stages': {}
    }
    
    try:
        # STAGE 1: URL Discovery
        print(f"\n" + "="*60)
        print(f"🔍 STAGE 1: URL DISCOVERY")
        print(f"="*60)
        
        # Need to create DiscoveryJob object for stage1
        from .schemas import DiscoveryJob
        discovery_job = DiscoveryJob(
            base_urls=base_urls,  
            execution_id=execution_id,
            environment=environment
        )
        
        stage1_result = stage1_discovery_orchestrator.remote(discovery_job)
        
        results['stages']['discovery'] = stage1_result
        
        if stage1_result['status'] != 'completed':
            raise Exception(f"Stage 1 failed: {stage1_result.get('error', 'Unknown error')}")
        
        discovered_urls = stage1_result['discovered_urls']
        print(f"✅ Stage 1 complete: {discovered_urls:,} URLs discovered")
        
        # STAGE 2: Product Extraction
        print(f"\n" + "="*60)
        print(f"🤖 STAGE 2: PRODUCT EXTRACTION")
        print(f"="*60)
        
        stage2_result = stage2_extraction_dispatcher.remote(
            execution_id=execution_id,
            environment=environment,
            max_products=max_products
        )
        
        results['stages']['extraction'] = stage2_result
        
        if stage2_result['status'] != 'completed':
            raise Exception(f"Stage 2 failed: {stage2_result.get('error', 'Unknown error')}")
        
        extracted_products = stage2_result['extracted_products']
        print(f"✅ Stage 2 complete: {extracted_products:,} products extracted")
        
        # STAGE 3: Product Categorization
        print(f"\n" + "="*60)
        print(f"🏷️  STAGE 3: PRODUCT CATEGORIZATION")
        print(f"="*60)
        
        stage3_result = stage3_categorization_dispatcher.remote(
            execution_id=execution_id,
            environment=environment
        )
        
        results['stages']['categorization'] = stage3_result
        
        if stage3_result['status'] != 'completed':
            raise Exception(f"Stage 3 failed: {stage3_result.get('error', 'Unknown error')}")
        
        categorized_products = stage3_result['categorized_products']
        print(f"✅ Stage 3 complete: {categorized_products:,} products categorized")
        
        # STAGE 4: HSA/FSA Classification
        print(f"\n" + "="*60)
        print(f"🏥 STAGE 4: HSA/FSA CLASSIFICATION")
        print(f"="*60)
        
        stage4_result = stage4_classification_dispatcher.remote(
            execution_id=execution_id,
            environment=environment
        )
        
        results['stages']['classification'] = stage4_result
        
        if stage4_result['status'] != 'completed':
            raise Exception(f"Stage 4 failed: {stage4_result.get('error', 'Unknown error')}")
        
        classified_products = stage4_result['classified_products']
        eligibility_dist = stage4_result['eligibility_distribution']
        
        print(f"✅ Stage 4 complete: {classified_products:,} products classified")
        print(f"🏥 HSA/FSA distribution:")
        for status, count in eligibility_dist.items():
            print(f"   {status}: {count} products")
        
        # STAGE 5: Turbopuffer Upload
        print(f"\n" + "="*60)
        print(f"🗄️  STAGE 5: TURBOPUFFER UPLOAD")
        print(f"="*60)
        
        stage5_result = stage5_turbopuffer_dispatcher.remote(
            execution_id=execution_id,
            environment=environment,
            turbopuffer_namespace=turbopuffer_namespace
        )
        
        results['stages']['turbopuffer'] = stage5_result
        
        if stage5_result['status'] != 'completed':
            raise Exception(f"Stage 5 failed: {stage5_result.get('error', 'Unknown error')}")
        
        uploaded_products = stage5_result['successful_uploads']
        failed_uploads = stage5_result['failed_uploads']
        namespace = stage5_result['turbopuffer_namespace']
        
        print(f"✅ Stage 5 complete: {uploaded_products:,} successful, {failed_uploads:,} failed uploads")
        print(f"🗄️  Turbopuffer namespace: {namespace}")
        
        # Calculate final results
        pipeline_time = time.time() - pipeline_start
        
        print(f"\n" + "="*60)
        print(f"🎉 PIPELINE COMPLETE!")
        print(f"="*60)
        print(f"📋 Execution ID: {execution_id}")
        print(f"🔗 Sites processed: {len(base_urls)}")
        print(f"🔍 URLs discovered: {discovered_urls:,}")
        print(f"🤖 Products extracted: {extracted_products:,}")
        print(f"🏷️  Products categorized: {categorized_products:,}")
        print(f"🏥 Products classified: {classified_products:,}")
        print(f"🗄️  Products uploaded: {uploaded_products:,}")
        print(f"⏱️  Total pipeline time: {pipeline_time/60:.1f} minutes")
        
        results.update({
            'status': 'completed',
            'pipeline_time': pipeline_time,
            'final_stats': {
                'sites_processed': len(base_urls),
                'urls_discovered': discovered_urls,
                'products_extracted': extracted_products,
                'products_categorized': categorized_products,
                'products_classified': classified_products,
                'products_uploaded': uploaded_products,
                'failed_uploads': failed_uploads,
                'turbopuffer_namespace': namespace
            }
        })
        
        return results
        
    except Exception as e:
        pipeline_time = time.time() - pipeline_start
        error_msg = str(e)
        
        print(f"\n❌ PIPELINE FAILED: {error_msg}")
        print(f"⏱️  Failed after: {pipeline_time/60:.1f} minutes")
        
        results.update({
            'status': 'failed',
            'error': error_msg,
            'pipeline_time': pipeline_time
        })
        
        return results

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=600  # 10 minutes for quick test
)
def run_pipeline_test(
    base_urls: List[str],
    max_products: int = 10,
    environment: str = "dev"
) -> Dict[str, Any]:
    """
    Run a quick test of the pipeline with limited products
    
    Args:
        base_urls: List of e-commerce site URLs
        max_products: Number of products to limit test to (default 10)
        environment: dev or prod environment
        
    Returns:
        Test results
    """
    
    print(f"🧪 RUNNING PIPELINE TEST")
    print(f"🔗 Sites: {len(base_urls)}")
    print(f"📊 Max products: {max_products}")
    
    return run_complete_pipeline.remote(
        base_urls=base_urls,
        environment=environment,
        max_products=max_products,
        turbopuffer_namespace=f"test-products-{environment}"
    )

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=60
)
def get_pipeline_status(execution_id: str, environment: str = "dev") -> Dict[str, Any]:
    """
    Get the current status of a pipeline execution by checking S3 outputs
    
    Args:
        execution_id: Pipeline execution ID
        environment: dev or prod environment
        
    Returns:
        Pipeline status information
    """
    
    from .s3_utils import S3Manager
    
    s3_manager = S3Manager()
    status = {
        'execution_id': execution_id,
        'environment': environment,
        'stages': {}
    }
    
    # Check each stage's output
    stages = [
        ('discovery', 'discovered_urls.csv'),
        ('extraction', 'extracted_products.csv'),
        ('categorization', 'categorized_products.csv'),
        ('classification', 'classified_products.csv'),
        ('turbopuffer', 'uploaded_products.csv')
    ]
    
    for stage_name, filename in stages:
        try:
            s3_path = s3_manager.build_s3_path(environment, execution_id, stage_name, filename)
            df = s3_manager.download_dataframe(s3_path)
            
            status['stages'][stage_name] = {
                'status': 'completed',
                'record_count': len(df),
                's3_path': s3_path
            }
            
        except Exception as e:
            status['stages'][stage_name] = {
                'status': 'not_found',
                'error': str(e)
            }
    
    return status

# Convenience function for common use cases
@app.function(
    image=image,
     
    secrets=secrets,
    timeout=14400
)
def run_single_site_pipeline(
    site_url: str,
    environment: str = "dev",
    max_products: int = None
) -> Dict[str, Any]:
    """
    Run the complete pipeline on a single e-commerce site
    
    Args:
        site_url: Single e-commerce site URL
        environment: dev or prod environment
        max_products: Optional limit on products
        
    Returns:
        Pipeline results
    """
    
    return run_complete_pipeline.remote(
        base_urls=[site_url],
        environment=environment,
        max_products=max_products
    )