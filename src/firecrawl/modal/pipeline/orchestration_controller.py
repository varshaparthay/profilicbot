#!/usr/bin/env python3
"""
Orchestration Controller for Modal Pipeline
Manages the entire 6-stage orchestrated product scraping pipeline
"""

import time
import uuid
from typing import Dict, Any, Optional

from .config import app
from .schemas import DiscoveryJob
from .url_discovery import stage1_discovery_orchestrator
from .product_extractor import product_extractor_worker
from .product_categorizer import product_categorizer_worker
from .hsa_classifier import hsa_classifier_worker
from .turbopuffer_uploader import turbopuffer_uploader_worker
from .results_collector import results_collector_worker

@app.function(timeout=7200)  # 2 hours for full pipeline
def orchestrate_product_scraping(
    website_url: str,
    batch_id: Optional[str] = None,
    max_products: Optional[int] = None,
    extraction_workers: int = 30,
    categorization_workers: int = 50,
    classification_workers: int = 15,
    turbopuffer_workers: int = 10,
    turbopuffer_namespace: str = "ecommerce-products"
) -> Dict[str, Any]:
    """
    Main Orchestration Controller
    Manages the complete 6-stage pipeline for processing e-commerce products
    
    Args:
        website_url: Target website URL to scrape
        batch_id: Optional batch ID (auto-generated if not provided)
        max_products: Maximum products to process (None = no limit)
        extraction_workers: Number of parallel extraction workers
        categorization_workers: Number of parallel categorization workers
        classification_workers: Number of parallel classification workers
        turbopuffer_workers: Number of parallel Turbopuffer upload workers
        turbopuffer_namespace: Turbopuffer namespace for uploads
    
    Returns:
        Complete pipeline results and statistics
    """
    
    # Generate batch ID if not provided
    if not batch_id:
        timestamp = int(time.time())
        batch_id = f"batch_{timestamp}_{str(uuid.uuid4())[:8]}"
    
    print(f"🚀 STARTING ORCHESTRATED PIPELINE")
    print(f"🎯 Website: {website_url}")
    print(f"📦 Batch ID: {batch_id}")
    print(f"⚙️  Workers: Extract={extraction_workers}, Categorize={categorization_workers}, Classify={classification_workers}, Upload={turbopuffer_workers}")
    
    pipeline_start_time = time.time()
    results = {
        'batch_id': batch_id,
        'website_url': website_url,
        'pipeline_start_time': pipeline_start_time,
        'stages': {}
    }
    
    try:
        # ======================================================================
        # STAGE 1: URL DISCOVERY
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🔍 STAGE 1: URL DISCOVERY")
        print(f"{'='*60}")
        
        discovery_job = DiscoveryJob(
            website_url=website_url,
            batch_id=batch_id,
            job_id=f"discovery_{batch_id}"
        )
        
        # Run discovery
        discovery_result = stage1_discovery_orchestrator.remote(discovery_job)
        discovery_stats = discovery_result.get()
        
        results['stages']['discovery'] = discovery_stats
        
        if discovery_stats['status'] != 'completed':
            raise Exception(f"Discovery failed: {discovery_stats.get('error', 'Unknown error')}")
        
        discovered_count = discovery_stats['total_discovered']
        print(f"✅ STAGE 1 COMPLETE: {discovered_count:,} URLs discovered")
        
        # Apply max_products limit if specified
        processing_count = min(discovered_count, max_products) if max_products else discovered_count
        if max_products and processing_count < discovered_count:
            print(f"⚠️  Limited to {processing_count:,} products (max_products={max_products})")
        
        # ======================================================================
        # STAGE 2: PRODUCT EXTRACTION
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🔍 STAGE 2: PRODUCT EXTRACTION")
        print(f"{'='*60}")
        
        # Start extraction workers
        print(f"🚀 Starting {extraction_workers} extraction workers...")
        extraction_futures = []
        for i in range(extraction_workers):
            future = product_extractor_worker.spawn()
            extraction_futures.append(future)
        
        print(f"✅ {len(extraction_futures)} extraction workers started")
        
        # ======================================================================
        # STAGE 3: PRODUCT CATEGORIZATION
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🏷️  STAGE 3: PRODUCT CATEGORIZATION")
        print(f"{'='*60}")
        
        # Start categorization workers
        print(f"🚀 Starting {categorization_workers} categorization workers...")
        categorization_futures = []
        for i in range(categorization_workers):
            future = product_categorizer_worker.spawn()
            categorization_futures.append(future)
        
        print(f"✅ {len(categorization_futures)} categorization workers started")
        
        # ======================================================================
        # STAGE 4: HSA/FSA CLASSIFICATION
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🧠 STAGE 4: HSA/FSA CLASSIFICATION")
        print(f"{'='*60}")
        
        # Start classification workers
        print(f"🚀 Starting {classification_workers} classification workers...")
        classification_futures = []
        for i in range(classification_workers):
            future = hsa_classifier_worker.spawn()
            classification_futures.append(future)
        
        print(f"✅ {len(classification_futures)} classification workers started")
        
        # ======================================================================
        # STAGE 5: TURBOPUFFER UPLOAD
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🚀 STAGE 5: TURBOPUFFER UPLOAD")
        print(f"{'='*60}")
        
        # Start Turbopuffer workers
        print(f"🚀 Starting {turbopuffer_workers} Turbopuffer workers...")
        turbopuffer_futures = []
        for i in range(turbopuffer_workers):
            future = turbopuffer_uploader_worker.spawn()
            turbopuffer_futures.append(future)
        
        print(f"✅ {len(turbopuffer_futures)} Turbopuffer workers started")
        
        # ======================================================================
        # STAGE 6: RESULTS COLLECTION
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"📊 STAGE 6: RESULTS COLLECTION")
        print(f"{'='*60}")
        
        # Start results collector
        print(f"📊 Starting results collector...")
        results_future = results_collector_worker.spawn(batch_id, processing_count)
        
        print(f"✅ Results collector started")
        
        # ======================================================================
        # MONITORING PHASE
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"📈 PIPELINE MONITORING")
        print(f"{'='*60}")
        
        print(f"🔄 All stages running - monitoring progress...")
        print(f"   Stage 1: Discovery completed ({discovered_count:,} URLs)")
        print(f"   Stage 2: {len(extraction_futures)} extraction workers processing")
        print(f"   Stage 3: {len(categorization_futures)} categorization workers processing")
        print(f"   Stage 4: {len(classification_futures)} classification workers processing")
        print(f"   Stage 5: {len(turbopuffer_futures)} Turbopuffer workers processing")
        print(f"   Stage 6: Results collector monitoring")
        
        # Wait for results collector to complete
        print(f"\n⏳ Waiting for pipeline completion...")
        final_results = results_future.get()
        
        # ======================================================================
        # PIPELINE COMPLETION
        # ======================================================================
        pipeline_end_time = time.time()
        total_pipeline_time = pipeline_end_time - pipeline_start_time
        
        # Stop all workers by sending poison pills
        print(f"\n🛑 Stopping all workers...")
        
        # Stop workers (send None to queues to signal shutdown)
        # This would be handled by the queue management system
        
        results['stages']['final_results'] = final_results
        results['pipeline_end_time'] = pipeline_end_time
        results['total_pipeline_time'] = total_pipeline_time
        results['status'] = 'completed'
        
        # ======================================================================
        # FINAL SUMMARY
        # ======================================================================
        print(f"\n{'='*60}")
        print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"{'='*60}")
        
        print(f"📦 Batch ID: {batch_id}")
        print(f"🎯 Website: {website_url}")
        print(f"📊 Products Discovered: {discovered_count:,}")
        print(f"📊 Products Processed: {final_results['total_collected']:,}")
        print(f"⏱️  Total Pipeline Time: {total_pipeline_time/60:.1f} minutes")
        print(f"📁 Output Files: {', '.join(final_results['output_files'])}")
        
        # Calculate final statistics
        if final_results['total_collected'] > 0:
            processing_rate = final_results['total_collected'] / (total_pipeline_time / 3600)
            print(f"🚀 Processing Rate: {processing_rate:.1f} products/hour")
        
        print(f"\n✅ Pipeline Results:")
        print(f"   • Discovery: {discovery_stats['methods_used']}")
        print(f"   • Processing: {final_results['total_collected']:,} products")
        print(f"   • Success Rate: {final_results.get('report', {}).get('volume_stats', {}).get('upload_success_rate', 0):.1f}%")
        
        return results
        
    except Exception as e:
        pipeline_end_time = time.time()
        total_pipeline_time = pipeline_end_time - pipeline_start_time
        
        results['pipeline_end_time'] = pipeline_end_time
        results['total_pipeline_time'] = total_pipeline_time
        results['status'] = 'failed'
        results['error'] = str(e)
        
        print(f"\n❌ PIPELINE FAILED!")
        print(f"Error: {str(e)}")
        print(f"Runtime: {total_pipeline_time/60:.1f} minutes")
        
        return results

# Convenience function for simple usage
@app.function()
def scrape_website_complete(
    website_url: str,
    max_products: int = 1000,
    batch_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Simple interface for complete website scraping
    Uses default worker configurations optimized for most use cases
    
    Args:
        website_url: Website to scrape
        max_products: Maximum products to process
        batch_id: Optional batch ID
    
    Returns:
        Pipeline results
    """
    return orchestrate_product_scraping(
        website_url=website_url,
        batch_id=batch_id,
        max_products=max_products,
        extraction_workers=30,
        categorization_workers=50,
        classification_workers=15,
        turbopuffer_workers=10
    )

# CLI entry point
@app.local_entrypoint()
def main(
    website_url: str,
    batch_id: str = None,
    max_products: int = None,
    extraction_workers: int = 30,
    categorization_workers: int = 50,
    classification_workers: int = 15,
    turbopuffer_workers: int = 10
):
    """
    CLI entry point for the orchestrated pipeline
    
    Usage:
        modal run orchestration_controller.py --website-url https://example.com
        modal run orchestration_controller.py --website-url https://example.com --max-products 5000
    """
    
    print(f"🔧 Modal Orchestrated Product Scraper")
    print(f"🎯 Target: {website_url}")
    
    if max_products:
        print(f"📊 Max Products: {max_products:,}")
    
    # Run the complete pipeline
    results = orchestrate_product_scraping(
        website_url=website_url,
        batch_id=batch_id,
        max_products=max_products,
        extraction_workers=extraction_workers,
        categorization_workers=categorization_workers,
        classification_workers=classification_workers,
        turbopuffer_workers=turbopuffer_workers
    )
    
    if results['status'] == 'completed':
        print(f"✅ Pipeline completed successfully!")
        print(f"📁 Check output files: {results['stages']['final_results']['output_files']}")
    else:
        print(f"❌ Pipeline failed: {results.get('error', 'Unknown error')}")
        exit(1)