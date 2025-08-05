#!/usr/bin/env python3
"""
Results Collector for Modal Pipeline
Collects final results from Turbopuffer uploads and generates reports
"""

import time
import csv
import json
from typing import List, Dict, Any
from collections import defaultdict, Counter

from .config import app, image, secrets, turbopuffer_queue
from .schemas import TurbopufferProduct

@app.function(
    image=image,
    
    secrets=secrets,
    timeout=3600,  # 1 hour for result collection
    concurrency_limit=1  # 1 collector per batch
)
def results_collector_worker(batch_id: str, expected_products: int = None):
    """
    Results Collector Worker
    Continuously collects results from turbopuffer_queue and generates final reports
    
    Args:
        batch_id: The batch ID to collect results for
        expected_products: Expected number of products (optional, for progress tracking)
    """
    print(f"📊 Results Collector started for batch: {batch_id}")
    if expected_products:
        print(f"   Expected products: {expected_products}")
    
    collected_products = []
    start_time = time.time()
    last_product_time = time.time()
    
    # Collection timeout (stop if no products received for 10 minutes)
    COLLECTION_TIMEOUT = 600  # 10 minutes
    
    while True:
        try:
            # Get result from queue with timeout
            try:
                turbopuffer_product: TurbopufferProduct = turbopuffer_queue.get(timeout=60)
            except:
                # Check if we've been waiting too long without new products
                if time.time() - last_product_time > COLLECTION_TIMEOUT:
                    print(f"⏰ Collection timeout reached - finalizing results")
                    break
                continue
            
            if turbopuffer_product is None:  # Poison pill to stop collector
                print("💊 Received stop signal - finalizing results")
                break
            
            # Only collect products from our batch
            if turbopuffer_product.batch_id != batch_id:
                continue
            
            print(f"📥 Collected: {turbopuffer_product.name} (upload: {'✅' if turbopuffer_product.upload_success else '❌'})")
            collected_products.append(turbopuffer_product)
            last_product_time = time.time()
            
            # Progress update
            if len(collected_products) % 100 == 0:
                print(f"   📊 Progress: {len(collected_products)} products collected")
            
            # Check if we've collected expected number of products
            if expected_products and len(collected_products) >= expected_products:
                print(f"✅ Collected expected number of products ({expected_products})")
                break
                
        except Exception as e:
            print(f"❌ Error collecting results: {str(e)}")
            continue
    
    # Generate final results
    collection_time = time.time() - start_time
    
    print(f"\n📊 GENERATING FINAL RESULTS...")
    print(f"   Total products collected: {len(collected_products)}")
    print(f"   Collection time: {collection_time:.1f} seconds")
    
    # Generate comprehensive report
    report = _generate_comprehensive_report(collected_products, batch_id, collection_time)
    
    # Save results in multiple formats
    output_files = _save_results(collected_products, batch_id, report)
    
    print(f"✅ RESULTS COLLECTION COMPLETE!")
    print(f"📁 Output files: {', '.join(output_files)}")
    
    return {
        'batch_id': batch_id,
        'total_collected': len(collected_products),
        'collection_time': collection_time,
        'output_files': output_files,
        'report': report
    }

def _generate_comprehensive_report(products: List[TurbopufferProduct], batch_id: str, collection_time: float) -> Dict[str, Any]:
    """Generate comprehensive processing report"""
    
    if not products:
        return {
            'batch_id': batch_id,
            'total_products': 0,
            'collection_time': collection_time,
            'error': 'No products collected'
        }
    
    # Basic statistics
    total_products = len(products)
    successful_uploads = sum(1 for p in products if p.upload_success)
    failed_uploads = total_products - successful_uploads
    
    # Processing time statistics
    extraction_times = [p.extraction_time for p in products if p.extraction_time]
    classification_times = [p.classification_time for p in products if p.classification_time]
    total_processing_times = [p.total_processing_time for p in products if p.total_processing_time]
    
    # Category distribution
    category_distribution = Counter(p.primary_category for p in products)
    eligibility_distribution = Counter(p.eligibility_status for p in products)
    likelihood_distribution = Counter(p.hsa_fsa_likelihood for p in products)
    
    # Upload statistics
    upload_success_rate = (successful_uploads / total_products * 100) if total_products > 0 else 0
    
    # Processing efficiency
    avg_extraction_time = sum(extraction_times) / len(extraction_times) if extraction_times else 0
    avg_classification_time = sum(classification_times) / len(classification_times) if classification_times else 0
    avg_total_time = sum(total_processing_times) / len(total_processing_times) if total_processing_times else 0
    
    # HSA/FSA eligibility analysis
    eligible_products = sum(1 for p in products if p.eligibility_status in ['eligible', 'prescription_required'])
    not_eligible_products = sum(1 for p in products if p.eligibility_status == 'not_eligible')
    unclear_products = sum(1 for p in products if p.eligibility_status in ['unclear', 'unknown'])
    
    eligibility_rate = (eligible_products / total_products * 100) if total_products > 0 else 0
    
    report = {
        'batch_id': batch_id,
        'collection_timestamp': time.time(),
        'collection_time': collection_time,
        
        # Volume Statistics
        'volume_stats': {
            'total_products': total_products,
            'successful_uploads': successful_uploads,
            'failed_uploads': failed_uploads,
            'upload_success_rate': round(upload_success_rate, 2)
        },
        
        # Processing Performance
        'performance_stats': {
            'avg_extraction_time': round(avg_extraction_time, 2),
            'avg_classification_time': round(avg_classification_time, 2),
            'avg_total_processing_time': round(avg_total_time, 2),
            'total_extraction_time': round(sum(extraction_times), 2),
            'total_classification_time': round(sum(classification_times), 2),
            'products_per_hour': round(total_products / (collection_time / 3600), 2) if collection_time > 0 else 0
        },
        
        # Category Analysis
        'category_distribution': dict(category_distribution),
        'top_categories': category_distribution.most_common(10),
        
        # HSA/FSA Eligibility Analysis
        'eligibility_analysis': {
            'eligible_products': eligible_products,
            'not_eligible_products': not_eligible_products,
            'unclear_products': unclear_products,
            'eligibility_rate': round(eligibility_rate, 2),
            'eligibility_distribution': dict(eligibility_distribution),
            'likelihood_distribution': dict(likelihood_distribution)
        },
        
        # Quality Metrics
        'quality_metrics': {
            'products_with_descriptions': sum(1 for p in products if p.description and len(p.description) > 100),
            'avg_description_length': round(sum(len(p.description or '') for p in products) / total_products, 2),
            'products_with_structured_data': sum(1 for p in products if p.structured_data),
            'turbopuffer_upload_success_rate': round(upload_success_rate, 2)
        }
    }
    
    return report

def _save_results(products: List[TurbopufferProduct], batch_id: str, report: Dict[str, Any]) -> List[str]:
    """Save results in multiple formats"""
    
    output_files = []
    timestamp = int(time.time())
    
    # 1. Save detailed CSV with all product data
    csv_filename = f"results_{batch_id}_{timestamp}.csv"
    
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'url', 'name', 'primary_category', 'secondary_category',
                'eligibility_status', 'eligibility_rationale', 'hsa_fsa_likelihood',
                'extraction_time', 'classification_time', 'total_processing_time',
                'turbopuffer_id', 'upload_success', 'upload_timestamp',
                'description_preview', 'batch_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, product in enumerate(products, 1):
                writer.writerow({
                    'url': product.url,
                    'name': product.name,
                    'primary_category': product.primary_category,
                    'secondary_category': product.secondary_category,
                    'eligibility_status': product.eligibility_status,
                    'eligibility_rationale': product.eligibility_rationale[:500] if product.eligibility_rationale else '',
                    'hsa_fsa_likelihood': product.hsa_fsa_likelihood,
                    'extraction_time': round(product.extraction_time, 2),
                    'classification_time': round(product.classification_time, 2),
                    'total_processing_time': round(product.total_processing_time, 2),
                    'turbopuffer_id': product.turbopuffer_id,
                    'upload_success': product.upload_success,
                    'upload_timestamp': product.upload_timestamp,
                    'description_preview': product.description[:200] if product.description else '',
                    'batch_id': product.batch_id
                })
        
        output_files.append(csv_filename)
        print(f"   ✅ Saved detailed CSV: {csv_filename}")
        
    except Exception as e:
        print(f"   ❌ Error saving CSV: {str(e)}")
    
    # 2. Save HSA/FSA eligible products only
    eligible_csv = f"eligible_products_{batch_id}_{timestamp}.csv"
    eligible_products = [p for p in products if p.eligibility_status in ['eligible', 'prescription_required']]
    
    if eligible_products:
        try:
            with open(eligible_csv, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'url', 'category', 'eligibility_status', 'rationale']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for product in eligible_products:
                    writer.writerow({
                        'name': product.name,
                        'url': product.url,
                        'category': product.primary_category,
                        'eligibility_status': product.eligibility_status,
                        'rationale': product.eligibility_rationale[:300] if product.eligibility_rationale else ''
                    })
            
            output_files.append(eligible_csv)
            print(f"   ✅ Saved eligible products CSV: {eligible_csv} ({len(eligible_products)} products)")
            
        except Exception as e:
            print(f"   ❌ Error saving eligible products CSV: {str(e)}")
    
    # 3. Save processing report as JSON
    report_filename = f"report_{batch_id}_{timestamp}.json"
    
    try:
        with open(report_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(report, jsonfile, indent=2, default=str)
        
        output_files.append(report_filename)
        print(f"   ✅ Saved processing report: {report_filename}")
        
    except Exception as e:
        print(f"   ❌ Error saving report: {str(e)}")
    
    # 4. Print summary to console
    _print_summary_report(report)
    
    return output_files

def _print_summary_report(report: Dict[str, Any]):
    """Print summary report to console"""
    
    print(f"\n{'=' * 60}")
    print(f"📊 PROCESSING SUMMARY - BATCH {report['batch_id']}")
    print(f"{'=' * 60}")
    
    volume = report['volume_stats']
    performance = report['performance_stats']
    eligibility = report['eligibility_analysis']
    quality = report['quality_metrics']
    
    print(f"📈 VOLUME STATISTICS:")
    print(f"   Total Products: {volume['total_products']:,}")
    print(f"   Successful Uploads: {volume['successful_uploads']:,}")
    print(f"   Upload Success Rate: {volume['upload_success_rate']}%")
    
    print(f"\n⚡ PERFORMANCE STATISTICS:")
    print(f"   Avg Extraction Time: {performance['avg_extraction_time']:.2f}s")
    print(f"   Avg Classification Time: {performance['avg_classification_time']:.2f}s")
    print(f"   Avg Total Processing Time: {performance['avg_total_processing_time']:.2f}s")
    print(f"   Processing Rate: {performance['products_per_hour']:.1f} products/hour")
    
    print(f"\n🏥 HSA/FSA ELIGIBILITY ANALYSIS:")
    print(f"   Eligible Products: {eligibility['eligible_products']:,} ({eligibility['eligibility_rate']:.1f}%)")
    print(f"   Not Eligible: {eligibility['not_eligible_products']:,}")
    print(f"   Unclear/Unknown: {eligibility['unclear_products']:,}")
    
    print(f"\n📊 TOP CATEGORIES:")
    top_categories = report.get('top_categories', [])
    for category, count in top_categories[:5]:
        print(f"   {category}: {count:,} products")
    
    print(f"\n✅ QUALITY METRICS:")
    print(f"   Products with Good Descriptions: {quality['products_with_descriptions']:,}")
    print(f"   Avg Description Length: {quality['avg_description_length']:.0f} chars")
    print(f"   Products with Structured Data: {quality['products_with_structured_data']:,}")
    
    print(f"\n{'=' * 60}")

# Alternative function for batch processing
@app.function(
    image=image,
    
    secrets=secrets,
    timeout=1800  # 30 minutes for batch collection
)
def collect_results_batch(products: List[TurbopufferProduct], batch_id: str) -> dict:
    """
    Batch collect results at once
    Alternative to worker-based processing for smaller batches
    """
    print(f"📊 Batch collecting results for {len(products)} products (batch: {batch_id})")
    
    start_time = time.time()
    
    # Generate report
    report = _generate_comprehensive_report(products, batch_id, 0)
    
    # Save results
    output_files = _save_results(products, batch_id, report)
    
    collection_time = time.time() - start_time
    
    print(f"✅ Batch collection complete in {collection_time:.1f}s")
    
    return {
        'batch_id': batch_id,
        'total_collected': len(products),
        'collection_time': collection_time,
        'output_files': output_files,
        'report': report
    }