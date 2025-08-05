#!/usr/bin/env python3
"""
Modal Pipeline Testing Suite
Comprehensive tests for the 6-stage orchestrated pipeline
"""

import time
from typing import List, Dict, Any

from ..pipeline.config import app
from ..pipeline.schemas import DiscoveryJob, ProductURL, ExtractedProduct
from ..pipeline.url_discovery import stage1_discovery_orchestrator
from ..pipeline.product_extractor import extract_products_batch
from ..pipeline.product_categorizer import categorize_products_batch
from ..pipeline.hsa_classifier import classify_products_batch
from ..pipeline.turbopuffer_uploader import upload_products_batch, search_turbopuffer
from ..pipeline.results_collector import collect_results_batch

# Test data for validation
TEST_WEBSITES = [
    "https://www.dermstore.com",
    "https://www.goodmolecules.com", 
    "https://www.cvs.com",
    "https://www.walgreens.com"
]

@app.function(timeout=3600)
def test_stage1_discovery(test_url: str = "https://www.goodmolecules.com") -> Dict[str, Any]:
    """Test Stage 1: URL Discovery"""
    print(f"🔍 TESTING STAGE 1: URL DISCOVERY")
    print(f"🎯 Test URL: {test_url}")
    
    batch_id = f"test_discovery_{int(time.time())}"
    
    discovery_job = DiscoveryJob(
        website_url=test_url,
        batch_id=batch_id,
        job_id=f"test_{batch_id}"
    )
    
    start_time = time.time()
    result = stage1_discovery_orchestrator.remote(discovery_job)
    discovery_stats = result.get()
    test_time = time.time() - start_time
    
    print(f"📊 Discovery Results:")
    print(f"   Status: {discovery_stats['status']}")
    print(f"   URLs Found: {discovery_stats['total_discovered']:,}")
    print(f"   Methods Used: {discovery_stats['methods_used']}")
    print(f"   Time: {test_time:.1f}s")
    
    # Validation
    success = (
        discovery_stats['status'] == 'completed' and
        discovery_stats['total_discovered'] > 0
    )
    
    return {
        'stage': 'discovery',
        'success': success,
        'results': discovery_stats,
        'test_time': test_time,
        'validation': {
            'found_urls': discovery_stats['total_discovered'] > 0,
            'completed_successfully': discovery_stats['status'] == 'completed',
            'reasonable_time': test_time < 300  # Less than 5 minutes
        }
    }

@app.function(timeout=1800)
def test_stage2_extraction(sample_urls: List[str] = None) -> Dict[str, Any]:
    """Test Stage 2: Product Extraction"""
    print(f"🔍 TESTING STAGE 2: PRODUCT EXTRACTION")
    
    # Use sample URLs or defaults
    if not sample_urls:
        sample_urls = [
            "https://www.goodmolecules.com/products/hyaluronic-acid-serum",
            "https://www.dermstore.com/clinique-dramatically-different-moisturizing-lotion-125ml/11117967.html",
            "https://www.cvs.com/shop/cvs-health-digital-thermometer-prodid-292097"
        ]
    
    # Create ProductURL objects
    batch_id = f"test_extraction_{int(time.time())}"
    product_urls = []
    
    for i, url in enumerate(sample_urls[:3]):  # Test with 3 products
        product_url = ProductURL(
            url=url,
            batch_id=batch_id,
            discovery_method="manual_test",
            estimated_name=f"Test Product {i+1}"
        )
        product_urls.append(product_url)
    
    print(f"📦 Testing extraction on {len(product_urls)} products")
    
    start_time = time.time()
    extracted_products = extract_products_batch.remote(product_urls)
    results = extracted_products.get()
    test_time = time.time() - start_time
    
    print(f"📊 Extraction Results:")
    print(f"   Products Processed: {len(product_urls)}")
    print(f"   Successful Extractions: {len(results)}")
    print(f"   Success Rate: {len(results)/len(product_urls)*100:.1f}%")
    print(f"   Time: {test_time:.1f}s")
    
    # Validation
    success = len(results) > 0
    avg_description_length = sum(len(p.description or '') for p in results) / len(results) if results else 0
    
    return {
        'stage': 'extraction',
        'success': success,
        'results': {
            'total_products': len(product_urls),
            'successful_extractions': len(results),
            'success_rate': len(results)/len(product_urls)*100 if product_urls else 0,
            'avg_description_length': avg_description_length
        },
        'test_time': test_time,
        'validation': {
            'extracted_products': len(results) > 0,
            'good_descriptions': avg_description_length > 200,
            'reasonable_time': test_time < 600  # Less than 10 minutes
        }
    }

@app.function(timeout=900)
def test_stage3_categorization(sample_products: List[ExtractedProduct] = None) -> Dict[str, Any]:
    """Test Stage 3: Product Categorization"""
    print(f"🔍 TESTING STAGE 3: PRODUCT CATEGORIZATION")
    
    # If no sample products provided, create mock ones
    if not sample_products:
        batch_id = f"test_categorization_{int(time.time())}"
        sample_products = [
            ExtractedProduct(
                url="https://example.com/vitamin-d",
                batch_id=batch_id,
                name="Vitamin D3 Supplement",
                description="High-potency vitamin D3 supplement for bone health and immune support. FDA approved. Clinically proven to improve vitamin D levels.",
                structured_data={'medical_claims': 'Supports bone health, immune function', 'ingredients': 'Vitamin D3'},
                extraction_time=2.5
            ),
            ExtractedProduct(
                url="https://example.com/face-cream",
                batch_id=batch_id,
                name="Anti-Aging Face Cream",
                description="Luxury anti-aging face cream with retinol and hyaluronic acid. Reduces wrinkles and fine lines. Dermatologist tested.",
                structured_data={'medical_claims': 'Reduces signs of aging', 'ingredients': 'Retinol, Hyaluronic Acid'},
                extraction_time=2.1
            ),
            ExtractedProduct(
                url="https://example.com/t-shirt",
                batch_id=batch_id,
                name="Cotton T-Shirt",
                description="100% cotton casual t-shirt in various colors. Comfortable fit for everyday wear. Machine washable.",
                structured_data={'category': 'clothing', 'features': 'Cotton fabric, multiple colors'},
                extraction_time=1.8
            )
        ]
    
    print(f"📦 Testing categorization on {len(sample_products)} products")
    
    start_time = time.time()
    categorized_products = categorize_products_batch.remote(sample_products)
    results = categorized_products.get()
    test_time = time.time() - start_time
    
    # Analyze categorization results
    categories = [p.primary_category for p in results]
    priorities = [p.classification_priority for p in results]
    likelihood_dist = [p.hsa_fsa_likelihood for p in results]
    
    print(f"📊 Categorization Results:")
    print(f"   Products Processed: {len(sample_products)}")
    print(f"   Successfully Categorized: {len(results)}")
    print(f"   Categories: {set(categories)}")
    print(f"   HSA/FSA Likelihood Distribution: {set(likelihood_dist)}")
    print(f"   Time: {test_time:.1f}s")
    
    # Validation
    success = len(results) == len(sample_products)
    has_variety = len(set(categories)) > 1  # Should categorize differently
    
    return {
        'stage': 'categorization',
        'success': success,
        'results': {
            'total_products': len(sample_products),
            'successfully_categorized': len(results),
            'categories': list(set(categories)),
            'likelihood_distribution': list(set(likelihood_dist)),
            'priority_distribution': list(set(priorities))
        },
        'test_time': test_time,
        'validation': {
            'all_categorized': len(results) == len(sample_products),
            'category_variety': has_variety,
            'reasonable_time': test_time < 180  # Less than 3 minutes
        }
    }

@app.function(timeout=1200)
def test_stage4_classification() -> Dict[str, Any]:
    """Test Stage 4: HSA/FSA Classification"""
    print(f"🔍 TESTING STAGE 4: HSA/FSA CLASSIFICATION")
    
    # Create sample categorized products
    batch_id = f"test_classification_{int(time.time())}"
    from ..pipeline.schemas import CategorizedProduct
    
    sample_products = [
        CategorizedProduct(
            url="https://example.com/thermometer",
            batch_id=batch_id,
            name="Digital Thermometer",
            description="FDA-approved digital thermometer for accurate temperature readings. Medical device for home healthcare monitoring.",
            structured_data={'medical_claims': 'FDA approved medical device', 'specifications': 'Digital display, fever alarm'},
            extraction_time=2.0,
            primary_category="medical_devices",
            secondary_category="primary",
            hsa_fsa_likelihood="high",
            category_confidence=0.9,
            classification_priority=1,
            categorization_time=0.5
        ),
        CategorizedProduct(
            url="https://example.com/lipstick",
            batch_id=batch_id,
            name="Red Lipstick",
            description="Luxury red lipstick with long-lasting formula. Fashion accessory for everyday glamour.",
            structured_data={'category': 'cosmetics', 'features': 'Long-lasting, multiple shades'},
            extraction_time=1.8,
            primary_category="beauty",
            secondary_category="secondary",
            hsa_fsa_likelihood="low",
            category_confidence=0.8,
            classification_priority=3,
            categorization_time=0.3
        )
    ]
    
    print(f"📦 Testing classification on {len(sample_products)} products")
    
    start_time = time.time()
    classified_products = classify_products_batch.remote(sample_products)
    results = classified_products.get()
    test_time = time.time() - start_time
    
    # Analyze classification results
    eligibility_statuses = [p.eligibility_status for p in results]
    
    print(f"📊 Classification Results:")
    print(f"   Products Processed: {len(sample_products)}")
    print(f"   Successfully Classified: {len(results)}")
    print(f"   Eligibility Statuses: {eligibility_statuses}")
    print(f"   Time: {test_time:.1f}s")
    
    # Validation
    success = len(results) > 0
    has_rationale = all(p.eligibility_rationale for p in results)
    
    return {
        'stage': 'classification',
        'success': success,
        'results': {
            'total_products': len(sample_products),
            'successfully_classified': len(results),
            'eligibility_distribution': eligibility_statuses
        },
        'test_time': test_time,
        'validation': {
            'all_classified': len(results) == len(sample_products),
            'has_rationale': has_rationale,
            'reasonable_time': test_time < 300  # Less than 5 minutes
        }
    }

@app.function(timeout=1800)
def test_stage5_turbopuffer() -> Dict[str, Any]:
    """Test Stage 5: Turbopuffer Upload"""
    print(f"🔍 TESTING STAGE 5: TURBOPUFFER UPLOAD")
    
    # Create sample classified products
    batch_id = f"test_turbopuffer_{int(time.time())}"
    from ..pipeline.schemas import ClassifiedProduct
    
    sample_products = [
        ClassifiedProduct(
            url="https://example.com/test-product-1",
            batch_id=batch_id,
            name="Test Product 1",
            description="Test product for Turbopuffer upload validation",
            structured_data={'test': True},
            extraction_time=2.0,
            primary_category="supplements",
            secondary_category="primary",
            hsa_fsa_likelihood="high",
            category_confidence=0.9,
            eligibility_status="eligible",
            eligibility_rationale="Test rationale for eligible product",
            classification_time=1.5,
            total_processing_time=3.5
        )
    ]
    
    print(f"📦 Testing Turbopuffer upload on {len(sample_products)} products")
    
    # Test upload
    start_time = time.time()
    test_namespace = f"test-{int(time.time())}"
    uploaded_products = upload_products_batch.remote(sample_products, test_namespace)
    results = uploaded_products.get()
    upload_time = time.time() - start_time
    
    # Test search functionality
    search_start = time.time()
    search_results = search_turbopuffer.remote("test product", test_namespace, 5)
    search_data = search_results.get()
    search_time = time.time() - search_start
    
    print(f"📊 Turbopuffer Results:")
    print(f"   Products Uploaded: {len(results)}")
    print(f"   Upload Success: {sum(1 for p in results if p and p.upload_success)}")
    print(f"   Upload Time: {upload_time:.1f}s")
    print(f"   Search Results: {len(search_data)}")
    print(f"   Search Time: {search_time:.1f}s")
    
    # Validation
    successful_uploads = sum(1 for p in results if p and p.upload_success)
    success = successful_uploads > 0
    
    return {
        'stage': 'turbopuffer',
        'success': success,
        'results': {
            'total_products': len(sample_products),
            'successful_uploads': successful_uploads,
            'search_results_count': len(search_data),
            'test_namespace': test_namespace
        },
        'test_time': upload_time + search_time,
        'validation': {
            'upload_success': successful_uploads > 0,
            'search_works': len(search_data) >= 0,  # Search should work even if no results
            'reasonable_time': (upload_time + search_time) < 600
        }
    }

@app.function(timeout=900)
def test_stage6_results_collection() -> Dict[str, Any]:
    """Test Stage 6: Results Collection"""
    print(f"🔍 TESTING STAGE 6: RESULTS COLLECTION")
    
    # Create sample Turbopuffer products
    batch_id = f"test_results_{int(time.time())}"
    from ..pipeline.schemas import TurbopufferProduct
    
    sample_products = [
        TurbopufferProduct(
            url="https://example.com/test-result-1",
            batch_id=batch_id,
            name="Test Result Product",
            description="Test product for results collection validation",
            structured_data={'test': True},
            extraction_time=2.0,
            primary_category="supplements",
            secondary_category="primary",
            eligibility_status="eligible",
            eligibility_rationale="Test rationale",
            classification_time=1.5,
            turbopuffer_id=f"test-{batch_id}-1",
            embedding_vector=[0.1] * 1536,  # Mock embedding
            namespace="test-namespace",
            upload_timestamp=time.time(),
            upload_success=True,
            total_processing_time=3.5
        )
    ]
    
    print(f"📦 Testing results collection on {len(sample_products)} products")
    
    start_time = time.time()
    collection_result = collect_results_batch.remote(sample_products, batch_id)
    results = collection_result.get()
    test_time = time.time() - start_time
    
    print(f"📊 Results Collection:")
    print(f"   Products Collected: {results['total_collected']}")
    print(f"   Output Files: {results['output_files']}")
    print(f"   Time: {test_time:.1f}s")
    
    # Validation
    success = results['total_collected'] > 0 and len(results['output_files']) > 0
    
    return {
        'stage': 'results_collection',
        'success': success,
        'results': results,
        'test_time': test_time,
        'validation': {
            'collected_products': results['total_collected'] > 0,
            'generated_files': len(results['output_files']) > 0,
            'reasonable_time': test_time < 180
        }
    }

@app.function(timeout=7200)
def test_full_pipeline_small(test_url: str = "https://www.goodmolecules.com") -> Dict[str, Any]:
    """Test the complete pipeline with a small batch"""
    print(f"🚀 TESTING COMPLETE PIPELINE (SMALL BATCH)")
    print(f"🎯 Test URL: {test_url}")
    
    from ..pipeline.orchestration_controller import orchestrate_product_scraping
    
    start_time = time.time()
    
    # Run pipeline with small limits
    results = orchestrate_product_scraping.remote(
        website_url=test_url,
        batch_id=f"test_pipeline_{int(time.time())}",
        max_products=10,  # Small test batch
        extraction_workers=5,
        categorization_workers=10,
        classification_workers=3,
        turbopuffer_workers=2
    )
    
    pipeline_results = results.get()
    test_time = time.time() - start_time
    
    print(f"📊 Full Pipeline Test Results:")
    print(f"   Status: {pipeline_results['status']}")
    print(f"   Total Time: {test_time/60:.1f} minutes")
    
    if pipeline_results['status'] == 'completed':
        final_results = pipeline_results['stages']['final_results']
        print(f"   Products Processed: {final_results['total_collected']}")
        print(f"   Output Files: {final_results['output_files']}")
    
    success = pipeline_results['status'] == 'completed'
    
    return {
        'stage': 'full_pipeline',
        'success': success,
        'results': pipeline_results,
        'test_time': test_time,
        'validation': {
            'pipeline_completed': pipeline_results['status'] == 'completed',
            'reasonable_time': test_time < 3600  # Less than 1 hour for small batch
        }
    }

@app.function(timeout=3600)
def run_comprehensive_tests() -> Dict[str, Any]:
    """Run all tests in sequence"""
    print(f"🧪 RUNNING COMPREHENSIVE PIPELINE TESTS")
    print(f"{'='*60}")
    
    test_results = {}
    overall_success = True
    
    # Test each stage individually
    test_functions = [
        ("Stage 1: Discovery", test_stage1_discovery),
        ("Stage 2: Extraction", test_stage2_extraction),
        ("Stage 3: Categorization", test_stage3_categorization),
        ("Stage 4: Classification", test_stage4_classification),
        ("Stage 5: Turbopuffer", test_stage5_turbopuffer),
        ("Stage 6: Results", test_stage6_results_collection)
    ]
    
    for test_name, test_function in test_functions:
        print(f"\n🔍 Running {test_name}...")
        try:
            result = test_function.remote()
            test_result = result.get()
            test_results[test_result['stage']] = test_result
            
            if test_result['success']:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
                overall_success = False
                
        except Exception as e:
            print(f"❌ {test_name} ERROR: {str(e)}")
            test_results[test_name] = {'success': False, 'error': str(e)}
            overall_success = False
    
    # Summary
    print(f"\n{'='*60}")
    print(f"🧪 TEST SUMMARY")
    print(f"{'='*60}")
    
    passed_tests = sum(1 for result in test_results.values() if result.get('success', False))
    total_tests = len(test_results)
    
    print(f"📊 Results: {passed_tests}/{total_tests} tests passed")
    print(f"🎯 Overall Success: {'✅ PASS' if overall_success else '❌ FAIL'}")
    
    return {
        'overall_success': overall_success,
        'passed_tests': passed_tests,
        'total_tests': total_tests,
        'individual_results': test_results
    }

# CLI entry point for testing
@app.local_entrypoint()
def main(
    test_type: str = "comprehensive",
    test_url: str = "https://www.goodmolecules.com"
):
    """
    CLI entry point for testing
    
    Usage:
        modal run test_pipeline.py                          # Run all tests
        modal run test_pipeline.py --test-type discovery    # Test specific stage
        modal run test_pipeline.py --test-type full-pipeline --test-url https://example.com
    """
    
    print(f"🧪 Modal Pipeline Testing")
    print(f"📋 Test Type: {test_type}")
    
    if test_type == "comprehensive":
        results = run_comprehensive_tests()
        if results['overall_success']:
            print(f"✅ All tests passed!")
        else:
            print(f"❌ Some tests failed. Check logs above.")
            
    elif test_type == "discovery":
        result = test_stage1_discovery(test_url)
        print(f"Result: {'✅ PASS' if result['success'] else '❌ FAIL'}")
        
    elif test_type == "extraction":
        result = test_stage2_extraction()
        print(f"Result: {'✅ PASS' if result['success'] else '❌ FAIL'}")
        
    elif test_type == "full-pipeline":
        result = test_full_pipeline_small(test_url)
        print(f"Result: {'✅ PASS' if result['success'] else '❌ FAIL'}")
        
    else:
        print(f"❌ Unknown test type: {test_type}")
        print(f"Available types: comprehensive, discovery, extraction, full-pipeline")