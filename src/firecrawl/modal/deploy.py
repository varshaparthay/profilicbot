#!/usr/bin/env python3
"""
Production deployment for Modal S3-based product scraping pipeline
5-Stage Pipeline: Discovery → Extraction → Categorization → Classification → Turbopuffer
"""

import modal
from pipeline.config import app, image, secrets

# Import core pipeline functions
from pipeline.main_orchestrator import (
    run_complete_pipeline, 
    get_pipeline_status,
    run_single_site_pipeline
)
from pipeline.s3_utils import test_s3_connection
from pipeline.url_discovery import stage1_discovery_orchestrator
from pipeline.extraction_dispatcher import stage2_extraction_dispatcher
from pipeline.categorization_dispatcher import stage3_categorization_dispatcher
from pipeline.classification_dispatcher import stage4_classification_dispatcher
from pipeline.turbopuffer_dispatcher import stage5_turbopuffer_dispatcher

@app.function()
def health_check():
    """Pipeline health check"""
    return {"status": "healthy", "pipeline": "s3-product-scraper-v1"}

if __name__ == "__main__":
    print("🚀 Deploying Production S3-based Product Scraping Pipeline...")
    print("📋 Available Functions:")
    print("   • run_complete_pipeline - Full 5-stage pipeline")
    print("   • run_single_site_pipeline - Single site processing")
    print("   • get_pipeline_status - Check pipeline status")
    print("   • health_check - Health check")
    print("   • Individual stage functions (stage1-5)")
    print("\n✅ Production deployment ready!")