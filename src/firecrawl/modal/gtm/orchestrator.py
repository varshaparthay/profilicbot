#!/usr/bin/env python3
"""
GTM Pipeline Orchestrator - Local execution script
"""

import modal
import argparse
import sys
from urllib.parse import urlparse

def validate_url(url):
    """Validate URL format"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def run_gtm_pipeline(website_url: str, single_url: bool = False, user_email: str = None):
    """Run the GTM pipeline"""
    
    if not validate_url(website_url):
        print(f"❌ Invalid URL: {website_url}")
        print("Please provide a valid URL starting with http:// or https://")
        return
    
    print(f"🚀 Starting GTM Pipeline")
    print(f"🌐 Website: {website_url}")
    print(f"🎯 Mode: {'Single URL' if single_url else 'Full Website Discovery'}")
    if user_email:
        print(f"📧 Email notification: {user_email}")
    
    try:
        # Import the Modal function
        print("🔧 Connecting to Modal...")
        
        # Get the function from the deployed app
        gtm_function = modal.Function.from_name("gtm-pipeline", "start_gtm_pipeline")
        
        # Execute the pipeline
        print(f"⚡ Executing GTM pipeline...")
        result = gtm_function.remote(website_url, single_url, user_email)
        
        # Print results
        print("\n" + "="*60)
        print("🎉 GTM PIPELINE COMPLETED!")
        print("="*60)
        
        print(f"📋 Execution ID: {result['execution_id']}")
        print(f"🌐 Website: {result['website_url']}")
        print(f"🎯 Mode: {'Single URL' if result['single_url_mode'] else 'Full Website Discovery'}")
        print(f"🔍 URLs Discovered: {result['urls_discovered']}")
        print(f"✅ URLs Processed: {result['urls_processed']}")
        print(f"❌ Errors: {result['errors']}")
        print(f"📁 Results: {result['results_path']}")
        print(f"💾 S3 Location: {result['s3_location']}")
        
        if result['errors'] > 0:
            print(f"\n⚠️ {result['errors']} URLs failed processing - check S3 for error details")
        
        if user_email:
            print(f"\n📧 Email notification sent to: {user_email}")
        
        # Worker summary
        print(f"\n📊 Worker Summary:")
        for i, worker_result in enumerate(result['worker_results']):
            print(f"   Worker {worker_result['worker_id']}: {worker_result['processed']} processed, {worker_result['errors']} errors")
        
        return result
        
    except Exception as e:
        print(f"💥 Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    parser = argparse.ArgumentParser(description='GTM Pipeline Orchestrator')
    parser.add_argument('website_url', help='Website URL to process (e.g., https://example.com)')
    parser.add_argument('--single', action='store_true', help='Process only the single URL (no discovery)')
    parser.add_argument('--email', type=str, help='Email address to send completion notification')
    
    args = parser.parse_args()
    
    print("🚀 GTM Pipeline Orchestrator")
    print("="*50)
    
    # Run the pipeline
    result = run_gtm_pipeline(args.website_url, args.single, args.email)
    
    if result and result.get('status') == 'completed':
        print("\n✅ Pipeline completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()