#!/usr/bin/env python3
"""
GTM Pipeline Webhook Endpoint
Web API endpoint to trigger GTM pipeline via HTTP requests
"""

import modal

# Create image with required dependencies and mounted files
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install([
        "fastapi[standard]", # Must be first for web endpoints
        "firecrawl-py",
        "openai", 
        "pandas",
        "boto3",
        "beautifulsoup4",
        "requests",
        "fsspec",           # Required for pandas S3 operations
        "s3fs"              # S3 filesystem for pandas
    ])
    .add_local_file("/Users/varsha/src/profilicbot/src/prompts/categorization_prompt.txt", remote_path="/app/categorization_prompt.txt")
    .add_local_file("/Users/varsha/src/profilicbot/src/prompts/flex_product_categories.json", remote_path="/app/flex_product_categories.json")  
    .add_local_file("/Users/varsha/src/profilicbot/src/prompts/flex_guide_mapped_to_categories.json", remote_path="/app/flex_guide_mapped_to_categories.json")
)

# Secrets for APIs
secrets = [
    modal.Secret.from_name("firecrawl-api-key"),
    modal.Secret.from_name("openai-api-key"), 
    modal.Secret.from_name("aws-s3-credentials")
]

app = modal.App("gtm-webhook")

# =============================================================================
# WEB API ENDPOINTS  
# =============================================================================

@app.function(secrets=secrets, timeout=86400, memory=2048)
@modal.fastapi_endpoint(method="POST", docs=True)
def api_run_gtm_pipeline(data: dict):
    """
    Run the GTM pipeline via REST API
    
    Expected JSON body:
    {
        "website_url": "https://example.com",
        "single_url": false,
        "email": "user@company.com"
    }
    """
    try:
        website_url = data.get("website_url")
        if not website_url:
            return {"status": "error", "error": "website_url is required"}
        
        single_url = data.get("single_url", False)
        user_email = data.get("email")
        
        # Validate URL format
        if not website_url.startswith(('http://', 'https://')):
            return {
                "status": "error", 
                "error": "Invalid URL format: website_url must start with http:// or https://"
            }
        
        print(f"🌐 GTM Pipeline API Request:")
        print(f"   Website URL: {website_url}")
        print(f"   Single URL Mode: {single_url}")
        print(f"   Email: {user_email or 'None'}")
        
        # Import and run the pipeline
        from pipeline import start_gtm_pipeline
        result = start_gtm_pipeline.remote(website_url, single_url, user_email)
        
        return {
            "status": result.get("status", "unknown"),
            "message": "GTM pipeline completed" if result.get("status") == "completed" else result.get("error", "Pipeline failed"),
            "execution_id": result.get("execution_id"),
            "website_url": result.get("website_url"), 
            "urls_discovered": result.get("urls_discovered"),
            "urls_processed": result.get("urls_processed"),
            "errors": result.get("errors"),
            "results_path": result.get("results_path"),
            "s3_location": result.get("s3_location"),
            "email_sent": user_email is not None and result.get("status") == "completed"
        }
        
    except Exception as e:
        print(f"💥 GTM Pipeline API Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "error",
            "error": str(e)
        }

@app.function()
@modal.fastapi_endpoint(docs=True)
def api_health():
    """Health check endpoint"""
    return {
        "status": "healthy", 
        "service": "gtm-pipeline-webhook",
        "version": "1.0.0",
        "endpoints": {
            "POST /api_run_gtm_pipeline": "Trigger GTM pipeline",
            "GET /api_health": "Health check",
            "GET /docs": "API documentation"
        }
    }

if __name__ == "__main__":
    print("🚀 GTM Pipeline Webhook")
    print("=" * 50)
    print("Web endpoints:")
    print("  POST /api_run_gtm_pipeline - Trigger GTM pipeline")  
    print("  GET /api_health - Health check")
    print("  GET /docs - API documentation")
    print()
    print("Deploy with: modal deploy webhook.py")
    print("Then use the webhook URL to trigger via curl!")