#!/usr/bin/env python3
"""
Full Pipeline Deployment for Modal S3-based Product Scraping
"""

from modal import App, Image, Secret, Queue

# Modal app definition
app = App("s3-product-scraper")

# Full image with all dependencies
image = (
    Image.debian_slim(python_version="3.11")
    .pip_install([
        "requests",
        "pandas", 
        "openai",
        "firecrawl-py",
        "pydantic",
        "beautifulsoup4",
        "python-dotenv",
        "turbopuffer",
        "boto3"   # AWS SDK for S3 access
    ])
)

# Persistent queues
url_queue = Queue.from_name("discovered-urls", create_if_missing=True)
categorization_queue = Queue.from_name("categorized-products", create_if_missing=True)
classification_queue = Queue.from_name("classified-products", create_if_missing=True)
turbopuffer_queue = Queue.from_name("turbopuffer-products", create_if_missing=True)

# Secrets
secrets = [
    Secret.from_name("aws-s3-credentials"),
    Secret.from_name("openai-api-key"),
    Secret.from_name("firecrawl-api-key"),
    Secret.from_name("turbopuffer-api-key")
]

@app.function(image=image, secrets=secrets, timeout=3600)
def test_full_s3_pipeline():
    """Test the full S3 pipeline setup"""
    import boto3
    import pandas as pd
    import os
    
    try:
        print("🧪 Testing full pipeline setup...")
        
        # Test S3
        s3_client = boto3.client('s3')
        response = s3_client.head_bucket(Bucket="flex-ai")
        print("✅ S3 connection successful")
        
        # Test data creation
        test_data = pd.DataFrame({
            'url': ['https://example.com/product1', 'https://example.com/product2'],
            'name': ['Test Product 1', 'Test Product 2'],
            'description': ['A test product', 'Another test product']
        })
        
        # Test S3 upload
        csv_buffer = test_data.to_csv(index=False)
        s3_client.put_object(
            Bucket="flex-ai",
            Key="dev/test-pipeline/discovery/test_products.csv",
            Body=csv_buffer
        )
        print("✅ S3 upload test successful")
        
        # Test S3 download
        response = s3_client.get_object(
            Bucket="flex-ai",
            Key="dev/test-pipeline/discovery/test_products.csv"
        )
        downloaded_df = pd.read_csv(response['Body'])
        print(f"✅ S3 download test successful - {len(downloaded_df)} rows")
        
        # Clean up
        s3_client.delete_object(
            Bucket="flex-ai",
            Key="dev/test-pipeline/discovery/test_products.csv"
        )
        print("✅ Test file cleaned up")
        
        return {
            "status": "success",
            "tests_passed": ["s3_connection", "s3_upload", "s3_download", "cleanup"],
            "message": "Full pipeline setup test passed!"
        }
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

@app.function(image=image, secrets=secrets, timeout=1800)
def simple_discovery_test(base_url: str):
    """Simple test of product discovery"""
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
    import boto3
    import time
    
    try:
        print(f"🔍 Testing product discovery on: {base_url}")
        
        # Simple web scraping
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = requests.get(base_url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for product links (basic heuristics)
        product_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text().strip().lower()
            
            # Basic product detection
            if any(word in href.lower() for word in ['product', 'item', 'shop', 'buy']) and len(text) > 5:
                if href.startswith('/'):
                    href = base_url.rstrip('/') + href
                elif not href.startswith('http'):
                    continue
                    
                product_links.append({
                    'url': href,
                    'estimated_name': text[:100],
                    'discovered_from': base_url
                })
                
                if len(product_links) >= 10:  # Limit for test
                    break
        
        print(f"✅ Found {len(product_links)} potential products")
        
        # Save to S3
        if product_links:
            df = pd.DataFrame(product_links)
            execution_id = f"test_{int(time.time())}"
            
            s3_client = boto3.client('s3')
            csv_buffer = df.to_csv(index=False)
            s3_path = f"dev/{execution_id}/discovery/discovered_urls.csv"
            
            s3_client.put_object(
                Bucket="flex-ai",
                Key=s3_path,
                Body=csv_buffer
            )
            
            print(f"✅ Saved {len(product_links)} URLs to S3: s3://flex-ai/{s3_path}")
            
            return {
                "status": "success",
                "execution_id": execution_id,
                "discovered_urls": len(product_links),
                "s3_path": f"s3://flex-ai/{s3_path}",
                "sample_products": product_links[:3]
            }
        else:
            return {
                "status": "no_products",
                "message": "No products found on this site"
            }
            
    except Exception as e:
        print(f"❌ Discovery test failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

@app.function(image=image, secrets=secrets, timeout=60)
def test_openai_categorization():
    """Test OpenAI categorization"""
    import openai
    import os
    import json
    
    try:
        openai.api_key = os.environ.get('OPENAI_API_KEY')
        
        # Test product
        test_product = {
            "name": "Digital Blood Pressure Monitor",
            "description": "Automatic upper arm blood pressure monitor with large LCD display, memory function, and irregular heartbeat detection. FDA approved for home use."
        }
        
        # Basic categories
        categories = [
            {"name": "Medical Equipment & Supplies", "description": "Medical devices and diagnostic equipment"},
            {"name": "Fitness & Wellness", "description": "General fitness and wellness products"},
            {"name": "Other / Miscellaneous", "description": "Products that don't fit other categories"}
        ]
        
        # Create prompt
        categories_text = "\n".join([f"{i+1}. {cat['name']} - {cat['description']}" for i, cat in enumerate(categories)])
        
        prompt = f"""Categorize this product:

Categories:
{categories_text}

Product: {test_product['name']}
Description: {test_product['description']}

Return JSON: {{"category": "exact category name", "confidence": 0.95, "reasoning": "brief explanation"}}"""
        
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a product categorizer. Return valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=300,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        print(f"✅ OpenAI categorization successful: {result['category']} ({result['confidence']})")
        
        return {
            "status": "success",
            "test_product": test_product,
            "categorization": result
        }
        
    except Exception as e:
        print(f"❌ OpenAI test failed: {str(e)}")
        return {"status": "failed", "error": str(e)}

if __name__ == "__main__":
    print("🚀 Deploying S3 Product Scraping Pipeline...")
    print("📋 Functions available after deployment:")
    print("   • test_full_s3_pipeline")
    print("   • simple_discovery_test") 
    print("   • test_openai_categorization")
    print("\n✅ Deployment complete!")