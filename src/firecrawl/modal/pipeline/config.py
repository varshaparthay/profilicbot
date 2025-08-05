#!/usr/bin/env python3
"""
Modal Configuration and Shared Setup
Latest Modal API for S3-based product scraping pipeline
"""

from modal import App, Image, Secret, Queue, Volume

# Modal app definition
app = App("orchestrated-product-scraper")

# Use Python 3.12 for deployment compatibility
image = (
    Image.debian_slim(python_version="3.12")
    .apt_install("curl")
    .pip_install([
        "requests",
        "pandas", 
        "openai",
        "firecrawl-py", 
        "pydantic",
        "beautifulsoup4",
        "python-dotenv",
        "turbopuffer",
        "boto3"  # AWS SDK for S3 access
    ])
)

# Volume for persistent storage (if needed)
data_volume = Volume.from_name("pipeline-data", create_if_missing=True)

# Create persistent queues for each stage of the pipeline
url_queue = Queue.from_name("discovered-urls", create_if_missing=True)
product_queue = Queue.from_name("extracted-products", create_if_missing=True)
categorization_queue = Queue.from_name("categorized-products", create_if_missing=True)
classification_queue = Queue.from_name("classified-products", create_if_missing=True)
turbopuffer_queue = Queue.from_name("turbopuffer-products", create_if_missing=True)

# Shared secrets - you'll need to create these in Modal dashboard
secrets = [
    Secret.from_name("aws-s3-credentials"),  # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    Secret.from_name("openai-api-key"),      # OPENAI_API_KEY
    Secret.from_name("firecrawl-api-key"),   # FIRECRAWL_API_KEY
    Secret.from_name("turbopuffer-api-key")  # TURBOPUFFER_API_KEY
]

# Prompts will be loaded from local files and passed as data
PROMPTS_PATH = "/Users/varsha/src/profilicbot/src/prompts"