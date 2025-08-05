# Product Eligibility Pipeline

A comprehensive HSA/FSA product eligibility classification system built on Modal.com that processes e-commerce websites to determine which products qualify for Health Savings Account (HSA) and Flexible Spending Account (FSA) purchases.

## Overview

The pipeline automatically discovers, extracts, categorizes, and classifies products from any e-commerce website to determine their HSA/FSA eligibility status. It uses advanced web scraping, AI-powered categorization, and intelligent classification to process thousands of products efficiently.

## Architecture

The pipeline consists of 5 main stages that run in an overlapping architecture for maximum performance:

```
Discovery → Extraction ↘
                        → Categorization → Classification → CSV Export
```

**Key Features:**
- ✅ **Overlapping stages** - Multiple stages run simultaneously for maximum throughput
- ✅ **Queue-based processing** - Uses Modal queues for distributed work
- ✅ **Automatic scaling** - Dynamic worker allocation (50 extraction, 30 categorization, 30 classification)
- ✅ **Robust error handling** - Graceful failures with detailed logging
- ✅ **S3 checkpointing** - Resume from failures, avoid duplicate work
- ✅ **24-hour timeouts** - Handles large datasets without timing out

## Stage 1: Discovery

**Purpose:** Find all product URLs on an e-commerce website

**Technology Stack:**
- Sitemap parsing (primary method)
- Firecrawl crawl_url API (fallback method)
- BeautifulSoup for HTML parsing

**Process:**
1. **Sitemap Discovery (Primary)**
   - Checks `robots.txt` for sitemap locations
   - Parses XML sitemaps to find product URLs
   - Handles gzipped sitemaps and sitemap indexes
   - Filters for product-specific URL patterns
   - Excludes non-product pages (login, terms, etc.)

2. **Firecrawl Crawl (Fallback)**
   - Uses Firecrawl's intelligent crawling when sitemaps fail
   - Crawls up to 100 pages across the site
   - Automatically navigates categories and pagination
   - Extracts product names from page titles/H1 tags
   - Filters out obvious non-product pages

**Output:**
- `discovered_urls.csv` - List of discovered product URLs with metadata
- S3 location: `s3://flex-ai/{environment}/{execution_id}/discovery/`

**Key Parameters:**
- `max_products`: Maximum number of products to discover
- `discovery_depth`: Crawl depth (default: 3)

## Stage 2: Extraction

**Purpose:** Extract detailed product information from each discovered URL

**Technology Stack:**
- Firecrawl API for web scraping
- Structured data extraction with JSON schema
- 50 parallel workers for high throughput

**Process:**
1. **Queue Processing**
   - Reads URLs from discovery queue
   - Processes URLs in parallel using 50 workers
   - Implements checkpointing to avoid re-processing

2. **Firecrawl Extraction**
   - Uses Firecrawl's `scrape_url` with structured extraction
   - Extracts: name, description, price, brand, features, category
   - Handles JavaScript-heavy sites and anti-bot measures
   - Returns structured JSON data

3. **Data Processing**
   - Validates extracted data quality
   - Adds metadata (timestamps, worker IDs, execution tracking)
   - Saves individual product JSON files to S3
   - Queues successful extractions for categorization

**Output:**
- Individual JSON files for each product
- `extracted_products.csv` - Consolidated extraction results
- S3 location: `s3://flex-ai/{environment}/{execution_id}/extraction/`

**Extracted Fields:**
```json
{
  "name": "Product Name",
  "description": "Product description...",
  "price": "$29.99",
  "brand": "Brand Name",
  "features": "Key features...",
  "extracted_category": "Category",
  "status": "success",
  "url": "https://...",
  "extraction_timestamp": 1234567890
}
```

## Stage 3: Categorization

**Purpose:** Categorize products using AI to determine their primary category

**Technology Stack:**
- OpenAI GPT-4o-mini for categorization
- 30 parallel workers
- Custom category taxonomy for HSA/FSA products

**Process:**
1. **Queue Processing**
   - Reads extracted products from categorization queue
   - Processes in parallel using 30 workers
   - Implements checkpointing for reliability

2. **AI Categorization**
   - Uses structured prompt with product details
   - References comprehensive category taxonomy
   - Returns category with confidence score and reasoning
   - Maps to HSA/FSA-relevant categories

3. **Category Validation**
   - Validates against predefined category list
   - Ensures consistent categorization
   - Adds categorization metadata

**Output:**
- Individual JSON files with categorization data
- `categorized_products.csv` - Consolidated categorization results
- S3 location: `s3://flex-ai/{environment}/{execution_id}/categorization/`

**Category Taxonomy:** Includes categories like:
- Personal Care & Hygiene
- Medical Devices & Equipment
- Health & Wellness Supplements
- First Aid & Safety
- Vision Care
- And 20+ other HSA/FSA-relevant categories

## Stage 4: Classification

**Purpose:** Determine HSA/FSA eligibility status for each categorized product

**Technology Stack:**
- OpenAI GPT-4o-mini for classification
- 30 parallel workers
- HSA/FSA compliance rules and guidelines

**Process:**
1. **Queue Processing**
   - Reads categorized products from classification queue
   - Processes in parallel using 30 workers
   - Implements checkpointing for consistency

2. **Eligibility Classification**
   - Uses detailed HSA/FSA compliance prompts
   - References IRS guidelines and regulations
   - Considers product category, description, and features
   - Provides eligibility rationale and confidence score

3. **Classification Output**
   - Eligibility status: "Eligible", "Not Eligible", "Requires Letter of Medical Necessity"
   - Detailed rationale explaining the decision
   - Confidence score (0-100)
   - Additional considerations and notes

**Output:**
- Individual JSON files with classification data
- `classified_products.csv` - **FINAL RESULTS**
- S3 location: `s3://flex-ai/{environment}/{execution_id}/classification/`

**Final Classification Schema:**
```json
{
  "eligibility_status": "Eligible|Not Eligible|Requires LMN",
  "eligibility_rationale": "Detailed explanation...",
  "additional_considerations": "Extra notes...",
  "lmn_qualification_probability": "High|Medium|Low",
  "classification_confidence": 85,
  "classification_timestamp": 1234567890
}
```

## Stage 5: CSV Consolidation

**Purpose:** Consolidate all individual JSON files into final CSV files

**Technology Stack:**
- Pandas for data processing
- S3 batch operations
- 4GB memory allocation for large datasets

**Process:**
1. **JSON Collection**
   - Reads all JSON files from each stage
   - Handles pagination for large result sets (6000+ files)
   - Processes files in batches for memory efficiency

2. **CSV Generation**
   - Creates consolidated CSV for each stage
   - Maintains all original data and metadata
   - Optimized for large datasets

**Output:**
- `extracted_products.csv` - All extraction results
- `categorized_products.csv` - All categorization results  
- `classified_products.csv` - **PRIMARY OUTPUT** - Complete HSA/FSA eligibility data

## Usage

### API Endpoints

#### Run Complete Pipeline
```bash
curl -X POST "https://joinflexhealth--product-eligibility-api-run-pipeline.modal.run" \
  -H "Content-Type: application/json" \
  -d '{
    "base_url": "https://example.com/",
    "max_products": 1000,
    "environment": "prod"
  }'
```

#### Run Discovery Only
```bash
curl -X POST "https://joinflexhealth--product-eligibility-api-discovery.modal.run" \
  -H "Content-Type: application/json" \
  -d '{
    "base_url": "https://example.com/",
    "max_products": 100
  }'
```

### Command Line

#### Complete Pipeline
```bash
modal run product_eligibility.py::run_full_pipeline \
  --base-url "https://example.com/" \
  --max-products 1000
```

#### Individual Stages
```bash
# Discovery only
modal run product_eligibility.py::discovery_stage \
  --base-url "https://example.com/" \
  --max-products 500

# Consolidate existing results
modal run product_eligibility.py::consolidate_json_to_csv \
  --execution-id "exec_1234567890" \
  --stage-name "classification"
```

## Performance & Scaling

### Worker Configuration
- **Discovery**: 1 worker (sitemap parsing + Firecrawl crawling)
- **Extraction**: 50 workers (Firecrawl API calls)
- **Categorization**: 30 workers (OpenAI API calls)  
- **Classification**: 30 workers (OpenAI API calls)
- **CSV Consolidation**: 1 worker (4GB memory, 2 CPU cores)

### Throughput
- **Typical processing rate**: 1000-5000 products per hour (depends on site complexity)
- **Bottlenecks**: API rate limits (Firecrawl, OpenAI)
- **Optimization**: Overlapping stages maximize parallel processing

### Resource Limits
- **Function timeouts**: 24 hours (maximum Modal allows)
- **Memory allocation**: 2-4GB per worker depending on stage
- **Queue limits**: 5000 items per Modal queue
- **Storage**: Unlimited S3 storage for results

## Error Handling & Reliability

### Checkpointing
- All stages implement S3-based checkpointing
- Resume from failures without losing progress
- Skip already-processed items automatically

### Failure Modes
- **Discovery failure**: Pipeline fails fast if both sitemap and Firecrawl fail
- **Extraction failure**: Individual products marked as failed, pipeline continues
- **AI failures**: Products marked with error status, pipeline continues
- **Queue timeouts**: Workers gracefully handle empty queues and completion signals

### Monitoring
- Detailed logging at each stage
- Progress tracking via S3 file counts  
- Error reporting with specific failure reasons
- Real-time worker status updates

## Output Files

### Primary Output
- **`classified_products.csv`** - Complete HSA/FSA eligibility results
  - All original product data + categorization + classification
  - Ready for import into e-commerce systems
  - Includes confidence scores and rationales

### Intermediate Files  
- `discovered_urls.csv` - All discovered product URLs
- `extracted_products.csv` - Product details from web scraping
- `categorized_products.csv` - Products with AI-assigned categories

### File Locations
All files stored in S3: `s3://flex-ai/{environment}/{execution_id}/{stage}/`

## Requirements

### Environment Variables
```bash
FIRECRAWL_API_KEY=your_firecrawl_key
OPENAI_API_KEY=your_openai_key
```

### Modal Secrets
- `firecrawl-api-key`
- `openai-api-key`  
- `aws-s3-credentials`

### Dependencies
- modal
- firecrawl-py
- openai
- beautifulsoup4
- pandas
- boto3
- requests

## Supported E-commerce Platforms

The pipeline works with any e-commerce website, including:
- ✅ **Shopify** stores
- ✅ **WooCommerce** sites  
- ✅ **BigCommerce** stores
- ✅ **Custom e-commerce** platforms
- ✅ **Major retailers** (Amazon, Target, REI, etc.)
- ✅ **Health & beauty** sites (Dermstore, Sephora, etc.)

**Universal compatibility** through intelligent URL pattern detection and Firecrawl's robust scraping capabilities.

## Example Results

```csv
url,name,price,brand,primary_category,eligibility_status,eligibility_rationale,classification_confidence
https://site.com/product/1,Blood Pressure Monitor,$89.99,Omron,Medical Devices,Eligible,FDA-approved medical device for monitoring health conditions,95
https://site.com/product/2,Luxury Face Cream,$150.00,Brand,Personal Care,Not Eligible,Cosmetic product for general beauty purposes not medical need,90
https://site.com/product/3,Thermometer,$24.99,Brand,Medical Devices,Eligible,Essential medical device for health monitoring,98
```

## Contributing

1. All functions have 24-hour timeouts and adequate memory allocation
2. Use proper error handling and checkpointing
3. Follow the queue-based architecture patterns
4. Add comprehensive logging for debugging
5. Test with small datasets before large production runs

## License

Internal use only - Proprietary software for HSA/FSA product classification.