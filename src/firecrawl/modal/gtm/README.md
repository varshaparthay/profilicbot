# GTM Pipeline - Website Discovery and Content Processing

A Modal-based pipeline that uses Firecrawl's map functionality to discover and process website content at scale.

## Overview

The GTM Pipeline provides two processing modes:

1. **Single URL Mode** (`--single`): Process only the specified URL
2. **Full Website Discovery Mode** (default): Use Firecrawl's map to discover all URLs on a website and process them

## Features

- **Firecrawl Map Integration**: Automatically discover all URLs on a website
- **Scalable Processing**: Queue-based architecture with up to 50 parallel workers
- **Content Extraction**: Extract title, content, and metadata from each URL
- **Content Analysis**: Basic content type detection and topic extraction
- **S3 Storage**: Results saved to S3 in JSON and CSV formats
- **Error Handling**: Comprehensive error handling with retry logic

## Quick Start

### 1. Deploy the Pipeline

```bash
cd /Users/varsha/src/profilicbot/src/firecrawl/modal/gtm
modal deploy pipeline.py
```

### 2. Run Single URL Mode

```bash
python3 orchestrator.py https://example.com --single
```

### 3. Run Full Website Discovery

```bash
python3 orchestrator.py https://example.com
```

### 4. Run with Email Notification

```bash
# Single URL with email notification
python3 orchestrator.py https://example.com --single --email user@company.com

# Full website discovery with email notification  
python3 orchestrator.py https://example.com --email user@company.com
```


## Architecture

### Pipeline Stages

1. **Discovery Stage**: 
   - Single URL: Adds the URL directly to processing queue
   - Full Website: Uses Firecrawl's `map_url()` to discover all URLs on the website

2. **Processing Stage** (matches dermstore structure):
   - **Stage 1 - Firecrawl Scrape**: Enhanced scraping with retry logic using Firecrawl's structured extraction with AI prompts
   - **Stage 2 - Categorization**: AI-powered categorization using OpenAI GPT-4o-mini to classify content into up to 3 categories from 60+ predefined categories
   - **Stage 3 - Classification**: HSA/FSA eligibility classification using dynamic guide lookup based on categorization results

3. **Consolidation Stage**:
   - Collects all worker results from S3
   - Creates final CSV with all processed URLs and their data
   - Sends email notification if email address provided

### Worker Architecture

- **Dynamic Worker Scaling**: 1 worker per 10 URLs (minimum 5, maximum 50)
- **Queue-based Processing**: Uses Modal Queue for distributed task processing
- **Fault Tolerance**: Workers handle timeouts and API failures gracefully
- **Checkpointing**: Results saved to S3 immediately after processing

## Output Format

### Individual Results (JSON)
Saved to: `s3://flex-ai/gtm/{execution_id}/results/{url_id}.json`

```json
{
  "url": "https://example.com/page",
  "url_id": "gtm_123_url_000001",
  "execution_id": "gtm_123",
  "discovery_method": "firecrawl_map",
  
  // Stage 1: Extraction (same format as dermstore)
  "extracted_name": "Page Title or Main Heading",
  "detailed_description": "Comprehensive page description including what it is, key information, important details, and main topics covered...",
  "ingredients": "Key components, technologies, or elements mentioned",
  "conditions_treats": "Problems, issues, or use cases this content addresses",
  "category": "blog",
  "extraction_status": "success",
  
  // Stage 2: Categorization
  "primary_category": "Medical Equipment & Supplies",
  "secondary_category": "Telemedicine & Virtual Care", 
  "tertiary_category": "Diagnostic & Monitoring Tests",
  "categorization_reasoning": "Content discusses medical technology and diagnostic tools",
  "categorization_confidence": 85,
  "categorization_status": "success",
  
  // Stage 3: Classification (HSA/FSA Eligibility)
  "eligibilityStatus": "Eligible",
  "explanation": "Product qualifies as medical equipment for health monitoring",
  "additionalConsiderations": "Device must be used for medical purposes",
  "lmnQualificationProbability": "N/A",
  "confidencePercentage": 95,
  "classification_status": "success",
  
  // Metadata
  "processing_timestamp": 1703123456.789,
  "overall_status": "completed"
}
```

### Consolidated Results (CSV)
Saved to: `s3://flex-ai/gtm/{execution_id}/outputs/gtm_{domain}_{execution_id}.csv`

Contains all processed URLs with their extracted content and analysis.

## Testing

### Run Full Test Suite
```bash
modal run test_pipeline.py::run_full_test_suite
```

### Individual Tests
```bash
# Test S3 connectivity
modal run test_pipeline.py::test_s3_connectivity

# Test single URL mode
modal run test_pipeline.py::test_single_url_mode

# Test map discovery mode
modal run test_pipeline.py::test_map_discovery_mode
```

## Configuration

### Required Secrets
- `firecrawl-api-key`: Firecrawl API key for web scraping and extraction
- `openai-api-key`: OpenAI API key for categorization and classification stages  
- `aws-s3-credentials`: AWS credentials for S3 access and SES email notifications

### Environment Variables
Set these in your Modal secrets:
- `FIRECRAWL_API_KEY`: Your Firecrawl API key
- `OPENAI_API_KEY`: Your OpenAI API key
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

## Examples

### Process a Single Page
```bash
python3 orchestrator.py https://docs.firecrawl.dev --single
```

### Discover and Process Entire Website
```bash
python3 orchestrator.py https://firecrawl.dev
```

## Pipeline Functions

### Main Functions
- `start_gtm_pipeline(website_url, single_url)`: Main pipeline orchestrator
- `discover_and_load_urls_to_queue()`: Firecrawl map-based URL discovery
- `load_single_url_to_queue()`: Single URL processing
- `gtm_worker()`: Parallel worker for URL processing
- `consolidate_gtm_results()`: Results consolidation

### Processing Functions
- `process_single_url()`: Process individual URL through 3-stage extraction, categorization, and classification
- `stage1_firecrawl_scrape()`: Extract content using Firecrawl with structured AI prompts
- `stage2_categorize_content()`: AI-powered categorization into up to 3 predefined categories  
- `stage3_classify_eligibility()`: HSA/FSA classification with dynamic guide lookup

### Utility Functions
- `save_gtm_result_to_s3()`: Save successful results to S3
- `save_gtm_error_to_s3()`: Save error results to S3
- `health_check()`: Pipeline health check

## Scaling and Performance

- **URL Discovery**: Firecrawl map is fast and comprehensive
- **Parallel Processing**: Up to 50 workers process URLs simultaneously
- **Memory Efficient**: Content limited to 5000 characters per URL for storage
- **Rate Limiting**: Built-in delays to respect target websites
- **Timeout Handling**: 24-hour maximum runtime with proper timeout handling

## Troubleshooting

### Common Issues

1. **"Invalid URL" Error**: Ensure URL starts with `http://` or `https://`
2. **No URLs Discovered**: Website might not have a sitemap or block crawlers
3. **Extraction Failures**: Target website might block scraping or have complex JS
4. **S3 Errors**: Check AWS credentials and S3 permissions

### Debugging

1. Check Modal logs: `modal logs gtm-pipeline`
2. View S3 results: Check `s3://flex-ai/gtm/{execution_id}/results/`
3. Run tests: `modal run test_pipeline.py::run_full_test_suite`

## Future Enhancements

- **AI-Powered Analysis**: Enhanced content analysis using OpenAI
- **Content Filtering**: Filter URLs by content type before processing
- **Custom Extractors**: Domain-specific content extraction patterns
- **Real-time Monitoring**: Dashboard for pipeline monitoring
- **Export Formats**: Additional output formats (JSON, XML, etc.)

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review Modal logs for detailed error information
3. Test individual components using the test suite