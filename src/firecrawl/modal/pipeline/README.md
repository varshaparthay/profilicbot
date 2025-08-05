# Modal Orchestrated Product Scraper

A sophisticated 6-stage distributed pipeline for processing millions of e-commerce products with HSA/FSA eligibility classification.

## Architecture

```
Website URL → Discovery → Extraction → Categorization → Classification → Turbopuffer → Results
               ↓           ↓             ↓               ↓              ↓           ↓
           url_queue → product_queue → categorization_queue → classification_queue → turbopuffer_queue → CSV/JSON
```

## Pipeline Stages

1. **url_discovery.py** - Universal product URL discovery (sitemaps + Firecrawl)
2. **product_extractor.py** - Comprehensive product data extraction via Firecrawl
3. **product_categorizer.py** - Intelligent categorization for token optimization
4. **hsa_classifier.py** - HSA/FSA eligibility classification with OpenAI
5. **turbopuffer_uploader.py** - Vector database upload with embeddings
6. **results_collector.py** - Final results aggregation and reporting

## Quick Start

### Basic Usage
```bash
modal run src.firecrawl.modal.pipeline.orchestration_controller --website-url https://www.example-store.com
```

### Advanced Usage
```bash
modal run src.firecrawl.modal.pipeline.orchestration_controller \
  --website-url https://www.dermstore.com \
  --max-products 10000 \
  --extraction-workers 50 \
  --classification-workers 20
```

## Configuration

### Required Secrets
Create a Modal secret named `scraper-secrets` with:
- `FIRECRAWL_API_KEY` - Your Firecrawl API key
- `OPENAI_API_KEY` - Your OpenAI API key  
- `TURBOPUFFER_API_KEY` - Your Turbopuffer API key
- `TURBOPUFFER_NAMESPACE` - Turbopuffer namespace (optional)

### Custom Prompts
Place your custom prompts in `src/prompts/`:
- `feligibity.txt` - HSA/FSA eligibility classification prompt
- `flex_product_guide.txt` - Additional classification guidance

## Worker Scaling

Default worker configuration:
- **Discovery**: 1 orchestrator
- **Extraction**: 30 workers (Firecrawl API limits)
- **Categorization**: 50 workers (fast processing)
- **Classification**: 15 workers (OpenAI rate limits)
- **Turbopuffer**: 10 workers (batch upload optimization)
- **Results**: 1 collector

## Output Files

The pipeline generates:
- `results_{batch_id}_{timestamp}.csv` - Complete product data
- `eligible_products_{batch_id}_{timestamp}.csv` - HSA/FSA eligible products only
- `report_{batch_id}_{timestamp}.json` - Processing statistics and analysis

## Cost Optimization Features

- **Categorization Stage**: Filters out irrelevant products (60-80% token reduction)
- **Category-Specific Prompts**: Shorter prompts for different product types
- **Batch Processing**: Efficient API usage and rate limit management
- **Queue-Based Processing**: Process only what you need, pause/resume capability

## Performance Targets

- **Discovery**: 1,000+ URLs per hour
- **Extraction**: 500+ products per hour  
- **Classification**: 300+ products per hour
- **End-to-End**: 200+ products per hour fully processed

## Files Overview

- `pipeline/` - Core pipeline implementation
  - `config.py` - Modal app configuration, queues, and shared resources
  - `schemas.py` - Data structures and category definitions
  - `orchestration_controller.py` - Main pipeline orchestration
  - Individual stage files for modular processing
- `tests/` - Comprehensive testing suite and documentation

## Example Results

For 1,000 products:
- **Cost**: ~$200-400 (Firecrawl + OpenAI + Modal)
- **Time**: ~5 hours end-to-end
- **Success Rate**: >90% discovery to final results
- **HSA/FSA Eligible**: Typically 15-30% of products

## Benefits

✅ **Scalable** - Process millions of products with independent stage scaling  
✅ **Cost-Effective** - Smart categorization reduces OpenAI costs by 60-80%  
✅ **Fault-Tolerant** - Queue-based processing with automatic retry  
✅ **Real-Time** - Results available as they complete  
✅ **Searchable** - Vector database integration for semantic search  
✅ **Modular** - Individual stages can be run independently  

## Testing

### Quick Test
```bash
# Run comprehensive test suite
modal run src.firecrawl.modal.tests.test_pipeline

# Test specific stages
modal run src.firecrawl.modal.tests.test_pipeline --test-type discovery
modal run src.firecrawl.modal.tests.test_pipeline --test-type extraction
```

### Production Testing
```bash
# Small batch test (10 products)
modal run src.firecrawl.modal.pipeline.orchestration_controller \
  --website-url https://www.goodmolecules.com \
  --max-products 10
```

See `tests/TESTING_GUIDE.md` for comprehensive testing procedures.

## Development

Each stage is implemented as a separate file for maintainability:
- Easy to test individual components
- Independent scaling and optimization
- Clear separation of concerns
- Modular error handling and monitoring