# Modal Pipeline Testing Guide

Complete testing strategy for the 6-stage orchestrated product scraping pipeline.

## Quick Start Testing

### 1. Setup Requirements

**Required Secrets:**
```bash
# Create Modal secret with:
modal secret create scraper-secrets \
  FIRECRAWL_API_KEY=your_key_here \
  OPENAI_API_KEY=your_key_here \
  TURBOPUFFER_API_KEY=your_key_here
```

**Required Files:**
- Ensure `src/prompts/feligibity.txt` exists with your HSA/FSA prompt
- Ensure `src/prompts/flex_product_guide.txt` exists (optional)

### 2. Run Comprehensive Tests

```bash
# Run all tests
modal run src.firecrawl.modal.tests.test_pipeline

# Test specific stage
modal run src.firecrawl.modal.tests.test_pipeline --test-type discovery

# Test full pipeline with custom URL
modal run src.firecrawl.modal.tests.test_pipeline --test-type full-pipeline --test-url https://www.dermstore.com
```

## Individual Stage Testing

### Stage 1: URL Discovery
```bash
modal run src.firecrawl.modal.tests.test_pipeline --test-type discovery --test-url https://www.goodmolecules.com
```
**Tests:**
- Sitemap discovery functionality
- Firecrawl mapping fallback
- URL filtering and deduplication
- Performance benchmarks

**Expected Results:**
- ✅ Status: completed
- ✅ URLs Found: > 0
- ✅ Time: < 5 minutes
- ✅ Methods: sitemap and/or firecrawl

### Stage 2: Product Extraction
```bash
modal run src.firecrawl.modal.tests.test_pipeline --test-type extraction
```
**Tests:**
- Firecrawl structured extraction
- Description quality (>200 chars)
- Error handling for failed URLs
- Processing speed

**Expected Results:**
- ✅ Success Rate: > 70%
- ✅ Avg Description: > 200 chars
- ✅ Time: < 10 minutes for 3 products

### Stage 3: Product Categorization
**Tests:**
- Keyword-based categorization
- Category confidence scoring
- Priority assignment logic
- Filtering effectiveness

**Expected Results:**
- ✅ All products categorized
- ✅ Variety in categories (supplements, beauty, excluded)
- ✅ Appropriate priorities (1-5 scale)

### Stage 4: HSA/FSA Classification
**Tests:**
- OpenAI API integration
- Custom prompt loading
- JSON response parsing
- Category-optimized prompts

**Expected Results:**
- ✅ All products classified
- ✅ Valid eligibility status (eligible/not_eligible/unclear)
- ✅ Detailed rationale provided

### Stage 5: Turbopuffer Upload
**Tests:**
- Vector embedding generation
- Batch upload functionality
- Search capabilities
- Error handling

**Expected Results:**
- ✅ Successful uploads
- ✅ Search functionality works
- ✅ Embeddings generated correctly

### Stage 6: Results Collection
**Tests:**
- CSV file generation
- Report statistics accuracy
- File format validation
- Processing metrics

**Expected Results:**
- ✅ Multiple output files created
- ✅ Comprehensive statistics
- ✅ Valid CSV format

## Production Testing Strategy

### Phase 1: Small Batch Testing (10-50 products)
```bash
# Test with small batch
modal run src.firecrawl.modal.pipeline.orchestration_controller \
  --website-url https://www.goodmolecules.com \
  --max-products 10
```

**Validation:**
- Check all output files are generated
- Verify HSA/FSA classifications are reasonable
- Confirm Turbopuffer uploads successful
- Review processing times per stage

### Phase 2: Medium Batch Testing (100-1000 products)
```bash
# Test with medium batch
modal run src.firecrawl.modal.pipeline.orchestration_controller \
  --website-url https://www.dermstore.com \
  --max-products 500 \
  --extraction-workers 10 \
  --classification-workers 5
```

**Validation:**
- Monitor queue depths and worker utilization
- Check cost accumulation vs estimates
- Validate error handling and retry logic
- Review categorization effectiveness

### Phase 3: Large Batch Testing (1000+ products)
```bash
# Test with large batch
modal run src.firecrawl.modal.pipeline.orchestration_controller \
  --website-url https://www.cvs.com \
  --max-products 5000 \
  --extraction-workers 30 \
  --categorization-workers 50 \
  --classification-workers 15
```

**Validation:**
- End-to-end processing time
- Cost per product analysis
- Token usage reduction from categorization
- Overall success rate (discovery → final results)

## Performance Benchmarks

### Expected Performance Targets

| Stage | Target Rate | Benchmark |
|-------|-------------|-----------|
| Discovery | 1,000+ URLs/hour | Single orchestrator |
| Extraction | 500+ products/hour | 30 workers |
| Categorization | 2,000+ products/hour | 50 workers |
| Classification | 300+ products/hour | 15 workers |
| Turbopuffer | 1,000+ uploads/hour | 10 workers |
| End-to-End | 200+ products/hour | Complete pipeline |

### Cost Benchmarks

| Volume | Expected Cost | Components |
|--------|---------------|------------|
| 100 products | $20-40 | Mostly Firecrawl |
| 1,000 products | $200-400 | Firecrawl + OpenAI |
| 10,000 products | $2,000-4,000 | All stages |

## Debugging Common Issues

### 1. Discovery Issues
```bash
# Check if website has sitemap
curl https://example.com/sitemap.xml

# Test Firecrawl directly
curl -X POST 'https://api.firecrawl.dev/v0/map' \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://example.com"}'
```

### 2. Extraction Issues
- **Empty descriptions**: Check Firecrawl response format
- **High failure rate**: Verify API key and quotas
- **Slow processing**: Check Firecrawl rate limits

### 3. Classification Issues
- **Missing rationale**: Check OpenAI JSON parsing
- **Generic responses**: Verify custom prompts loaded
- **Rate limit errors**: Reduce classification workers

### 4. Turbopuffer Issues
- **Upload failures**: Check API key and namespace
- **Search not working**: Verify embeddings generated
- **Slow uploads**: Check batch size configuration

## Monitoring and Alerts

### Key Metrics to Monitor
1. **Queue Depths**: Identify bottlenecks
2. **Error Rates**: Track failures per stage
3. **Processing Times**: Monitor performance degradation
4. **Cost Accumulation**: Track spending vs budget
5. **Success Rates**: Overall pipeline effectiveness

### Alert Thresholds
- Queue depth > 1000 items
- Error rate > 10% for any stage
- Processing time > 2x baseline
- Cost exceeding $0.50 per product
- Overall success rate < 80%

## Quality Assurance Checklist

### Before Production Deployment
- [ ] All comprehensive tests pass
- [ ] Custom prompts loaded correctly
- [ ] API keys and secrets configured
- [ ] Small batch test successful (10 products)
- [ ] Medium batch test successful (100 products)
- [ ] Cost estimates validated
- [ ] Output file formats verified
- [ ] Turbopuffer search functionality tested

### Regular Production Testing
- [ ] Weekly small batch tests on new sites
- [ ] Monthly comprehensive test suite
- [ ] Quarterly performance benchmark reviews
- [ ] Continuous monitoring of error rates and costs

## Troubleshooting Guide

### Common Error Messages

**"Firecrawl API key not available"**
- Solution: Check Modal secret configuration

**"Custom prompts not loaded"**
- Solution: Verify prompt files in `src/prompts/` directory

**"OpenAI rate limit exceeded"**
- Solution: Reduce classification workers or add delays

**"Turbopuffer upload failed"**
- Solution: Check API key and namespace permissions

**"Queue timeout"**
- Solution: Increase worker concurrency or timeout values

### Performance Optimization

1. **Slow Discovery**: Try different websites, check sitemap availability
2. **Extraction Bottleneck**: Increase extraction workers
3. **Classification Delays**: Optimize prompt length, reduce workers
4. **Upload Issues**: Adjust batch size for Turbopuffer

## Success Criteria

### Individual Stage Success
- ✅ Discovery: Finds >100 URLs in <5 minutes
- ✅ Extraction: >80% success rate with >200 char descriptions
- ✅ Categorization: All products categorized with appropriate priorities
- ✅ Classification: >95% products classified with rationale
- ✅ Turbopuffer: >95% upload success rate
- ✅ Results: Complete CSV and JSON files generated

### End-to-End Success
- ✅ Complete pipeline runs without manual intervention
- ✅ >80% products make it from discovery to final results
- ✅ Cost per product stays under $0.50
- ✅ Processing rate >200 products/hour
- ✅ HSA/FSA classifications are accurate and detailed

## Next Steps After Testing

1. **Production Deployment**: Scale up to full workloads
2. **Monitoring Setup**: Implement real-time dashboards
3. **Cost Optimization**: Fine-tune worker ratios
4. **Quality Improvements**: Refine categorization rules
5. **Feature Enhancements**: Add new capabilities based on results