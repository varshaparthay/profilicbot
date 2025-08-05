# Modal Secrets Setup Guide

This guide shows how to configure the required secrets for the S3-based product scraping pipeline.

## Required Secrets

You need to create 4 secrets in your Modal dashboard:

### 1. AWS S3 Credentials (`aws-s3-credentials`)

For S3 bucket access (`s3://flex-ai`):

```bash
modal secret create aws-s3-credentials \
  AWS_ACCESS_KEY_ID=your_aws_access_key \
  AWS_SECRET_ACCESS_KEY=your_aws_secret_key \
  AWS_DEFAULT_REGION=us-east-1
```

**Alternative: Using AWS CLI**
```bash
# If you have AWS CLI configured, you can copy from ~/.aws/credentials:
modal secret create aws-s3-credentials \
  AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id) \
  AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key) \
  AWS_DEFAULT_REGION=$(aws configure get region)
```

### 2. OpenAI API Key (`openai-api-key`)

For GPT-4o-mini categorization and classification:

```bash
modal secret create openai-api-key \
  OPENAI_API_KEY=sk-your-openai-api-key
```

### 3. Firecrawl API Key (`firecrawl-api-key`)

For product extraction:

```bash
modal secret create firecrawl-api-key \
  FIRECRAWL_API_KEY=fc-your-firecrawl-api-key
```

### 4. Turbopuffer API Key (`turbopuffer-api-key`)

For vector database uploads:

```bash
modal secret create turbopuffer-api-key \
  TURBOPUFFER_API_KEY=your-turbopuffer-api-key
```

## AWS S3 Bucket Setup

Make sure your S3 bucket has the correct structure:

```
s3://flex-ai/
├── dev/          # Development environment
└── prod/         # Production environment
```

### S3 IAM Permissions

Your AWS user needs these permissions for the `flex-ai` bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::flex-ai",
        "arn:aws:s3:::flex-ai/*"
      ]
    }
  ]
}
```

## Verification Commands

Test that your secrets are working:

### Test AWS S3 Access
```bash
modal run pipeline.s3_utils test_s3_connection
```

### Test All Services
```bash
modal run pipeline.main_orchestrator run_pipeline_test \
  --base-urls '["https://example-store.com"]' \
  --max-products 1
```

## Environment Variables in Code

The secrets are automatically available as environment variables in your Modal functions:

```python
import os

# AWS credentials (automatically used by boto3)
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION')

# API keys
openai_key = os.environ.get('OPENAI_API_KEY')
firecrawl_key = os.environ.get('FIRECRAWL_API_KEY')
turbopuffer_key = os.environ.get('TURBOPUFFER_API_KEY')
```

## Troubleshooting

### Common Issues

1. **S3 Access Denied**: Check your AWS credentials and bucket permissions
2. **OpenAI Rate Limits**: Reduce concurrency limits in worker functions
3. **Firecrawl Quota**: Monitor your Firecrawl usage limits
4. **Turbopuffer Errors**: Verify namespace and API key

### Debug Commands

```bash
# Check if secrets exist
modal secret list

# View secret details (values are hidden)
modal secret get aws-s3-credentials

# Test individual components
modal run pipeline.url_discovery stage1_url_discovery \
  --base-urls '["https://example.com"]' \
  --execution-id test123 \
  --environment dev
```

## Security Best Practices

1. **Rotate Keys Regularly**: Update API keys every 90 days
2. **Least Privilege**: Only grant necessary S3 permissions
3. **Monitor Usage**: Track API usage across all services
4. **Environment Separation**: Use different keys for dev/prod
5. **Audit Access**: Review who has access to Modal secrets

## Next Steps

After setting up secrets:

1. Test with a small dataset: `run_pipeline_test()`
2. Monitor S3 costs and usage
3. Set up CloudWatch for AWS monitoring
4. Configure alerts for API rate limits