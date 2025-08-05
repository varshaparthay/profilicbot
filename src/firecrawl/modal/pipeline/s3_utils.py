#!/usr/bin/env python3
"""
S3 Utilities and Dynamic Batching for Modal Pipeline
Common functions for S3-based processing across all stages
"""

import boto3
import pandas as pd
import json
import uuid
import time
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
import os

@dataclass
class BatchReference:
    """Reference to a batch stored in S3"""
    execution_id: str
    stage: str
    batch_number: int
    item_count: int
    s3_input_path: str
    s3_output_path: str
    environment: str

class S3Manager:
    """Centralized S3 operations for the pipeline"""
    
    def __init__(self):
        # AWS credentials are automatically loaded from environment variables
        # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        self.bucket = "flex-ai"
    
    def build_s3_path(self, environment: str, execution_id: str, stage: str, filename: str) -> str:
        """Build standardized S3 path"""
        return f"s3://{self.bucket}/{environment}/{execution_id}/{stage}/{filename}"
    
    def upload_dataframe(self, df: pd.DataFrame, s3_path: str) -> bool:
        """Upload pandas DataFrame to S3 as CSV"""
        try:
            # Parse S3 path
            path_parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1]
            
            # Upload CSV
            csv_buffer = df.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=csv_buffer.encode('utf-8'),
                ContentType='text/csv'
            )
            return True
        except Exception as e:
            print(f"❌ Error uploading to S3: {str(e)}")
            return False
    
    def download_dataframe(self, s3_path: str) -> pd.DataFrame:
        """Download CSV from S3 as pandas DataFrame"""
        try:
            # Parse S3 path
            path_parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1]
            
            # Download and parse CSV
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            print(f"❌ Error downloading from S3: {str(e)}")
            return pd.DataFrame()
    
    def upload_json(self, data: Any, s3_path: str) -> bool:
        """Upload JSON data to S3"""
        try:
            # Parse S3 path
            path_parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1]
            
            # Upload JSON
            json_data = json.dumps(data, indent=2, default=str)
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"❌ Error uploading JSON to S3: {str(e)}")
            return False
    
    def download_json(self, s3_path: str) -> Any:
        """Download JSON from S3"""
        try:
            # Parse S3 path
            path_parts = s3_path.replace("s3://", "").split("/", 1)
            bucket = path_parts[0]  
            key = path_parts[1]
            
            # Download and parse JSON
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            return data
        except Exception as e:
            print(f"❌ Error downloading JSON from S3: {str(e)}")
            return None

def calculate_optimal_batching(total_items: int, processing_time_per_item: float = 1.0) -> Tuple[int, int]:
    """
    Calculate optimal batch size and worker count based on total items
    
    Args:
        total_items: Total number of items to process
        processing_time_per_item: Average processing time per item in minutes
    
    Returns:
        (batch_size, max_workers) tuple
    """
    
    if total_items <= 1000:
        # Small jobs: 25 items/batch, up to 10 workers
        # Example: 500 items = 20 batches, 10 workers, ~25 min
        return 25, min(10, max(1, total_items // 25))
    
    elif total_items <= 10000:
        # Medium jobs: 25 items/batch, up to 30 workers  
        # Example: 5000 items = 200 batches, 30 workers, ~25 min
        return 25, min(30, max(1, total_items // 25))
    
    elif total_items <= 50000:
        # Large jobs: 50 items/batch, up to 50 workers
        # Example: 25000 items = 500 batches, 50 workers, ~50 min
        return 50, min(50, max(1, total_items // 50))
    
    else:
        # Very large jobs: 100 items/batch, up to 100 workers
        # Example: 100000 items = 1000 batches, 100 workers, ~100 min
        return 100, min(100, max(1, total_items // 100))

def create_dynamic_batches(
    df: pd.DataFrame, 
    execution_id: str, 
    stage: str, 
    environment: str,
    processing_time_per_item: float = 1.0
) -> List[BatchReference]:
    """
    Create dynamic batches from DataFrame and return batch references
    
    Args:
        df: Input DataFrame to batch
        execution_id: Unique execution ID
        stage: Processing stage name
        environment: dev/prod environment
        processing_time_per_item: Processing time per item (for optimization)
    
    Returns:
        List of BatchReference objects
    """
    
    total_items = len(df)
    batch_size, max_workers = calculate_optimal_batching(total_items, processing_time_per_item)
    
    print(f"📊 Dynamic Batching Strategy:")
    print(f"   Total items: {total_items:,}")
    print(f"   Batch size: {batch_size}")
    print(f"   Max workers: {max_workers}")
    print(f"   Estimated batches: {(total_items + batch_size - 1) // batch_size}")
    
    s3_manager = S3Manager()
    batch_references = []
    
    # Create batches
    for batch_num in range(0, total_items, batch_size):
        batch_df = df.iloc[batch_num:batch_num + batch_size].copy()
        
        if len(batch_df) == 0:
            continue
            
        # Create batch reference
        batch_ref = BatchReference(
            execution_id=execution_id,
            stage=stage,
            batch_number=len(batch_references),
            item_count=len(batch_df),
            s3_input_path=s3_manager.build_s3_path(
                environment, execution_id, stage, f"batch_{len(batch_references)}_input.csv"
            ),
            s3_output_path=s3_manager.build_s3_path(
                environment, execution_id, stage, f"batch_{len(batch_references)}_output.csv"
            ),
            environment=environment
        )
        
        # Upload batch to S3
        success = s3_manager.upload_dataframe(batch_df, batch_ref.s3_input_path)
        if success:
            batch_references.append(batch_ref)
            print(f"   ✅ Created batch {batch_ref.batch_number}: {batch_ref.item_count} items")
        else:
            print(f"   ❌ Failed to create batch {len(batch_references)}")
    
    print(f"📤 Created {len(batch_references)} batches for processing")
    return batch_references

def combine_batch_results(
    batch_references: List[BatchReference],
    output_s3_path: str
) -> pd.DataFrame:
    """
    Combine results from all batches into a single CSV
    
    Args:
        batch_references: List of batch references
        output_s3_path: S3 path for combined results
        
    Returns:
        Combined DataFrame
    """
    
    s3_manager = S3Manager()
    combined_dfs = []
    
    print(f"🔄 Combining {len(batch_references)} batch results...")
    
    for batch_ref in batch_references:
        try:
            batch_df = s3_manager.download_dataframe(batch_ref.s3_output_path)
            if len(batch_df) > 0:
                combined_dfs.append(batch_df)
                print(f"   ✅ Combined batch {batch_ref.batch_number}: {len(batch_df)} items")
            else:
                print(f"   ⚠️  Empty batch {batch_ref.batch_number}")
        except Exception as e:
            print(f"   ❌ Failed to combine batch {batch_ref.batch_number}: {str(e)}")
    
    if combined_dfs:
        final_df = pd.concat(combined_dfs, ignore_index=True)
        
        # Upload combined results
        success = s3_manager.upload_dataframe(final_df, output_s3_path)
        if success:
            print(f"✅ Combined results uploaded to: {output_s3_path}")
            print(f"📊 Total items: {len(final_df):,}")
        else:
            print(f"❌ Failed to upload combined results")
            
        return final_df
    else:
        print(f"❌ No batch results to combine")
        return pd.DataFrame()

def generate_execution_id() -> str:
    """Generate unique execution ID for pipeline run"""
    timestamp = int(time.time())
    unique_id = str(uuid.uuid4())[:8]
    return f"exec_{timestamp}_{unique_id}"

def create_stage_folder_structure(execution_id: str, environment: str) -> Dict[str, str]:
    """
    Create S3 folder structure for all pipeline stages
    
    Returns:
        Dictionary of stage names to S3 paths
    """
    
    s3_manager = S3Manager()
    stages = [
        "discovery", "extraction", "categorization", 
        "classification", "turbopuffer", "results", "metadata"
    ]
    
    stage_paths = {}
    for stage in stages:
        stage_path = s3_manager.build_s3_path(environment, execution_id, stage, "")
        stage_paths[stage] = stage_path.rstrip("/")
    
    return stage_paths

def test_s3_connection() -> bool:
    """
    Test S3 connection and permissions
    
    Returns:
        True if connection successful, False otherwise
    """
    
    try:
        s3_manager = S3Manager()
        
        print(f"🔍 Testing S3 connection...")
        print(f"🪣 Bucket: {s3_manager.bucket}")
        print(f"🌍 Region: {os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')}")
        
        # Test bucket access
        response = s3_manager.s3_client.head_bucket(Bucket=s3_manager.bucket)
        print(f"✅ Bucket access successful")
        
        # Test write permissions with a small test file
        test_df = pd.DataFrame({'test': ['connection_test']})
        test_path = s3_manager.build_s3_path("dev", "test", "connection", "test.csv")
        
        upload_success = s3_manager.upload_dataframe(test_df, test_path)
        if upload_success:
            print(f"✅ Write permissions successful")
            
            # Test read permissions
            downloaded_df = s3_manager.download_dataframe(test_path)
            if len(downloaded_df) > 0:
                print(f"✅ Read permissions successful")
                
                # Clean up test file
                path_parts = test_path.replace("s3://", "").split("/", 1)
                s3_manager.s3_client.delete_object(
                    Bucket=path_parts[0], 
                    Key=path_parts[1]
                )
                print(f"🧹 Test file cleaned up")
                
                print(f"🎉 S3 connection test PASSED!")
                return True
            else:
                print(f"❌ Read test failed")
                return False
        else:
            print(f"❌ Write test failed")
            return False
            
    except Exception as e:
        print(f"❌ S3 connection test FAILED: {str(e)}")
        print(f"💡 Check your AWS credentials and bucket permissions")
        return False