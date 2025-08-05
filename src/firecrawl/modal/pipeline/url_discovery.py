#!/usr/bin/env python3
"""
Stage 1: S3-based Discovery Orchestrator for Modal Pipeline  
Discovers all product URLs and writes them to S3 CSV
"""

import time
import pandas as pd
from typing import List
from bs4 import BeautifulSoup
import requests

from .config import app, image, secrets
from .schemas import DiscoveryJob
from .s3_utils import S3Manager

@app.function(
    image=image,
    secrets=secrets,
    timeout=1800  # 30 minutes for discovery
)
def stage1_discovery_orchestrator(discovery_job: DiscoveryJob) -> dict:
    """
    Stage 1: S3-based Discovery Orchestrator
    Discovers all product URLs from multiple sites and saves to S3
    
    Returns: {
        'execution_id': str,
        'discovered_urls': int,
        'discovery_csv_path': str,
        'discovery_time': float,
        'status': 'completed' | 'failed'
    }
    """
    start_time = time.time()
    
    print(f"🚀 STAGE 1: S3-based Discovery Orchestrator")
    print(f"📋 Execution ID: {discovery_job.execution_id}")
    print(f"🌍 Environment: {discovery_job.environment}")
    print(f"🎯 Sites: {len(discovery_job.base_urls)}")
    
    try:
        # Discover products from all base URLs
        all_discovered_urls = []
        
        for site_url in discovery_job.base_urls:
            print(f"\n🔍 Discovering products from: {site_url}")
            site_urls = _discover_products_from_single_site(site_url, discovery_job.max_products)
            all_discovered_urls.extend(site_urls)
            print(f"   ✅ Found {len(site_urls)} URLs")
        
        print(f"\n📊 Total discovered URLs: {len(all_discovered_urls)}")
        
        # Apply max_products limit if specified
        if discovery_job.max_products and len(all_discovered_urls) > discovery_job.max_products:
            all_discovered_urls = all_discovered_urls[:discovery_job.max_products]
            print(f"⚠️  Limited to {discovery_job.max_products} products")
        
        # Create DataFrame
        df = pd.DataFrame(all_discovered_urls)
        
        # Save to S3
        s3_manager = S3Manager()
        discovery_csv_path = s3_manager.build_s3_path(
            discovery_job.environment, discovery_job.execution_id, "discovery", "discovered_urls.csv"
        )
        
        success = s3_manager.upload_dataframe(df, discovery_csv_path)
        
        if success:
            discovery_time = time.time() - start_time
            print(f"✅ Discovery complete: {len(df)} URLs saved to S3")
            print(f"📁 S3 path: {discovery_csv_path}")
            print(f"⏱️  Discovery time: {discovery_time:.1f} seconds")
            
            return {
                'execution_id': discovery_job.execution_id,
                'environment': discovery_job.environment,
                'discovered_urls': len(df),
                'discovery_csv_path': discovery_csv_path,
                'discovery_time': discovery_time,
                'status': 'completed'
            }
        else:
            raise Exception("Failed to save discovery results to S3")
            
    except Exception as e:
        discovery_time = time.time() - start_time
        print(f"❌ Discovery failed: {str(e)}")
        
        return {
            'execution_id': discovery_job.execution_id,
            'environment': discovery_job.environment,
            'discovered_urls': 0,
            'discovery_csv_path': None,
            'discovery_time': discovery_time,
            'status': 'failed',
            'error': str(e)
        }

def _discover_products_from_single_site(site_url: str, max_products: int = None) -> List[dict]:
    """Discover products from a single site using simple scraping"""
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(site_url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Simple product link detection
        product_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text().strip()
            
            # Basic product detection heuristics
            if any(word in href.lower() for word in ['product', 'item', 'shop', 'buy', 'p/']):
                if href.startswith('/'):
                    href = site_url.rstrip('/') + href
                elif not href.startswith('http'):
                    continue
                    
                estimated_name = text[:100] if text else href.split('/')[-1].replace('-', ' ').title()
                
                product_links.append({
                    'url': href,
                    'estimated_name': estimated_name,
                    'discovered_from': site_url
                })
                
                if max_products and len(product_links) >= max_products:
                    break
        
        return product_links
        
    except Exception as e:
        print(f"   ❌ Error discovering from {site_url}: {str(e)}")
        return []