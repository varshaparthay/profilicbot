#!/usr/bin/env python3
"""
Data Schemas for Modal Orchestrated Pipeline
Defines all data structures used between stages
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

# =====================================================
# JOB DATA STRUCTURES
# =====================================================

@dataclass
class DiscoveryJob:
    """Job for S3-based URL discovery stage"""
    base_urls: List[str]
    execution_id: str
    environment: str
    max_products: Optional[int] = None

@dataclass
class ProductURL:
    """Discovered product URL"""
    url: str
    batch_id: str
    discovery_method: str
    estimated_name: str

@dataclass
class ExtractedProduct:
    """Extracted product data from Firecrawl"""
    url: str
    batch_id: str
    name: str
    description: str
    structured_data: Dict[str, Any]
    extraction_time: float

@dataclass
class CategorizedProduct:
    """Product with category classification"""
    url: str
    batch_id: str
    name: str
    description: str
    structured_data: Dict[str, Any]
    extraction_time: float
    # Categorization fields
    primary_category: str
    secondary_category: str
    hsa_fsa_likelihood: str  # "high", "medium", "low", "excluded"
    category_confidence: float
    classification_priority: int  # 1=high priority, 5=skip
    categorization_time: float

@dataclass
class ClassifiedProduct:
    """Product with HSA/FSA classification"""
    url: str
    batch_id: str
    name: str
    description: str
    structured_data: Dict[str, Any]
    extraction_time: float
    primary_category: str
    secondary_category: str
    hsa_fsa_likelihood: str
    category_confidence: float
    # Classification fields
    eligibility_status: str
    eligibility_rationale: str
    classification_time: float
    total_processing_time: float

@dataclass
class TurbopufferProduct:
    """Product uploaded to Turbopuffer"""
    url: str
    batch_id: str
    name: str
    description: str
    structured_data: Dict[str, Any]
    extraction_time: float
    primary_category: str
    secondary_category: str
    eligibility_status: str
    eligibility_rationale: str
    classification_time: float
    # Turbopuffer fields
    turbopuffer_id: str
    embedding_vector: List[float]
    namespace: str
    upload_timestamp: float
    upload_success: bool
    total_processing_time: float

# =====================================================
# PYDANTIC SCHEMAS FOR EXTRACTION
# =====================================================

class ProductExtractionSchema(BaseModel):
    """Schema for Firecrawl structured extraction"""
    name: str = Field(description="Product name or title")
    description: str = Field(description="Extremely detailed product description including ALL features, benefits, usage instructions, ingredients/components, technical specifications, and any medical/health/therapeutic claims")
    price: Optional[str] = Field(description="Product price if available")
    brand: Optional[str] = Field(description="Brand name")
    ingredients: Optional[str] = Field(description="Complete list of ingredients, components, materials")
    features: Optional[str] = Field(description="Comprehensive list of ALL key features, benefits, capabilities")
    usage: Optional[str] = Field(description="Detailed instructions on how to use the product")
    specifications: Optional[str] = Field(description="Complete technical specifications, dimensions, weight, compatibility")
    medical_claims: Optional[str] = Field(description="ALL health, medical, therapeutic, wellness, fitness claims")
    category: Optional[str] = Field(description="Product category or type")

# =====================================================
# CATEGORY DEFINITIONS
# =====================================================

PRODUCT_CATEGORIES = {
    "PRIMARY": {
        "skincare": {
            "keywords": ["cream", "serum", "moisturizer", "cleanser", "acne", "anti-aging", "sunscreen", "treatment"],
            "hsa_fsa_likelihood": "high",
            "priority": 1
        },
        "supplements": {
            "keywords": ["vitamin", "mineral", "supplement", "probiotic", "omega", "calcium", "iron"],
            "hsa_fsa_likelihood": "high", 
            "priority": 1
        },
        "medical_devices": {
            "keywords": ["monitor", "thermometer", "blood pressure", "glucose", "tens", "massager"],
            "hsa_fsa_likelihood": "high",
            "priority": 1
        },
        "first_aid": {
            "keywords": ["bandage", "antiseptic", "pain relief", "aspirin", "ibuprofen", "wound care"],
            "hsa_fsa_likelihood": "high",
            "priority": 1
        },
        "vision_care": {
            "keywords": ["glasses", "reading glasses", "contact lens", "eye drops", "vision"],
            "hsa_fsa_likelihood": "high",
            "priority": 1
        },
        "oral_care": {
            "keywords": ["toothbrush", "whitening", "dental", "oral", "teeth", "gum"],
            "hsa_fsa_likelihood": "medium",
            "priority": 2
        }
    },
    "SECONDARY": {
        "beauty": {
            "keywords": ["makeup", "cosmetic", "lipstick", "foundation", "mascara"],
            "hsa_fsa_likelihood": "low",
            "priority": 3
        },
        "fitness": {
            "keywords": ["recovery", "muscle", "therapy", "foam roller", "compression"],
            "hsa_fsa_likelihood": "medium",
            "priority": 2
        },
        "baby_care": {
            "keywords": ["baby", "infant", "formula", "diaper", "pediatric"],
            "hsa_fsa_likelihood": "medium",
            "priority": 2
        }
    },
    "EXCLUDED": {
        "clothing": {
            "keywords": ["shirt", "pants", "dress", "fashion", "apparel", "clothing"],
            "hsa_fsa_likelihood": "excluded",
            "priority": 5
        },
        "electronics": {
            "keywords": ["phone", "computer", "headphones", "speaker", "gaming"],
            "hsa_fsa_likelihood": "excluded",
            "priority": 5
        },
        "food": {
            "keywords": ["snack", "beverage", "candy", "chocolate", "cookie"],
            "hsa_fsa_likelihood": "excluded",
            "priority": 5
        },
        "home": {
            "keywords": ["furniture", "decoration", "kitchen", "bedding", "lighting"],
            "hsa_fsa_likelihood": "excluded",
            "priority": 5
        }
    }
}

# Keywords that indicate medical/therapeutic benefits
MEDICAL_INDICATORS = [
    "FDA approved", "clinically proven", "therapeutic", "medical grade",
    "doctor recommended", "prescription", "treatment", "therapy",
    "relief", "healing", "pain", "inflammation", "infection"
]

# Keywords that exclude HSA/FSA eligibility  
EXCLUSION_INDICATORS = [
    "fashion", "style", "trendy", "decorative", "cosmetic only",
    "recreational", "entertainment", "gaming", "luxury"
]