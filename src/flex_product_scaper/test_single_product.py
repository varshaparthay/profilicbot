#!/usr/bin/env python3

import os
from dotenv import load_dotenv
from assign_eligiblity import Classifier, NewProductClassifierRequest

# Load environment variables
load_dotenv()

def test_single_product():
    """Test HSA/FSA classification with a single product to see the full prompt"""
    
    # Initialize classifier
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY not found in environment variables")
        return
    
    classifier = Classifier(api_key=api_key)
    
    # Test with a sample product (you can change these)
    test_name = "EyeCare Max Pro LED Device"
    test_description = '{"name": "DRx SpectraLite™ EyeCare Max Pro LED Device", "description": "The DRx SpectraLite™ EyeCare Max Pro is a professional-grade, FDA-cleared LED device designed for the eye area. It features 96 targeted red LED lights that visibly firm skin, reduce wrinkles and fine lines, and diminish puffiness and dark circles. The device utilizes 4 wavelengths of red light to stimulate collagen production, improving skin density and even skin tone around the eyes. It is suitable for all skin types and requires only 3 minutes of use per day. Included with the device are a USB charging cord, a storage bag, and a user manual. Clinical studies show that 97% of users experienced visible improvements in fine lines, wrinkles, and skin tone after 10 weeks of use.", "ingredients": ["Alpha Hydroxy Acids", "Hyaluronic Acid", "Lactic Acid", "Niacinamide", "Retinol", "Salicylic Acid", "Vitamin C"], "modeOfUse": "On clean, dry skin, place the device comfortably over the eye area. Power on the device and use for the programmed treatment time of 3 minutes. Once the device automatically shuts off, remove it and follow with your skincare routine.", "treatedConditions": ["Wrinkles", "Fine Lines", "Puffiness", "Dark Circles"], "symptoms": ["Aging skin", "Loss of skin elasticity", "Uneven skin tone"], "diagnosticUse": "None"}'

    print(f"Testing product: {test_name}")
    print(f"Description: {test_description}")
    print()
    
    try:
        # Create classification request
        request = NewProductClassifierRequest(
            name=test_name,
            description=test_description
        )
        
        # Classify the product (this will show the full prompt)
        result = classifier.classify_single(request)
        
        print("CLASSIFICATION RESULT:")
        print(f"Eligibility Status: {result.eligibilityStatus}")
        print(f"Explanation: {result.explanation}")
        print(f"Additional Considerations: {result.additionalConsiderations}")
        print(f"LMN Qualification Probability: {result.lmnQualificationProbability}")
        print(f"Confidence Percentage: {result.confidencePercentage}")
        
    except Exception as e:
        print(f"Error during classification: {e}")

if __name__ == "__main__":
    test_single_product()