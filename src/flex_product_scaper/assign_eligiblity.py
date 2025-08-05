import os
import json
import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pydantic import BaseModel, validator
from dotenv import load_dotenv
try:
    import tiktoken
except ImportError:
    tiktoken = None

# Load environment variables
load_dotenv()


class ProductClassifierError(Exception):
    pass


OPENAI_MODEL = "gpt-4o-mini"


# --- Request & Response Structures ---
class NewProductClassifierRequest(BaseModel):
    name: str
    description: str

    @validator('name', 'description')
    def must_not_be_empty(cls, v):
        if not v.strip():
            raise ProductClassifierError("Must provide both product name and description")
        return v


class ClassifierResponse(BaseModel):
    eligibilityStatus: str
    explanation: str
    additionalConsiderations: str
    lmnQualificationProbability: str
    confidencePercentage: int


class OpenAIMessage(BaseModel):
    role: str
    content: Optional[str]


class OpenAIRequest(BaseModel):
    model: str
    messages: List[OpenAIMessage]


class OpenAIChoice(BaseModel):
    message: OpenAIMessage
    finish_reason: Optional[str]


class OpenAIResponse(BaseModel):
    choices: List[OpenAIChoice]


# --- Classifier Logic ---
class Classifier:
    def __init__(self, api_key: str, client: Optional[requests.Session] = None):
        self.api_key = api_key
        self.client = client or requests.Session()
        self.base_url = os.getenv("OPENAI_API_BASE", "https://api.openai.com")

    def classify(self, requests: List[NewProductClassifierRequest]) -> List[ClassifierResponse]:
        results = []
        for req in requests:
            result = self.classify_single(req)
            results.append(result)
        return results

    def classify_single(self, param: NewProductClassifierRequest) -> ClassifierResponse:
        prompt = self.build_prompt(param)
        
        # Count tokens if tiktoken is available
        if tiktoken:
            try:
                encoding = tiktoken.encoding_for_model(OPENAI_MODEL)
                token_count = len(encoding.encode(prompt))
                print(f"📊 Token count for '{param.name}': {token_count:,} tokens")
            except Exception as e:
                print(f"⚠️  Could not count tokens: {e}")
        else:
            # Rough estimation: ~4 characters per token
            estimated_tokens = len(prompt) // 4
            print(f"📊 Estimated tokens for '{param.name}': {estimated_tokens:,} tokens (rough estimate)")

        openai_request = OpenAIRequest(
            model=OPENAI_MODEL,
            messages=[OpenAIMessage(role="user", content=prompt)]
        )

        response = self.client.post(
            f"{self.base_url}/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json=openai_request.model_dump()
        )

        if not response.ok:
            raise ProductClassifierError(
                f"OpenAI API error: {response.status_code} - {response.text}"
            )

        openai_response = OpenAIResponse(**response.json())

        first_message = openai_response.choices[0].message.content
        if not first_message:
            raise ProductClassifierError("No content in OpenAI response")

        return self.parse_response(first_message)

    def build_prompt(self, param: NewProductClassifierRequest) -> str:
        prompt_template = self._load_file("/Users/varsha/src/profilicbot/src/prompts/feligibity.txt")
        guide_content = self._load_file("/Users/varsha/src/profilicbot/src/prompts/flex_product_guide.txt")

        prompt = prompt_template.replace("{{Flex Product Guide}}", guide_content)
        prompt = prompt.replace("{{PRODUCT_NAME}}", param.name)
        prompt = prompt.replace("{{PRODUCT_DESCRIPTION}}", param.description)

        return prompt

    def _load_file(self, path: str) -> str:
        try:
            with open(path, "r", encoding="utf-8") as file:
                return file.read()
        except Exception as e:
            raise ProductClassifierError(f"Failed to load prompt file {path}: {e}")

    def parse_response(self, raw: str) -> ClassifierResponse:
        try:
            if "```json" in raw:
                start = raw.find("```json") + 7
                end = raw.find("```", start)
                json_str = raw[start:end].strip()
            elif '{' in raw:
                json_str = raw[raw.find('{'): raw.rfind('}') + 1]
            else:
                json_str = raw.strip()

            parsed = json.loads(json_str)
            return ClassifierResponse(**parsed)
        except Exception as e:
            raise ProductClassifierError(
                f"Failed to parse JSON response: {e}. Raw response: {raw}"
            )


def process_single_product(product_data: tuple, classifier: Classifier) -> dict:
    """Process a single product row - designed for parallel execution"""
    index, row_dict = product_data
    name = row_dict['name']
    
    # Try feligibot_description first, then fall back to description column
    feligibot_description = row_dict.get('feligibot_description', '')
    if pd.isna(feligibot_description) or not feligibot_description or str(feligibot_description).strip() == '':
        feligibot_description = row_dict.get('description', '')
    
    try:
        # Skip if no description available - directly assign not_eligible
        if pd.isna(feligibot_description) or not feligibot_description or str(feligibot_description).strip() == '':
            feligibot_answers = json.dumps({
                'eligibilityStatus': 'not_eligible',
                'explanation': 'No product description available for analysis',
                'additionalConsiderations': 'Product requires detailed description for HSA/FSA eligibility assessment',
                'lmnQualificationProbability': 'low',
                'confidencePercentage': 100
            }, ensure_ascii=False)
            feligibot_eligibility = 'not_eligible'
        else:
            # Create classification request
            request = NewProductClassifierRequest(
                name=name,
                description=str(feligibot_description)
            )
            
            # Classify the product
            result = classifier.classify_single(request)
            
            # Convert result to JSON string
            feligibot_answers = json.dumps({
                'eligibilityStatus': result.eligibilityStatus,
                'explanation': result.explanation,
                'additionalConsiderations': result.additionalConsiderations,
                'lmnQualificationProbability': result.lmnQualificationProbability,
                'confidencePercentage': result.confidencePercentage
            }, ensure_ascii=False)
            
            # Extract just the eligibility status for separate column
            feligibot_eligibility = result.eligibilityStatus
            
    except Exception as e:
        print(f"Error processing {name}: {e}")
        feligibot_answers = None
        feligibot_eligibility = None
    
    # Create result with all original columns plus feligibot_answers and feligibot_eligibility
    result_dict = row_dict.copy()
    result_dict['index'] = index
    result_dict['feligibot_answers'] = feligibot_answers
    result_dict['feligibot_eligibility'] = feligibot_eligibility
    
    return result_dict


def process_csv(input_file: str, output_file: str = None, max_workers: int = 5, batch_size: int = 50) -> None:
    """Process CSV file with parallel HSA/FSA eligibility classification"""
    # Read input CSV
    try:
        df = pd.read_csv(input_file)
        if 'name' not in df.columns:
            raise ValueError("CSV must contain 'name' column")
        if 'feligibot_description' not in df.columns:
            if 'description' in df.columns:
                print("Warning: 'feligibot_description' column not found, using 'description' column instead")
            else:
                print("Warning: Neither 'feligibot_description' nor 'description' columns found, will use empty descriptions")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Set default output file name
    if not output_file:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_with_eligibility.csv"
    
    # Initialize classifier
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY not found in environment variables")
        return
    
    classifier = Classifier(api_key=api_key)
    
    # Setup progress tracking files
    temp_output = f"{os.path.splitext(output_file)[0]}_temp.csv"
    progress_file = f"{os.path.splitext(output_file)[0]}_progress.txt"
    
    # Check for existing progress
    start_index = 0
    results = []
    if os.path.exists(temp_output) and os.path.exists(progress_file):
        try:
            existing_df = pd.read_csv(temp_output)
            results = existing_df.to_dict('records')
            with open(progress_file, 'r') as f:
                start_index = int(f.read().strip())
            print(f"Resuming from index {start_index} (found {len(results)} existing results)")
        except Exception as e:
            print(f"Could not resume from previous run: {e}")
            start_index = 0
            results = []
    
    print(f"Processing {len(df)} products with {max_workers} parallel workers...")
    
    # Prepare data for parallel processing
    remaining_data = [(index, row.to_dict()) for index, row in df.iloc[start_index:].iterrows()]
    
    try:
        # Process in parallel with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_data = {
                executor.submit(process_single_product, product_data, classifier): product_data 
                for product_data in remaining_data
            }
            
            completed_count = start_index
            batch_results = []
            
            # Process completed tasks
            for future in as_completed(future_to_data):
                product_data = future_to_data[future]
                try:
                    result = future.result()
                    batch_results.append(result)
                    completed_count += 1
                    
                    
                    # Log result
                    if result['feligibot_answers']:
                        # Check if it was classified or marked as not_eligible due to no description
                        answers = json.loads(result['feligibot_answers'])
                        if answers['eligibilityStatus'] == 'not_eligible' and 'No product description' in answers['explanation']:
                            print(f"[{completed_count}/{len(df)}] Not eligible (no description): {result['name']}")
                        else:
                            print(f"[{completed_count}/{len(df)}] Classified: {result['name']} -> {answers['eligibilityStatus']}")
                    else:
                        print(f"[{completed_count}/{len(df)}] Error: {result['name']}")
                    
                    # Save progress in batches
                    if len(batch_results) >= batch_size:
                        # Sort by original index to maintain order
                        batch_results.sort(key=lambda x: x['index'])
                        
                        # Add to results (remove index field)
                        for res in batch_results:
                            result_dict = res.copy()
                            result_dict.pop('index', None)
                            results.append(result_dict)
                        
                        # Save intermediate results
                        temp_df = pd.DataFrame(results)
                        temp_df.to_csv(temp_output, index=False)
                        
                        # Update progress
                        with open(progress_file, 'w') as f:
                            f.write(str(completed_count))
                        
                        print(f"Progress saved: {completed_count}/{len(df)} completed")
                        batch_results = []
                        
                except Exception as e:
                    print(f"Error processing {product_data[1]['name']}: {e}")
                    completed_count += 1
            
            # Handle remaining batch results
            if batch_results:
                batch_results.sort(key=lambda x: x['index'])
                for res in batch_results:
                    result_dict = res.copy()
                    result_dict.pop('index', None)
                    results.append(result_dict)
    
    except Exception as e:
        print(f"\nError during parallel processing: {e}")
        print(f"Progress saved in {temp_output}")
        return
    
    # Sort final results by original index to maintain order (if index exists)
    if results and 'index' in results[0]:
        results.sort(key=lambda x: x['index'])
    
    # Remove the index field before saving final output (if any remaining)
    for result in results:
        result.pop('index', None)
    
    # Save final results
    output_df = pd.DataFrame(results)
    output_df.to_csv(output_file, index=False)
    print(f"\nFinal results saved to: {output_file}")
    
    # Clean up temporary files
    for temp_file in [temp_output, progress_file]:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    # Show summary with breakdown
    total_count = len(results)
    processed_count = output_df['feligibot_answers'].notna().sum()
    
    # Count different eligibility statuses
    eligible_count = 0
    not_eligible_count = 0
    no_description_count = 0
    
    for _, row in output_df.iterrows():
        if pd.notna(row['feligibot_answers']):
            try:
                answers = json.loads(row['feligibot_answers'])
                if answers['eligibilityStatus'] == 'not_eligible' and 'No product description' in answers['explanation']:
                    no_description_count += 1
                elif answers['eligibilityStatus'] == 'not_eligible':
                    not_eligible_count += 1
                else:
                    eligible_count += 1
            except:
                pass
    
    print(f"\nSummary:")
    print(f"Total products: {total_count}")
    print(f"Successfully processed: {processed_count}")
    print(f"Eligible/Potentially eligible: {eligible_count}")
    print(f"Not eligible (analyzed): {not_eligible_count}")
    print(f"Not eligible (no description): {no_description_count}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) >= 2:
        # CSV mode: python assign_eligiblity.py <input_csv> [output_csv] [max_workers]
        input_csv = sys.argv[1]
        output_csv = sys.argv[2] if len(sys.argv) > 2 else None
        max_workers = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        print(f"Processing CSV file: {input_csv} with {max_workers} workers")
        process_csv(input_csv, output_csv, max_workers)
    else:
        print("Usage: python assign_eligiblity.py <input_csv> [output_csv] [max_workers]")
        print("CSV must contain 'name' column and optionally 'feligibot_description' or 'description' column")
