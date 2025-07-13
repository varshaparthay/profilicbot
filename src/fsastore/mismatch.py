import csv

# Load the 183 products
with open("tag_mismatches.txt", encoding="utf-8") as f:
    bad_products = set(line.strip() for line in f if line.strip())

# Read the CSV into a dict for quick lookup
product_data = {}
with open("/Users/varsha/src/projects/poc_eligiblity/fsa_products.csv", newline='', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        product = row["Product name"].strip()
        website = row["Website"].strip()
        eligibility = row["Eligibility"].strip()
        product_data[product] = (website, eligibility)

# Rerun prompt for each bad product
from openai import OpenAI
client = OpenAI()
PROMPT_ID = "pmpt_6872a5db71488195a758c56427755ded0c23a626ec4923e8"

output_lines = []

for product in bad_products:
    website, eligibility = product_data.get(product, ("", ""))
    if not website or not eligibility:
        print(f"Warning: Info missing for {product}")
        continue

    # Build the input string (customize as per your prompt template!)
    input_str = (
        f"Product: {product}\n"
        f"Website: {website}\n"
        f"Eligibility label: {eligibility}\n"
        "Strictly use the eligibility label exactly as given, no exceptions. "
    )

    try:
        print(f"Calling prompt for: {product}")
        response = client.responses.create(
            prompt={
                "id": PROMPT_ID,
                "version": "6"
            },
            input=input_str
        )
        openai_output = response.output[-1].content[0].text

    except Exception as e:
        openai_output = f"ERROR: {e}"

    output_lines.append(f"{product}\n{eligibility}\n{openai_output}\n\n")

with open("openai_answers_rerun.txt", "w", encoding="utf-8") as txtfile:
    txtfile.writelines(output_lines)

print("Done! Output saved to openai_answers_rerun.txt")
