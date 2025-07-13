from openai import OpenAI
import csv
import time

client = OpenAI()

PROMPT_ID = "pmpt_6872a5db71488195a758c56427755ded0c23a626ec4923e8"

output_lines = []

with open("/Users/varsha/src/projects/poc_eligiblity/fsa_products.csv", newline='', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        website = row["Website"]
        product = row["Product name"]
        eligibility = row["Eligibility"]
        print(f"Calling prompt for: {website}")

        input_str = (
            f"website: {website}\n"
            f"Eligibility label: {eligibility}\n"
        )

        # Pass product and eligibility as prompt variables/inputs
        try:
            response = client.responses.create(
                prompt={
                    "id": PROMPT_ID,
                    "version": "6"
                },
                input=website,
            )
            openai_output = response.output[-1].content[0].text
        except Exception as e:
            openai_output = f"ERROR: {e}"

        output_lines.append(f"{product}\n{eligibility}\n{openai_output}\n\n")
        # (Optional) Rate limit to avoid hammering API
        time.sleep(1)

with open("openai_answers.txt", "w", encoding="utf-8") as txtfile:
    txtfile.writelines('\n'.join(output_lines))

print("Done! Output saved to openai_answers.txt")
