def normalize_tag(tag):
    tag = tag.strip().lower()
    if tag in ["eligible w/lmn", "eligible with lmn", "letter of medical necessity"]:
        return "letter of medical necessity"
    if tag == "eligible":
        return "eligible"
    if tag == "not eligible":
        return "not eligible"
    return tag

bad_products = []

with open("openai_answers.txt", encoding="utf-8") as f:
    blocks = f.read().strip().split('\n\n')

for block in blocks:
    lines = block.strip().split('\n')
    if len(lines) < 3:
        continue  # skip incomplete blocks
    product = lines[0].strip()
    expected_tag = normalize_tag(lines[1])
    model_line = lines[2].strip()
    model_tag = normalize_tag(model_line.split(':', 1)[0])
    if expected_tag != model_tag:
        bad_products.append(product)

with open("tag_mismatches.txt", "w", encoding="utf-8") as f:
    for prod in bad_products:
        f.write(prod + "\n")

print(f"Done! {len(bad_products)} mismatches written to tag_mismatches.txt")
