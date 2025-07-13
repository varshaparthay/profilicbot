# File paths (change as needed)
master_file = "/Users/varsha/src/profilicbot/openai_answers.txt"
rerun_file = "/Users/varsha/src/profilicbot/openai_answers_rerun.txt"
mismatches_file = "/Users/varsha/src/profilicbot/tag_mismatches.txt"
final_file = "/Users/varsha/src/profilicbot/openai_answers_final.txt"

# 1. Read all mismatched product names
with open(mismatches_file, encoding="utf-8") as f:
    bad_products = set(line.strip() for line in f if line.strip())

# 2. Read rerun answers into a dict: product name -> block
rerun_blocks = {}
with open(rerun_file, encoding="utf-8") as f:
    blocks = f.read().strip().split('\n\n')
    for block in blocks:
        lines = block.strip().split('\n')
        if lines:
            product = lines[0].strip()
            rerun_blocks[product] = block.strip()

# 3. Read master file, replace blocks as needed
final_blocks = []
with open(master_file, encoding="utf-8") as f:
    blocks = f.read().strip().split('\n\n')
    for block in blocks:
        lines = block.strip().split('\n')
        if not lines:
            continue
        product = lines[0].strip()
        # If this product is in the mismatches list and we have a rerun for it, replace!
        if product in bad_products and product in rerun_blocks:
            final_blocks.append(rerun_blocks[product])
        else:
            final_blocks.append(block.strip())

# 4. Write final output
with open(final_file, "w", encoding="utf-8") as f:
    f.write('\n\n'.join(final_blocks))

print(f"Done! Final file written to: {final_file}")
