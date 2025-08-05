#!/usr/bin/env python3
"""
Script to fix the specific indentation issues in product_eligibility.py
Based on the structure analysis:
- Lines 637-789: Main try block content (needs consistent indentation)
- Lines 790-793: Main except block start (fixed manually already)  
- Lines 794-1254: Nested try-except block (needs adjustment)
"""

def fix_product_eligibility_indentation():
    with open('product_eligibility.py', 'r') as f:
        lines = f.readlines()
    
    fixed_lines = []
    
    for i, line in enumerate(lines):
        line_num = i + 1
        original_line = line
        
        # Special handling for different sections
        if 718 <= line_num <= 789:
            # This is the main sitemap processing section within the try block
            # Most of these lines should be indented to be inside the try block (16 spaces base)
            if line.strip():
                leading_spaces = len(line) - len(line.lstrip())
                
                # If it's currently unindented or minimally indented, fix it
                if leading_spaces < 16 and not line.strip().startswith('#'):
                    # Calculate correct indentation based on nesting
                    if line.strip().startswith(('except', 'if', 'for', 'while', 'with', 'try')):
                        # These are block statements, should be at 16 space level inside try
                        new_line = ' ' * 24 + line.lstrip()
                    elif line.strip().startswith(('print', 'import', 'response', 'content', 'root', 'urls_found')):
                        # These are regular statements, should be at 20 space level
                        new_line = ' ' * 28 + line.lstrip()
                    else:
                        # Default: add 4 spaces to current indentation if < 16
                        if leading_spaces < 16:
                            new_line = ' ' * (16 + (leading_spaces % 4)) + line.lstrip()
                        else:
                            new_line = line
                    fixed_lines.append(new_line)
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
                
        elif 814 <= line_num <= 1254:
            # This is the Firecrawl section - should be inside the except block
            # Current over-indentation needs to be reduced
            if line.strip():
                leading_spaces = len(line) - len(line.lstrip())
                
                # These lines are over-indented, reduce by 4 spaces
                if leading_spaces >= 20:
                    new_line = line[4:]
                    fixed_lines.append(new_line)
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
        else:
            # Keep other lines as-is
            fixed_lines.append(line)
    
    # Write the fixed content
    with open('product_eligibility.py', 'w') as f:
        f.writelines(fixed_lines)
    
    print(f"Fixed indentation for {len(fixed_lines)} lines")

if __name__ == "__main__":
    fix_product_eligibility_indentation()