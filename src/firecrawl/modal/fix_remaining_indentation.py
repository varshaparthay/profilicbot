#!/usr/bin/env python3
"""
Fix the remaining indentation issues in product_eligibility.py
Based on the pattern observed, most lines from around 995-1258 need to be indented properly
"""

def fix_remaining_indentation():
    with open('product_eligibility.py', 'r') as f:
        lines = f.readlines()
    
    fixed_lines = []
    
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Lines that need indentation fixes (approximately 995-1258)
        # These are inside the nested try block that starts around line 794
        if 995 <= line_num <= 1253:
            if line.strip():  # Non-empty lines
                leading_spaces = len(line) - len(line.lstrip())
                
                # If the line is not indented enough for the context, add 4 spaces
                # Most of these should be inside the nested try-except block
                if leading_spaces < 20 and not line.lstrip().startswith('#'):
                    # Add appropriate indentation based on the context
                    if line.strip().startswith(('def ', 'class ')):
                        # Functions/classes should be at 16 space level
                        new_line = ' ' * 16 + line.lstrip()
                    elif line.strip().startswith(('if ', 'elif ', 'else:', 'for ', 'while ', 'with ', 'try:', 'except ', 'finally:')):
                        # Control structures should be at 20 space level
                        new_line = ' ' * 20 + line.lstrip()
                    elif line.strip().startswith(('return ', 'break', 'continue', 'pass', 'raise ')):
                        # Control flow statements should be at 24 space level
                        new_line = ' ' * 24 + line.lstrip()
                    else:
                        # Regular statements should be at 20 space level minimum
                        if leading_spaces < 20:
                            new_line = ' ' * 20 + line.lstrip()
                        else:
                            new_line = line
                    
                    fixed_lines.append(new_line)
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    # Write the fixed content
    with open('product_eligibility.py', 'w') as f:
        f.writelines(fixed_lines)
    
    print(f"Applied indentation fixes to remaining lines")

if __name__ == "__main__":
    fix_remaining_indentation()