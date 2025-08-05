#!/usr/bin/env python3
"""
Script to fix the indentation issues in product_eligibility.py
"""

def fix_indentation():
    with open('product_eligibility.py', 'r') as f:
        lines = f.readlines()
    
    # Track indentation fixes needed
    # Lines 667-789 need to be indented by 4 more spaces (inside the main try block)
    # Lines 814-1254 need to be un-indented by 4 spaces (inside the except block)
    
    fixed_lines = []
    
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Main try block content (lines 667-789) - add 4 spaces if not empty/comment only
        if 667 <= line_num <= 789:
            if line.strip() and not line.lstrip().startswith('#'):
                # Add 4 spaces to existing indentation
                leading_spaces = len(line) - len(line.lstrip())
                if leading_spaces >= 12:  # Already properly indented
                    fixed_lines.append(line)
                else:
                    fixed_lines.append('    ' + line)
            else:
                fixed_lines.append(line)
        
        # Except block content (lines 814-1253) - remove 4 spaces from over-indented lines
        elif 814 <= line_num <= 1253:
            if line.strip() and not line.lstrip().startswith('#'):
                leading_spaces = len(line) - len(line.lstrip())
                if leading_spaces > 16:  # Over-indented, reduce by 4
                    fixed_lines.append(line[4:])
                else:
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
        
        else:
            fixed_lines.append(line)
    
    with open('product_eligibility.py', 'w') as f:
        f.writelines(fixed_lines)
    
    print("Indentation fixes applied")

if __name__ == "__main__":
    fix_indentation()