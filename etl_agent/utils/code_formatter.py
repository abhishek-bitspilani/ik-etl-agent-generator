"""Code formatting utilities to fix linting issues."""

import re
from typing import List


def format_long_lines(code: str, max_length: int = 120) -> str:
    """Format code to ensure lines don't exceed max_length."""
    lines = code.split('\n')
    formatted_lines = []
    
    for line in lines:
        if len(line) <= max_length:
            formatted_lines.append(line)
            continue
        
        # Try to break long lines intelligently
        formatted_line = break_long_line(line, max_length)
        formatted_lines.extend(formatted_line)
    
    return '\n'.join(formatted_lines)


def break_long_line(line: str, max_length: int) -> List[str]:
    """Break a long line into multiple lines."""
    # If it's a comment, just keep it (comments can be long)
    stripped = line.lstrip()
    if stripped.startswith('#'):
        return [line]
    
    # Try to break at common patterns
    # 1. Method chaining (.)
    if '.' in line and line.count('.') > 1:
        parts = line.split('.')
        result = [parts[0]]
        current = parts[0]
        
        for part in parts[1:]:
            if len(current + '.' + part) <= max_length:
                current += '.' + part
            else:
                result.append(current.rstrip())
                current = '    ' + part  # Indent continuation
        
        if current.strip():
            result.append(current)
        return result if len(result) > 1 else [line]
    
    # 2. Function calls with many arguments
    if '(' in line and line.count(',') > 2:
        # Try to break at commas
        match = re.match(r'^(\s*)(.*?)(\(.*?\))(.*)$', line)
        if match:
            indent, prefix, args, suffix = match.groups()
            # Simple approach: keep original if breaking is complex
            if len(line) > max_length * 1.5:
                # Very long line - break after opening paren
                return [
                    line[:line.find('(') + 1],
                    '    ' + line[line.find('(') + 1:]
                ]
    
    # 3. String concatenation
    if ' + ' in line and line.count(' + ') > 1:
        parts = line.split(' + ')
        result = []
        current = parts[0]
        
        for part in parts[1:]:
            if len(current + ' + ' + part) <= max_length:
                current += ' + ' + part
            else:
                result.append(current.rstrip() + ' +')
                # Determine indentation
                indent = len(line) - len(line.lstrip())
                current = ' ' * (indent + 4) + part.lstrip()
        
        if current.strip():
            result.append(current)
        return result if len(result) > 1 else [line]
    
    # 4. Assignment with long expressions
    if ' = ' in line:
        parts = line.split(' = ', 1)
        if len(parts) == 2 and len(parts[1]) > max_length - len(parts[0]) - 3:
            indent = len(line) - len(line.lstrip())
            return [
                parts[0] + ' = (',
                ' ' * (indent + 4) + parts[1] + ')'
            ]
    
    # If we can't break it intelligently, just return as-is
    # (Better to have a long line than broken code)
    return [line]


def format_code(code: str) -> str:
    """Format code to fix common linting issues."""
    # Fix line length issues
    code = format_long_lines(code)
    
    # Remove trailing whitespace
    lines = code.split('\n')
    cleaned_lines = [line.rstrip() for line in lines]
    code = '\n'.join(cleaned_lines)
    
    return code
