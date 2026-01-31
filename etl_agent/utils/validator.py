"""Code validation utilities for PySpark pipelines."""

import ast
import re
from typing import List, Tuple
from ..agent.state import ValidationResult


class CodeValidator:
    """Validates Python code for syntax and common issues."""
    
    @staticmethod
    def validate_syntax(code: str) -> Tuple[bool, List[str]]:
        """Validate Python syntax."""
        errors = []
        try:
            ast.parse(code)
            return True, []
        except SyntaxError as e:
            error_msg = f"Syntax error at line {e.lineno}: {e.msg}"
            errors.append(error_msg)
            return False, errors
        except Exception as e:
            errors.append(f"Parse error: {str(e)}")
            return False, errors
    
    @staticmethod
    def validate_pyspark_imports(code: str) -> List[str]:
        """Check for required PySpark imports."""
        issues = []
        if "from pyspark.sql" not in code and "import pyspark" not in code:
            issues.append("Missing PySpark imports")
        return issues
    
    @staticmethod
    def validate_best_practices(code: str) -> List[str]:
        """Check for PySpark best practices."""
        warnings = []
        
        # Check for SparkSession creation
        if "SparkSession" not in code and "spark" not in code.lower():
            warnings.append("No SparkSession found - ensure Spark context is created")
        
        # Check for error handling
        if "try:" not in code and "except" not in code:
            warnings.append("Consider adding error handling (try/except blocks)")
        
        # Check for logging
        if "logging" not in code and "logger" not in code.lower():
            warnings.append("Consider adding logging for better observability")
        
        # Check for docstrings
        if '"""' not in code and "'''" not in code:
            warnings.append("Consider adding docstrings to functions/classes")
        
        return warnings
    
    @staticmethod
    def lint_code(code: str) -> List[str]:
        """Basic linting checks."""
        issues = []
        lines = code.split('\n')
        
        # Check line length
        for i, line in enumerate(lines, 1):
            if len(line) > 120:
                issues.append(f"Line {i} exceeds 120 characters")
        
        # Check for common issues
        if re.search(r'\s+$', code, re.MULTILINE):
            issues.append("Trailing whitespace detected")
        
        # Check for print statements (should use logging)
        if re.search(r'\bprint\s*\(', code):
            issues.append("Consider using logging instead of print statements")
        
        return issues
    
    def validate(self, code: str) -> ValidationResult:
        """Run all validation checks."""
        is_valid, syntax_errors = self.validate_syntax(code)
        linting_issues = self.lint_code(code)
        pyspark_issues = self.validate_pyspark_imports(code)
        warnings = self.validate_best_practices(code)
        
        # Combine all issues
        all_issues = syntax_errors + linting_issues + pyspark_issues
        
        return ValidationResult(
            is_valid=is_valid and len(all_issues) == 0,
            syntax_errors=syntax_errors,
            linting_issues=linting_issues + pyspark_issues,
            warnings=warnings
        )
