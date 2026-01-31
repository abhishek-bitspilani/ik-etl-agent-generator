"""Utility functions for building Jupyter notebooks."""

from typing import List, Dict, Any
import json


def create_notebook_cell(cell_type: str, source: List[str], metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """Create a notebook cell."""
    cell = {
        "cell_type": cell_type,
        "metadata": metadata or {},
        "source": source
    }
    if cell_type == "code":
        cell["execution_count"] = None
        cell["outputs"] = []
    return cell


def create_markdown_cell(text: str) -> Dict[str, Any]:
    """Create a markdown cell."""
    return create_notebook_cell("markdown", text.splitlines(keepends=True))


def create_code_cell(code: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """Create a code cell."""
    # Ensure code is a string
    if isinstance(code, list):
        # Convert each item to string before joining
        code = '\n'.join(str(item) for item in code)
    elif not isinstance(code, str):
        code = str(code)
    return create_notebook_cell("code", code.splitlines(keepends=True), metadata)


def build_pipeline_notebook(
    title: str,
    description: str,
    code: str,
    user_story: str = None,
    dataset_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Build a complete pipeline notebook."""
    # Ensure code is a string
    if isinstance(code, list):
        # Convert each item to string before joining
        code = '\n'.join(str(item) for item in code)
    elif not isinstance(code, str):
        code = str(code)
    
    cells = []
    
    # Title and description
    title_md = f"# {title}\n\n{description}"
    if user_story:
        title_md += f"\n\n## User Story\n\n{user_story}"
    cells.append(create_markdown_cell(title_md))
    
    # Dataset information if available
    if dataset_info:
        dataset_md = "## Dataset Information\n\n"
        dataset_md += f"- **File**: `{dataset_info.get('file_path', 'N/A')}`\n"
        dataset_md += f"- **Schema**: See schema metadata below\n\n"
        dataset_md += "### Schema Metadata\n\n```json\n"
        dataset_md += dataset_info.get('schema_metadata', '{}')
        dataset_md += "\n```"
        cells.append(create_markdown_cell(dataset_md))
    
    # Setup and imports
    setup_md = "## Setup and Imports\n\nThis cell contains all necessary imports and Spark session initialization."
    cells.append(create_markdown_cell(setup_md))
    
    # Extract imports and setup from code
    setup_code = extract_setup_code(code)
    if setup_code and len(setup_code) > 50:  # Only split if setup is substantial
        cells.append(create_code_cell(setup_code))
        
        # Main pipeline code
        pipeline_md = "## Pipeline Code\n\nThis cell contains the main ETL pipeline logic."
        cells.append(create_markdown_cell(pipeline_md))
        
        main_code = extract_main_code(code, setup_code)
        if main_code and main_code != code and len(main_code) > 20:
            cells.append(create_code_cell(main_code))
        else:
            # If extraction failed, just use all code
            cells.append(create_code_cell(code))
    else:
        # If no clear setup, put all code in one cell
        pipeline_md = "## Pipeline Code\n\nThis cell contains the complete ETL pipeline."
        cells.append(create_markdown_cell(pipeline_md))
        cells.append(create_code_cell(code))
    
    # Execution instructions
    exec_md = """## Execution Instructions

1. Ensure PySpark is installed: `pip install pyspark`
2. Make sure the dataset file exists at the specified path
3. Run all cells in order (Cell → Run All)
4. Check the output for any errors or warnings"""
    cells.append(create_markdown_cell(exec_md))
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook


def build_test_notebook(
    title: str,
    description: str,
    test_code: str,
    pipeline_file: str = None
) -> Dict[str, Any]:
    """Build a complete test notebook."""
    # Ensure test_code is a string
    if isinstance(test_code, list):
        # Convert each item to string before joining
        test_code = '\n'.join(str(item) for item in test_code)
    elif not isinstance(test_code, str):
        test_code = str(test_code)
    
    cells = []
    
    # Title and description
    title_md = f"# {title}\n\n{description}"
    if pipeline_file:
        title_md += f"\n\n**Tests for**: `{pipeline_file}`"
    cells.append(create_markdown_cell(title_md))
    
    # Setup instructions
    setup_md = """## Setup

Before running tests, ensure:
1. PySpark and pytest are installed: `pip install pyspark pytest`
2. The pipeline code is available (either as a module or in the same directory)
3. Test data is available if required"""
    cells.append(create_markdown_cell(setup_md))
    
    # Imports and fixtures
    setup_code = extract_test_setup_code(test_code)
    if setup_code:
        setup_md = "## Test Setup\n\nImports and test fixtures."
        cells.append(create_markdown_cell(setup_md))
        cells.append(create_code_cell(setup_code))
    
    # Test cases
    test_md = "## Test Cases\n\nRun these cells to execute individual tests or run all cells to execute the full test suite."
    cells.append(create_markdown_cell(test_md))
    
    # Split test code into individual test functions
    test_functions = extract_test_functions(test_code)
    for test_func in test_functions:
        func_name = extract_function_name(test_func)
        if func_name:
            func_md = f"### Test: {func_name}\n\nThis test validates specific functionality."
            cells.append(create_markdown_cell(func_md))
        cells.append(create_code_cell(test_func))
    
    # If no individual functions found, add the whole code
    if not test_functions:
        cells.append(create_code_cell(test_code))
    
    # Execution instructions
    exec_md = """## Running Tests

You can run tests in two ways:

1. **Run all cells**: Execute all tests at once (Cell → Run All)
2. **Run individual cells**: Run specific test cells to debug issues

Test results will be displayed in the output of each cell."""
    cells.append(create_markdown_cell(exec_md))
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return notebook


def extract_setup_code(code: str) -> str:
    """Extract setup/import code from pipeline code."""
    # Ensure code is a string
    if isinstance(code, list):
        # Convert each item to string before joining
        code = '\n'.join(str(item) for item in code)
    elif not isinstance(code, str):
        code = str(code)
    lines = code.split('\n')
    setup_lines = []
    found_spark_session = False
    
    for i, line in enumerate(lines):
        # Stop at main function definitions
        if line.strip().startswith('def main') or line.strip().startswith('if __name__'):
            break
        
        # Include imports
        if line.strip().startswith('import') or line.strip().startswith('from'):
            setup_lines.append(line)
            continue
        
        # Include SparkSession creation
        if 'SparkSession' in line or 'spark' in line.lower() and '=' in line:
            if not found_spark_session:
                # Include a few lines before and after SparkSession creation
                start = max(0, i - 2)
                end = min(len(lines), i + 5)
                for j in range(start, end):
                    if lines[j] not in setup_lines:
                        setup_lines.append(lines[j])
                found_spark_session = True
            continue
        
        # Include configuration or setup variables
        if found_spark_session and (line.strip().startswith('#') or not line.strip()):
            continue
        
        # Stop if we hit actual pipeline logic (after setup)
        if found_spark_session and line.strip() and not line.strip().startswith('#'):
            # Check if this looks like main logic
            if any(keyword in line for keyword in ['read', 'load', 'df', 'DataFrame']):
                break
    
    return '\n'.join(setup_lines).strip() if setup_lines else ""


def extract_main_code(code: str, setup_code: str) -> str:
    """Extract main pipeline code excluding setup."""
    # Ensure code is a string
    if isinstance(code, list):
        # Convert each item to string before joining
        code = '\n'.join(str(item) for item in code)
    elif not isinstance(code, str):
        code = str(code)
    
    if not setup_code:
        return code
    
    # Simple approach: if setup code is found, try to find where it ends
    code_lines = code.split('\n')
    setup_lines = [l.strip() for l in setup_code.split('\n') if l.strip()]
    
    # Find the last line of setup in the main code
    last_setup_idx = -1
    for i, line in enumerate(code_lines):
        stripped = line.strip()
        if stripped in setup_lines:
            last_setup_idx = i
    
    # Return everything after setup, or all code if setup not found
    if last_setup_idx >= 0:
        main_code = '\n'.join(code_lines[last_setup_idx + 1:]).strip()
        return main_code if main_code else code
    
    return code


def extract_test_setup_code(test_code: str) -> str:
    """Extract test setup code (imports, fixtures)."""
    # Ensure test_code is a string
    if isinstance(test_code, list):
        # Convert each item to string before joining
        test_code = '\n'.join(str(item) for item in test_code)
    elif not isinstance(test_code, str):
        test_code = str(test_code)
    lines = test_code.split('\n')
    setup_lines = []
    
    for line in lines:
        if line.strip().startswith('def test_') or line.strip().startswith('@pytest'):
            break
        if line.strip().startswith('import') or line.strip().startswith('from') or \
           line.strip().startswith('@pytest.fixture') or line.strip().startswith('def '):
            setup_lines.append(line)
    
    return '\n'.join(setup_lines).strip()


def extract_test_functions(test_code: str) -> List[str]:
    """Extract individual test functions from test code."""
    # Ensure test_code is a string
    if isinstance(test_code, list):
        # Convert each item to string before joining
        test_code = '\n'.join(str(item) for item in test_code)
    elif not isinstance(test_code, str):
        test_code = str(test_code)
    functions = []
    lines = test_code.split('\n')
    current_func = []
    in_function = False
    indent_level = 0
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('def test_'):
            if current_func:
                functions.append('\n'.join(current_func))
            current_func = [line]
            in_function = True
            indent_level = len(line) - len(line.lstrip())
        elif in_function:
            if stripped and (len(line) - len(line.lstrip())) <= indent_level and not line.startswith(' '):
                # New top-level definition
                if current_func:
                    functions.append('\n'.join(current_func))
                current_func = []
                in_function = False
            else:
                current_func.append(line)
    
    if current_func:
        functions.append('\n'.join(current_func))
    
    return functions if functions else []


def extract_function_name(func_code: str) -> str:
    """Extract function name from function code."""
    for line in func_code.split('\n'):
        if line.strip().startswith('def '):
            return line.strip().split('(')[0].replace('def ', '').strip()
    return ""


def notebook_to_json(notebook: Dict[str, Any]) -> str:
    """Convert notebook dict to JSON string."""
    return json.dumps(notebook, indent=2)
