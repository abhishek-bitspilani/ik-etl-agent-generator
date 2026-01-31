"""Test code generator for PySpark pipelines."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any
import json

from ..utils.notebook_builder import build_test_notebook, notebook_to_json


class TestGenerator:
    """Generates test code for PySpark pipelines."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview", temperature: float = 0.7):
        """Initialize the test generator."""
        self.llm = ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=temperature
        )
    
    def generate(self, pipeline_code: str, pipeline_description: str) -> Dict[str, Any]:
        """Generate test code for a PySpark pipeline."""
        
        system_prompt = """You are an expert in writing comprehensive unit tests for PySpark data pipelines. Your task is to generate production-quality test code.

Requirements:
1. Use pytest and pyspark testing best practices
2. Include unit tests for all major functions
3. Test edge cases and error scenarios
4. Use mock data and fixtures appropriately
5. Test data transformations and validations
6. Include integration test patterns
7. Use proper assertions
8. Include test data setup and teardown
9. Follow pytest conventions and naming
10. Make tests maintainable and readable
11. Follow PEP 8 style guidelines - IMPORTANT: Keep lines under 120 characters
12. Break long lines appropriately - use parentheses for line continuation

The tests should be comprehensive and cover the pipeline's functionality thoroughly."""

        user_prompt = f"""Generate comprehensive test code for the following PySpark pipeline:

Pipeline Description:
{pipeline_description}

Pipeline Code:
```python
{pipeline_code}
```

Please generate:
1. Complete test code using pytest (ready to be put in a Jupyter notebook)
2. A descriptive test file name (e.g., test_pipeline_name.ipynb)
3. A brief description of what the tests cover

IMPORTANT: The code will be placed in a Jupyter notebook, so:
- Organize tests logically (setup first, then individual test functions)
- Include clear comments explaining each test
- Make tests executable in notebook cells
- Use pytest conventions

Return your response as JSON with the following structure:
{{
    "file_name": "test_pipeline_name.ipynb",
    "description": "Brief description of test coverage",
    "code": "Complete test code here"
}}"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        # Parse JSON response
        try:
            # Extract JSON from response (handle markdown code blocks)
            content = response.content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            result = json.loads(content)
            
            # Convert to notebook format
            file_name = result.get("file_name", "test_pipeline.ipynb")
            if not file_name.endswith(".ipynb"):
                # Replace .py with .ipynb
                file_name = file_name.replace(".py", ".ipynb")
            
            # Ensure code is a string
            test_code = result.get("code", "")
            if isinstance(test_code, list):
                # Convert each item to string before joining
                test_code = '\n'.join(str(item) for item in test_code)
            elif not isinstance(test_code, str):
                test_code = str(test_code)
            
            # Build notebook
            notebook = build_test_notebook(
                title=file_name.replace(".ipynb", "").replace("_", " ").title(),
                description=result.get("description", "Generated tests for PySpark pipeline"),
                test_code=test_code,
                pipeline_file=None  # Could be passed if needed
            )
            
            # Convert notebook to JSON string
            notebook_json = notebook_to_json(notebook)
            
            return {
                "file_name": file_name,
                "description": result.get("description", "Generated tests for PySpark pipeline"),
                "code": notebook_json  # Store notebook JSON as "code"
            }
        except json.JSONDecodeError:
            # Fallback: try to extract code from markdown
            file_name = "test_pipeline.ipynb"
            # Ensure code is a string
            test_code = response.content
            if isinstance(test_code, list):
                # Convert each item to string before joining
                test_code = '\n'.join(str(item) for item in test_code)
            elif not isinstance(test_code, str):
                test_code = str(test_code)
            
            notebook = build_test_notebook(
                title="Test Pipeline",
                description="Generated tests for PySpark pipeline",
                test_code=test_code
            )
            notebook_json = notebook_to_json(notebook)
            
            return {
                "file_name": file_name,
                "description": "Generated tests for PySpark pipeline",
                "code": notebook_json
            }
