"""Test code generator for PySpark pipelines."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any
import json


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

The tests should be comprehensive and cover the pipeline's functionality thoroughly."""

        user_prompt = f"""Generate comprehensive test code for the following PySpark pipeline:

Pipeline Description:
{pipeline_description}

Pipeline Code:
```python
{pipeline_code}
```

Please generate:
1. Complete test code using pytest
2. A descriptive test file name (e.g., test_pipeline_name.py)
3. A brief description of what the tests cover

Return your response as JSON with the following structure:
{{
    "file_name": "test_pipeline_name.py",
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
            return result
        except json.JSONDecodeError:
            # Fallback: try to extract code from markdown
            return {
                "file_name": "test_pipeline.py",
                "description": "Generated tests for PySpark pipeline",
                "code": response.content
            }
