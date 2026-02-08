"""PySpark pipeline code generator."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any, Optional
import json

from ..utils.notebook_builder import build_pipeline_notebook, notebook_to_json


class PipelineGenerator:
    """Generates PySpark pipeline code from user stories."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview", temperature: float = 0.7):
        """Initialize the pipeline generator."""
        self.llm = ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=temperature
        )
    
    def generate(self, user_story: str, dataset_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generate PySpark pipeline code from user story."""
        
        system_prompt = """You are an expert PySpark data engineer. Your task is to convert DevOps user stories into production-ready PySpark data pipeline code.

Requirements:
1. Generate clean, production-ready PySpark code
2. Include proper error handling and logging
3. Use best practices for Spark (broadcast variables, partitioning, etc.)
4. Include data validation and quality checks
5. Add comprehensive docstrings
6. Use type hints where appropriate
7. Follow PEP 8 style guidelines - IMPORTANT: Keep lines under 120 characters
8. Include configuration management
9. Make the code modular and reusable
10. When a dataset is provided, use the exact file path and schema fields from the metadata
11. Read CSV files using Spark's CSV reader with appropriate options (header=True, inferSchema=True or explicit schema)
12. Break long lines appropriately - use parentheses for line continuation, break method chains across lines

The code should be a complete, runnable PySpark pipeline that can be executed independently."""

        user_prompt = f"""Convert the following DevOps user story into PySpark data pipeline code:

User Story:
{user_story}
"""
        
        if dataset_info:
            user_prompt += f"""

IMPORTANT: The user story references a dataset. Use the following dataset information:

Dataset Schema:
{dataset_info.get('schema_description', '')}

Schema JSON:
{dataset_info.get('schema_metadata', '')}

Dataset File Path: {dataset_info.get('file_path', '')}

When generating the pipeline:
- Use the exact file path: {dataset_info.get('file_path', '')}
- Use the exact field names from the schema
- Respect field types (string, integer, decimal, date)
- Handle nullable fields appropriately
- Use the field descriptions to understand data semantics
"""
        
        user_prompt += """
Please generate:
1. Complete PySpark pipeline code (ready to be put in a Jupyter notebook)
2. A descriptive file name (e.g., pipeline_name.ipynb)
3. A brief description of what the pipeline does

IMPORTANT: The code will be placed in a Jupyter notebook, so:
- Organize code logically (imports first, then main logic)
- Include clear comments explaining each section
- Make code executable in notebook cells

Return your response as JSON with the following structure:
{
    "file_name": "pipeline_name.ipynb",
    "description": "Brief description",
    "code": "Complete PySpark code here"
}"""

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
            file_name = result.get("file_name", "pipeline.ipynb")
            if not file_name.endswith(".ipynb"):
                # Replace .py with .ipynb
                file_name = file_name.replace(".py", ".ipynb")
            
            # Ensure code is a string
            code = result.get("code", "")
            if isinstance(code, list):
                # Convert each item to string before joining
                code = '\n'.join(str(item) for item in code)
            elif not isinstance(code, str):
                code = str(code)
            
            # Build notebook
            notebook = build_pipeline_notebook(
                title=file_name.replace(".ipynb", "").replace("_", " ").title(),
                description=result.get("description", "Generated PySpark pipeline"),
                code=code,
                user_story=user_story,
                dataset_info=dataset_info
            )
            
            # Convert notebook to JSON string
            notebook_json = notebook_to_json(notebook)
            
            return {
                "file_name": file_name,
                "description": result.get("description", "Generated PySpark pipeline"),
                "code": notebook_json  # Store notebook JSON as "code"
            }
        except json.JSONDecodeError:
            # Fallback: try to extract code from markdown
            file_name = "pipeline.ipynb"
            # Ensure code is a string
            code = response.content
            if isinstance(code, list):
                # Convert each item to string before joining
                code = '\n'.join(str(item) for item in code)
            elif not isinstance(code, str):
                code = str(code)
            
            notebook = build_pipeline_notebook(
                title="Pipeline",
                description="Generated PySpark pipeline",
                code=code,
                user_story=user_story,
                dataset_info=dataset_info
            )
            notebook_json = notebook_to_json(notebook)
            
            return {
                "file_name": file_name,
                "description": "Generated PySpark pipeline",
                "code": notebook_json
            }
