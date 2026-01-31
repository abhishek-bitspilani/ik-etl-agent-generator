"""PySpark pipeline code generator."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any
import json


class PipelineGenerator:
    """Generates PySpark pipeline code from user stories."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview", temperature: float = 0.7):
        """Initialize the pipeline generator."""
        self.llm = ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=temperature
        )
    
    def generate(self, user_story: str) -> Dict[str, Any]:
        """Generate PySpark pipeline code from user story."""
        
        system_prompt = """You are an expert PySpark data engineer. Your task is to convert DevOps user stories into production-ready PySpark data pipeline code.

Requirements:
1. Generate clean, production-ready PySpark code
2. Include proper error handling and logging
3. Use best practices for Spark (broadcast variables, partitioning, etc.)
4. Include data validation and quality checks
5. Add comprehensive docstrings
6. Use type hints where appropriate
7. Follow PEP 8 style guidelines
8. Include configuration management
9. Make the code modular and reusable

The code should be a complete, runnable PySpark pipeline that can be executed independently."""

        user_prompt = f"""Convert the following DevOps user story into PySpark data pipeline code:

User Story:
{user_story}

Please generate:
1. Complete PySpark pipeline code
2. A descriptive file name (e.g., pipeline_name.py)
3. A brief description of what the pipeline does

Return your response as JSON with the following structure:
{{
    "file_name": "pipeline_name.py",
    "description": "Brief description",
    "code": "Complete PySpark code here"
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
                "file_name": "pipeline.py",
                "description": "Generated PySpark pipeline",
                "code": response.content
            }
