"""Documentation generator for PySpark pipelines."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any
import json

from ..agent.state import Documentation


class DocumentationGenerator:
    """Generates documentation for PySpark pipelines."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview", temperature: float = 0.7):
        """Initialize the documentation generator."""
        self.llm = ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=temperature
        )
    
    def generate(self, pipeline_code: str, pipeline_description: str, user_story: str) -> Documentation:
        """Generate documentation for a PySpark pipeline."""
        
        system_prompt = """You are a technical writer specializing in data engineering documentation. Your task is to create comprehensive, clear, and useful documentation for PySpark data pipelines.

The documentation should include:
1. Overview and purpose
2. Architecture and design decisions
3. Input/output specifications
4. Configuration requirements
5. Usage examples
6. Troubleshooting guide
7. Performance considerations
8. Dependencies and requirements

Format the documentation in Markdown."""

        user_prompt = f"""Generate comprehensive documentation for the following PySpark pipeline:

User Story:
{user_story}

Pipeline Description:
{pipeline_description}

Pipeline Code:
```python
{pipeline_code}
```

Please generate:
1. Complete documentation in Markdown format
2. A descriptive file name (e.g., README.md or pipeline_name.md)
3. A brief description of the documentation

Return your response as JSON with the following structure:
{{
    "file_name": "README.md",
    "description": "Brief description",
    "content": "Complete Markdown documentation here"
}}"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        # Parse JSON response
        try:
            content = response.content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            result = json.loads(content)
            return Documentation(
                content=result["content"],
                file_name=result["file_name"],
                description=result["description"]
            )
        except json.JSONDecodeError:
            # Fallback: use response as markdown
            return Documentation(
                content=response.content,
                file_name="README.md",
                description="Generated pipeline documentation"
            )
