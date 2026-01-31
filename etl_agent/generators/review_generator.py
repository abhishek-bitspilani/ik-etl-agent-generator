"""Code review generator using LLM."""

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from typing import Dict, Any
import json
import re

from ..agent.state import CodeReview


class ReviewGenerator:
    """Generates code reviews for PySpark pipelines."""
    
    def __init__(self, api_key: str, model: str = "gpt-4-turbo-preview", temperature: float = 0.3):
        """Initialize the review generator."""
        self.llm = ChatOpenAI(
            api_key=api_key,
            model=model,
            temperature=temperature  # Lower temperature for more consistent reviews
        )
    
    def generate(self, pipeline_code: str, test_code: str, pipeline_description: str) -> CodeReview:
        """Generate code review for pipeline and tests."""
        
        system_prompt = """You are an expert code reviewer specializing in PySpark data pipelines. Your task is to review code for:
1. Code quality and best practices
2. Performance optimizations
3. Error handling and robustness
4. Test coverage and quality
5. Documentation and maintainability
6. Security considerations
7. Adherence to PySpark best practices

Provide constructive feedback and specific suggestions for improvement."""

        user_prompt = f"""Review the following PySpark pipeline code and its tests:

Pipeline Description:
{pipeline_description}

Pipeline Code:
```python
{pipeline_code}
```

Test Code:
```python
{test_code}
```

Please provide:
1. A comprehensive code review
2. Specific suggestions for improvement (as a list)
3. A quality score from 0-100
4. Whether the code is approved (true/false)

Return your response as JSON with the following structure:
{{
    "review": "Detailed review text",
    "suggestions": ["suggestion1", "suggestion2", ...],
    "score": 85.5,
    "approved": true
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
            
            return CodeReview(
                review=result.get("review", ""),
                suggestions=result.get("suggestions", []),
                score=result.get("score"),
                approved=result.get("approved", False)
            )
        except json.JSONDecodeError:
            # Fallback: extract review from text
            review_text = response.content
            suggestions = re.findall(r'- (.+)', review_text)
            
            return CodeReview(
                review=review_text,
                suggestions=suggestions[:10],  # Limit to 10 suggestions
                score=None,
                approved=False
            )
