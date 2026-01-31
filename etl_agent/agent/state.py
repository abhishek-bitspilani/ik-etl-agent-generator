"""State management for LangGraph agent."""

from typing import TypedDict, Optional, List
from pydantic import BaseModel


class PipelineCode(BaseModel):
    """Generated pipeline code."""
    code: str
    file_name: str
    description: str


class TestCode(BaseModel):
    """Generated test code."""
    code: str
    file_name: str
    description: str


class ValidationResult(BaseModel):
    """Code validation results."""
    is_valid: bool
    syntax_errors: List[str]
    linting_issues: List[str]
    warnings: List[str]


class CodeReview(BaseModel):
    """Code review results."""
    review: str
    suggestions: List[str]
    score: Optional[float]  # 0-100
    approved: bool


class Documentation(BaseModel):
    """Generated documentation."""
    content: str
    file_name: str
    description: str


class AgentState(TypedDict):
    """State for the ETL agent workflow."""
    user_story: str
    pipeline_code: Optional[PipelineCode]
    test_code: Optional[TestCode]
    validation_result: Optional[ValidationResult]
    code_review: Optional[CodeReview]
    documentation: Optional[Documentation]
    pr_url: Optional[str]
    error: Optional[str]
    step: str  # Current step in workflow
