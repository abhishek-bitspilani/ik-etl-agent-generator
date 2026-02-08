"""State management for LangGraph agent."""

from typing import TypedDict, Optional, List, Dict, Any
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


class DatasetInfo(BaseModel):
    """Information about the dataset being used."""
    dataset_name: str
    domain: str
    file_path: str
    schema_description: str
    schema_metadata: str  # Renamed from schema_json to avoid BaseModel shadowing


class AgentState(TypedDict):
    """State for the ETL agent workflow."""
    user_story: str
    dataset_info: Optional[DatasetInfo]  # Detected dataset information
    pipeline_code: Optional[PipelineCode]
    test_code: Optional[TestCode]
    validation_result: Optional[ValidationResult]
    code_review: Optional[CodeReview]
    documentation: Optional[Documentation]
    pr_url: Optional[str]
    error: Optional[str]
    step: str  # Current step in workflow
