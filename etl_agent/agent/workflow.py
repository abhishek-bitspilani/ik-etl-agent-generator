"""LangGraph workflow for ETL agent."""

from langgraph.graph import StateGraph, END
from typing import Dict, Any
import logging
import json

from .state import AgentState, PipelineCode, TestCode, ValidationResult, CodeReview, Documentation, DatasetInfo
from ..generators.pipeline_generator import PipelineGenerator
from ..generators.test_generator import TestGenerator
from ..generators.review_generator import ReviewGenerator
from ..generators.doc_generator import DocumentationGenerator
from ..utils.validator import CodeValidator
from ..utils.dataset_loader import DatasetLoader
from ..utils.code_formatter import format_code
from ..github.client import GitHubClient
from ..config import Settings

logger = logging.getLogger(__name__)


class ETLAgentWorkflow:
    """LangGraph workflow for ETL pipeline generation."""
    
    def __init__(self, settings: Settings):
        """Initialize the workflow."""
        self.settings = settings
        self.pipeline_generator = PipelineGenerator(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            temperature=settings.temperature
        )
        self.test_generator = TestGenerator(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            temperature=settings.temperature
        )
        self.review_generator = ReviewGenerator(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            temperature=0.3  # Lower temperature for reviews
        )
        self.doc_generator = DocumentationGenerator(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            temperature=settings.temperature
        )
        self.validator = CodeValidator()
        self.dataset_loader = DatasetLoader(data_dir="data")
        self.github_client = GitHubClient(
            token=settings.github_token,
            repo=settings.github_repo,
            base_branch=settings.github_base_branch
        )
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow."""
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("detect_dataset", self._detect_dataset)
        workflow.add_node("generate_pipeline", self._generate_pipeline)
        workflow.add_node("generate_tests", self._generate_tests)
        workflow.add_node("validate_code", self._validate_code)
        workflow.add_node("review_code", self._review_code)
        workflow.add_node("generate_docs", self._generate_docs)
        workflow.add_node("create_pr", self._create_pr)
        
        # Define edges
        workflow.set_entry_point("detect_dataset")
        workflow.add_edge("detect_dataset", "generate_pipeline")
        workflow.add_edge("generate_pipeline", "generate_tests")
        workflow.add_edge("generate_tests", "validate_code")
        workflow.add_edge("validate_code", "review_code")
        workflow.add_edge("review_code", "generate_docs")
        workflow.add_edge("generate_docs", "create_pr")
        workflow.add_edge("create_pr", END)
        
        return workflow.compile()
    
    def _detect_dataset(self, state: AgentState) -> AgentState:
        """Detect and load dataset information from user story."""
        logger.info("=" * 60)
        logger.info("STEP 1: Detecting dataset references...")
        logger.info("=" * 60)
        state["step"] = "detect_dataset"
        
        try:
            user_story = state["user_story"]
            logger.info(f"Analyzing user story: {user_story[:100]}...")
            dataset = self.dataset_loader.find_dataset_by_reference(user_story)
            
            if dataset:
                logger.info(f"✅ Detected dataset: {dataset.dataset_name}")
                logger.info(f"   Domain: {dataset.domain}")
                logger.info(f"   File: {dataset.file_path}")
                logger.info(f"   Fields: {len(dataset.fields)}")
                state["dataset_info"] = DatasetInfo(
                    dataset_name=dataset.dataset_name,
                    domain=dataset.domain,
                    file_path=dataset.file_path,
                    schema_description=self.dataset_loader.get_schema_description(dataset),
                    schema_metadata=self.dataset_loader.get_schema_for_prompt(dataset)
                )
            else:
                logger.info("ℹ️ No dataset reference detected - proceeding without dataset")
                state["dataset_info"] = None
        except Exception as e:
            logger.error(f"❌ Error detecting dataset: {e}")
            state["dataset_info"] = None
        
        return state
    
    def _generate_pipeline(self, state: AgentState) -> AgentState:
        """Generate PySpark pipeline code."""
        logger.info("=" * 60)
        logger.info("STEP 2: Generating PySpark pipeline code...")
        logger.info("=" * 60)
        state["step"] = "generate_pipeline"
        
        try:
            # Prepare dataset info if available
            dataset_info = None
            if state.get("dataset_info"):
                dataset = state["dataset_info"]
                dataset_info = {
                    "file_path": dataset.file_path,
                    "schema_description": dataset.schema_description,
                    "schema_metadata": dataset.schema_metadata
                }
            
            result = self.pipeline_generator.generate(
                state["user_story"],
                dataset_info=dataset_info
            )
            
            # Format the generated code to fix linting issues
            # Note: result["code"] is notebook JSON, so we need to parse and format the actual code
            try:
                notebook = json.loads(result["code"])
                # Format code in all code cells
                for cell in notebook.get("cells", []):
                    if cell.get("cell_type") == "code":
                        source = cell.get("source", [])
                        if isinstance(source, list):
                            code_str = ''.join(source)
                        else:
                            code_str = str(source)
                        # Format the code
                        formatted_code = format_code(code_str)
                        # Update cell source
                        cell["source"] = formatted_code.splitlines(keepends=True)
                # Convert back to JSON
                import json as json_module
                result["code"] = json_module.dumps(notebook, indent=2)
            except Exception as e:
                logger.warning(f"Could not format code: {e}, using original")
            
            state["pipeline_code"] = PipelineCode(
                code=result["code"],
                file_name=result["file_name"],
                description=result["description"]
            )
            logger.info(f"✅ Pipeline generated successfully!")
            logger.info(f"   File: {result['file_name']}")
            logger.info(f"   Description: {result['description']}")
        except Exception as e:
            logger.error(f"❌ Error generating pipeline: {e}")
            logger.exception("Full error traceback:")
            state["error"] = f"Pipeline generation failed: {str(e)}"
        
        return state
    
    def _generate_tests(self, state: AgentState) -> AgentState:
        """Generate test code."""
        logger.info("=" * 60)
        logger.info("STEP 3: Generating test code...")
        logger.info("=" * 60)
        state["step"] = "generate_tests"
        
        if state.get("error") or not state.get("pipeline_code"):
            state["error"] = "Cannot generate tests: pipeline generation failed"
            return state
        
        try:
            pipeline_code = state["pipeline_code"]
            result = self.test_generator.generate(
                pipeline_code.code,
                pipeline_code.description
            )
            
            # Format the generated test code to fix linting issues
            try:
                notebook = json.loads(result["code"])
                # Format code in all code cells
                for cell in notebook.get("cells", []):
                    if cell.get("cell_type") == "code":
                        source = cell.get("source", [])
                        if isinstance(source, list):
                            code_str = ''.join(source)
                        else:
                            code_str = str(source)
                        # Format the code
                        formatted_code = format_code(code_str)
                        # Update cell source
                        cell["source"] = formatted_code.splitlines(keepends=True)
                # Convert back to JSON
                result["code"] = json.dumps(notebook, indent=2)
            except Exception as e:
                logger.warning(f"Could not format test code: {e}, using original")
            
            state["test_code"] = TestCode(
                code=result["code"],
                file_name=result["file_name"],
                description=result["description"]
            )
            logger.info(f"✅ Tests generated successfully!")
            logger.info(f"   File: {result['file_name']}")
            logger.info(f"   Description: {result['description']}")
        except Exception as e:
            logger.error(f"❌ Error generating tests: {e}")
            logger.exception("Full error traceback:")
            state["error"] = f"Test generation failed: {str(e)}"
        
        return state
    
    def _validate_code(self, state: AgentState) -> AgentState:
        """Validate generated code."""
        logger.info("=" * 60)
        logger.info("STEP 4: Validating code...")
        logger.info("=" * 60)
        state["step"] = "validate_code"
        
        if state.get("error") or not state.get("pipeline_code"):
            state["error"] = "Cannot validate: pipeline generation failed"
            return state
        
        try:
            pipeline_code = state["pipeline_code"]
            validation_result = self.validator.validate(pipeline_code.code)
            state["validation_result"] = validation_result
            
            if not validation_result.is_valid:
                logger.warning(f"⚠️ Validation found issues:")
                if validation_result.syntax_errors:
                    logger.warning(f"   Syntax errors: {len(validation_result.syntax_errors)}")
                    for error in validation_result.syntax_errors[:3]:
                        logger.warning(f"     - {error}")
                if validation_result.linting_issues:
                    logger.warning(f"   Linting issues: {len(validation_result.linting_issues)}")
                    for issue in validation_result.linting_issues[:3]:
                        logger.warning(f"     - {issue}")
            else:
                logger.info("✅ Code validation passed - no issues found")
            
            if validation_result.warnings:
                logger.info(f"ℹ️ Warnings ({len(validation_result.warnings)}):")
                for warning in validation_result.warnings[:3]:
                    logger.info(f"   - {warning}")
        except Exception as e:
            logger.error(f"Error validating code: {e}")
            # Don't fail the workflow on validation errors, just log them
            state["validation_result"] = ValidationResult(
                is_valid=False,
                syntax_errors=[str(e)],
                linting_issues=[],
                warnings=[]
            )
        
        return state
    
    def _review_code(self, state: AgentState) -> AgentState:
        """Review generated code."""
        logger.info("=" * 60)
        logger.info("STEP 5: Reviewing code...")
        logger.info("=" * 60)
        state["step"] = "review_code"
        
        if state.get("error") or not state.get("pipeline_code") or not state.get("test_code"):
            state["error"] = "Cannot review: missing generated code"
            return state
        
        try:
            pipeline_code = state["pipeline_code"]
            test_code = state["test_code"]
            
            code_review = self.review_generator.generate(
                pipeline_code.code,
                test_code.code,
                pipeline_code.description
            )
            state["code_review"] = code_review
            
            logger.info(f"✅ Code review completed")
            logger.info(f"   Score: {code_review.score}/100" if code_review.score else "   Score: N/A")
            logger.info(f"   Approved: {'Yes' if code_review.approved else 'No'}")
            if code_review.suggestions:
                logger.info(f"   Suggestions: {len(code_review.suggestions)}")
                for suggestion in code_review.suggestions[:3]:
                    logger.info(f"     - {suggestion}")
        except Exception as e:
            logger.error(f"Error reviewing code: {e}")
            # Don't fail the workflow on review errors
            state["code_review"] = CodeReview(
                review=f"Review generation failed: {str(e)}",
                suggestions=[],
                score=None,
                approved=False
            )
        
        return state
    
    def _generate_docs(self, state: AgentState) -> AgentState:
        """Generate documentation."""
        logger.info("=" * 60)
        logger.info("STEP 6: Generating documentation...")
        logger.info("=" * 60)
        state["step"] = "generate_docs"
        
        if state.get("error") or not state.get("pipeline_code"):
            state["error"] = "Cannot generate docs: pipeline generation failed"
            return state
        
        try:
            pipeline_code = state["pipeline_code"]
            documentation = self.doc_generator.generate(
                pipeline_code.code,
                pipeline_code.description,
                state["user_story"]
            )
            state["documentation"] = documentation
            logger.info(f"✅ Documentation generated successfully!")
            logger.info(f"   File: {documentation.file_name}")
            logger.info(f"   Description: {documentation.description}")
        except Exception as e:
            logger.error(f"Error generating documentation: {e}")
            # Don't fail the workflow on doc generation errors
            state["documentation"] = Documentation(
                content=f"# Pipeline Documentation\n\nDocumentation generation failed: {str(e)}",
                file_name="README.md",
                description="Placeholder documentation"
            )
        
        return state
    
    def _create_pr(self, state: AgentState) -> AgentState:
        """Create GitHub PR with generated code."""
        logger.info("=" * 60)
        logger.info("STEP 7: Creating GitHub PR...")
        logger.info("=" * 60)
        state["step"] = "create_pr"
        
        if state.get("error"):
            logger.error(f"Skipping PR creation due to error: {state['error']}")
            return state
        
        if not state.get("pipeline_code") or not state.get("test_code"):
            state["error"] = "Cannot create PR: missing generated code"
            return state
        
        try:
            pipeline = state["pipeline_code"]
            tests = state["test_code"]
            
            # Prepare files for PR
            # Note: pipeline.code and tests.code are now JSON strings for .ipynb files
            files = {
                f"notebooks/pipelines/{pipeline.file_name}": pipeline.code,
                f"notebooks/tests/{tests.file_name}": tests.code
            }
            
            # Add documentation if available
            if state.get("documentation"):
                docs = state["documentation"]
                files[f"docs/{docs.file_name}"] = docs.content
            
            # Create PR with enhanced body
            pr_title = f"ETL Pipeline: {pipeline.description}"
            
            # Build PR body with all information
            pr_body = f"""## Generated ETL Pipeline

**User Story:**
{state['user_story']}

**Pipeline:**
- File: `{pipeline.file_name}`
- Description: {pipeline.description}

**Tests:**
- File: `{tests.file_name}`
- Description: {tests.description}
"""
            
            # Add validation results
            if state.get("validation_result"):
                validation = state["validation_result"]
                pr_body += f"""
**Validation:**
- Status: {'✅ Passed' if validation.is_valid else '⚠️ Issues Found'}
"""
                if validation.syntax_errors:
                    pr_body += f"- Syntax Errors: {len(validation.syntax_errors)}\n"
                if validation.linting_issues:
                    pr_body += f"- Linting Issues: {len(validation.linting_issues)}\n"
                if validation.warnings:
                    pr_body += f"- Warnings: {len(validation.warnings)}\n"
            
            # Add code review
            if state.get("code_review"):
                review = state["code_review"]
                pr_body += f"""
**Code Review:**
- Score: {review.score}/100
- Status: {'✅ Approved' if review.approved else '⚠️ Needs Improvement'}
- Suggestions: {len(review.suggestions)}
"""
                if review.suggestions:
                    pr_body += "\n**Top Suggestions:**\n"
                    for suggestion in review.suggestions[:5]:
                        pr_body += f"- {suggestion}\n"
            
            # Add documentation
            if state.get("documentation"):
                docs = state["documentation"]
                pr_body += f"""
**Documentation:**
- File: `{docs.file_name}`
"""
            
            pr_body += "\n---\n*This PR was automatically generated by the ETL Agent.*"
            
            pr_url = self.github_client.create_pr_with_files(
                files=files,
                pr_title=pr_title,
                pr_body=pr_body
            )
            
            if pr_url:
                state["pr_url"] = pr_url
                logger.info(f"✅ PR created successfully!")
                logger.info(f"   URL: {pr_url}")
            else:
                logger.error("❌ Failed to create PR")
                state["error"] = "Failed to create PR"
        except Exception as e:
            logger.error(f"❌ Error creating PR: {e}")
            logger.exception("Full error traceback:")
            state["error"] = f"PR creation failed: {str(e)}"
        
        return state
    
    def run(self, user_story: str) -> AgentState:
        """Run the workflow with a user story."""
        initial_state: AgentState = {
            "user_story": user_story,
            "dataset_info": None,
            "pipeline_code": None,
            "test_code": None,
            "validation_result": None,
            "code_review": None,
            "documentation": None,
            "pr_url": None,
            "error": None,
            "step": "initialized"
        }
        
        result = self.graph.invoke(initial_state)
        return result
