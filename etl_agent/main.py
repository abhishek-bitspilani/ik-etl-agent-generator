"""Main entry point for ETL Agent."""

import argparse
import logging
import sys
from pathlib import Path

from .config import get_settings
from .agent.workflow import ETLAgentWorkflow

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)8s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Set workflow logger to INFO level for detailed output
workflow_logger = logging.getLogger('etl_agent.agent.workflow')
workflow_logger.setLevel(logging.INFO)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ETL Agent - Convert user stories to PySpark pipelines"
    )
    parser.add_argument(
        "--user-story",
        type=str,
        required=True,
        help="DevOps user story in natural language"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to file containing user story"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (optional)"
    )
    
    args = parser.parse_args()
    
    # Load settings
    try:
        settings = get_settings()
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")
        logger.error("Please ensure .env file is configured correctly")
        sys.exit(1)
    
    # Get user story
    if args.file:
        try:
            with open(args.file, 'r') as f:
                user_story = f.read()
        except Exception as e:
            logger.error(f"Failed to read user story file: {e}")
            sys.exit(1)
    else:
        user_story = args.user_story
    
    if not user_story.strip():
        logger.error("User story cannot be empty")
        sys.exit(1)
    
    logger.info("Starting ETL Agent workflow...")
    logger.info(f"User Story: {user_story[:100]}...")
    
    # Initialize and run workflow
    try:
        workflow = ETLAgentWorkflow(settings)
        result = workflow.run(user_story)
        
        # Print results
        print("\n" + "="*80)
        print("ETL Agent Results")
        print("="*80)
        
        if result.get("error"):
            print(f"\n❌ Error: {result['error']}")
            sys.exit(1)
        
        if result.get("dataset_info"):
            dataset = result["dataset_info"]
            print(f"\n📊 Dataset Detected:")
            print(f"   Name: {dataset.dataset_name}")
            print(f"   Domain: {dataset.domain}")
            print(f"   File: {dataset.file_path}")
        
        if result.get("pipeline_code"):
            pipeline = result["pipeline_code"]
            print(f"\n✅ Pipeline Generated:")
            print(f"   File: {pipeline.file_name}")
            print(f"   Description: {pipeline.description}")
        
        if result.get("test_code"):
            tests = result["test_code"]
            print(f"\n✅ Tests Generated:")
            print(f"   File: {tests.file_name}")
            print(f"   Description: {tests.description}")
        
        if result.get("validation_result"):
            validation = result["validation_result"]
            status = "✅ Passed" if validation.is_valid else "⚠️ Issues Found"
            print(f"\n🔍 Validation: {status}")
            if validation.syntax_errors:
                print(f"   Syntax Errors: {len(validation.syntax_errors)}")
            if validation.linting_issues:
                print(f"   Linting Issues: {len(validation.linting_issues)}")
            if validation.warnings:
                print(f"   Warnings: {len(validation.warnings)}")
        
        if result.get("code_review"):
            review = result["code_review"]
            print(f"\n📝 Code Review:")
            print(f"   Score: {review.score}/100" if review.score else "   Score: N/A")
            print(f"   Status: {'✅ Approved' if review.approved else '⚠️ Needs Improvement'}")
            print(f"   Suggestions: {len(review.suggestions)}")
            if review.suggestions:
                print(f"   Top suggestions:")
                for suggestion in review.suggestions[:3]:
                    print(f"     - {suggestion}")
        
        if result.get("documentation"):
            docs = result["documentation"]
            print(f"\n📚 Documentation Generated:")
            print(f"   File: {docs.file_name}")
            print(f"   Description: {docs.description}")
        
        if result.get("pr_url"):
            print(f"\n✅ Pull Request Created:")
            print(f"   URL: {result['pr_url']}")
        else:
            print("\n⚠️  No PR was created (check logs for details)")
        
        print("\n" + "="*80)
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
