"""Example usage of ETL Agent."""

from etl_agent.config import get_settings
from etl_agent.agent.workflow import ETLAgentWorkflow

# Example user stories
EXAMPLE_USER_STORIES = [
    """
    As a data engineer, I want to create a pipeline that:
    - Reads customer data from a CSV file
    - Filters customers with age > 18
    - Calculates total purchase amount per customer
    - Writes the results to a Parquet file
    """,
    """
    As a DevOps engineer, I need a data pipeline that:
    - Ingests log files from S3 bucket
    - Parses log entries and extracts error messages
    - Aggregates errors by hour and service
    - Outputs aggregated results to a database table
    """,
    """
    User Story: Build an ETL pipeline that:
    1. Reads sales transactions from JSON files
    2. Validates data quality (no nulls, valid dates)
    3. Transforms currency to USD
    4. Joins with product catalog
    5. Writes enriched data to Delta table
    """
]


def run_example():
    """Run an example ETL agent workflow."""
    try:
        settings = get_settings()
        
        # Use the first example user story
        user_story = EXAMPLE_USER_STORIES[0]
        
        print("Running ETL Agent with example user story...")
        print(f"\nUser Story:\n{user_story}\n")
        
        workflow = ETLAgentWorkflow(settings)
        result = workflow.run(user_story)
        
        # Display results
        if result.get("error"):
            print(f"❌ Error: {result['error']}")
        else:
            if result.get("pipeline_code"):
                print(f"✅ Pipeline: {result['pipeline_code'].file_name}")
            if result.get("test_code"):
                print(f"✅ Tests: {result['test_code'].file_name}")
            if result.get("pr_url"):
                print(f"✅ PR: {result['pr_url']}")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    run_example()
