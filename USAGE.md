# ETL Agent Usage Guide

## Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your credentials
```

3. **Run the agent:**
```bash
python -m etl_agent.main --user-story "Your user story here"
```

## Command Line Options

```bash
python -m etl_agent.main --help
```

Options:
- `--user-story`: User story in natural language (required)
- `--file`: Path to file containing user story (alternative to --user-story)
- `--config`: Path to custom config file (optional)

## Example User Stories

### Example 1: Simple Data Transformation
```
As a data engineer, I want to create a pipeline that:
- Reads customer data from a CSV file
- Filters customers with age > 18
- Calculates total purchase amount per customer
- Writes the results to a Parquet file
```

### Example 2: Log Processing
```
As a DevOps engineer, I need a data pipeline that:
- Ingests log files from S3 bucket
- Parses log entries and extracts error messages
- Aggregates errors by hour and service
- Outputs aggregated results to a database table
```

### Example 3: Complex ETL
```
User Story: Build an ETL pipeline that:
1. Reads sales transactions from JSON files
2. Validates data quality (no nulls, valid dates)
3. Transforms currency to USD
4. Joins with product catalog
5. Writes enriched data to Delta table
```

## Workflow

The ETL Agent follows this workflow:

1. **Parse User Story**: Understands the natural language requirements
2. **Generate Pipeline**: Creates PySpark code based on the story
3. **Generate Tests**: Creates comprehensive test suite
4. **Create PR**: Opens a GitHub pull request with the code

## Configuration

### Environment Variables

Required:
- `OPENAI_API_KEY`: Your OpenAI API key
- `GITHUB_TOKEN`: GitHub personal access token
- `GITHUB_REPO`: Target repository (format: `owner/repo`)

Optional:
- `OPENAI_MODEL`: Model to use (default: `gpt-4-turbo-preview`)
- `TEMPERATURE`: LLM temperature (default: `0.7`)
- `GITHUB_BASE_BRANCH`: Base branch for PRs (default: `main`)

### GitHub Token Permissions

Your GitHub token needs these permissions:
- `repo` (full control of private repositories)
- `pull_requests` (create and manage pull requests)

## Output Structure

Generated files are organized as:
```
pipelines/
  └── <pipeline_name>.py

tests/
  └── test_<pipeline_name>.py
```

## Troubleshooting

### Common Issues

1. **API Key Error**: Ensure `OPENAI_API_KEY` is set in `.env`
2. **GitHub Authentication**: Verify `GITHUB_TOKEN` has correct permissions
3. **Repository Access**: Ensure token has access to the target repository
4. **Branch Conflicts**: If branch exists, the agent will reuse it

### Debug Mode

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Advanced Usage

### Programmatic Usage

```python
from etl_agent.config import get_settings
from etl_agent.agent.workflow import ETLAgentWorkflow

settings = get_settings()
workflow = ETLAgentWorkflow(settings)
result = workflow.run("Your user story here")

if result.get("pr_url"):
    print(f"PR created: {result['pr_url']}")
```

### Custom Workflow

You can extend the workflow by modifying `etl_agent/agent/workflow.py` to add custom nodes or modify the graph structure.
