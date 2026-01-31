# ETL Agent

An intelligent ETL agent that converts DevOps user stories in natural language into PySpark data pipeline code, generates tests, and creates GitHub PRs.

## Features

- **Natural Language Processing**: Converts user stories into executable PySpark code
- **Code Generation**: Generates production-ready PySpark data pipelines
- **Test Generation**: Automatically creates comprehensive tests for pipelines
- **GitHub Integration**: Automatically raises PRs with generated code
- **LangGraph Workflow**: Uses LangGraph for orchestration and state management

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys and GitHub credentials
```

3. Run the agent:
```bash
python -m etl_agent.main --user-story "Your user story here"
```

## Configuration

- `OPENAI_API_KEY`: Your OpenAI API key for LLM operations
- `GITHUB_TOKEN`: GitHub personal access token
- `GITHUB_REPO`: Target repository (format: owner/repo)
- `GITHUB_BASE_BRANCH`: Base branch for PRs (default: main)

## Project Structure

```
etl_agent/
├── agent/          # LangGraph agent implementation
├── generators/     # Code and test generators
├── github/         # GitHub API integration
├── utils/          # Utility functions
└── main.py         # Entry point
```
