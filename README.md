# ETL Agent

An intelligent ETL agent that converts DevOps user stories in natural language into PySpark data pipeline code, generates tests, and creates GitHub PRs.

## Features

- **Natural Language Processing**: Converts user stories into executable PySpark code
- **Code Generation**: Generates production-ready PySpark data pipelines
- **Test Generation**: Automatically creates comprehensive tests for pipelines
- **GitHub Integration**: Automatically raises PRs with generated code
- **LangGraph Workflow**: Uses LangGraph for orchestration and state management
- **Sample Datasets**: Pre-configured sample data for telecom and healthcare domains
- **Schema-Aware Generation**: Automatically detects and uses dataset schemas from metadata
- **Code Validation**: Syntax and linting checks before PR creation
- **Code Review**: LLM-based code review with quality scores
- **Auto-Documentation**: Generates comprehensive documentation for pipelines
- **Streamlit UI**: Interactive web interface with real-time workflow tracking

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

**Option A: Streamlit UI (Recommended)**
```bash
streamlit run streamlit_app.py
```
The Streamlit UI provides:
- Dataset selection dropdown
- Interactive query input
- Real-time workflow progress tracking
- Color-coded status indicators
- Download generated notebooks

**Option B: Command Line**
```bash
python -m etl_agent.main --user-story "Your user story here"
```

## Sample Datasets

The agent includes sample datasets for testing:

- **Telecom**: Customer data with subscriptions, usage, and billing (`data/telecom/`)
- **Healthcare**: Patient records with diagnoses, treatments, and outcomes (`data/healthcare/`)

Each dataset includes:
- CSV file with sample data
- JSON metadata file describing the schema
- Field types, descriptions, and constraints

### Example Usage with Datasets

```bash
# Reference the telecom dataset
python -m etl_agent.main --user-story "Analyze the telecom customer data to calculate total revenue by subscription plan"

# Reference the healthcare dataset
python -m etl_agent.main --user-story "Process healthcare patient records to calculate treatment costs by department"
```

The agent automatically detects dataset references and uses the schema metadata when generating code.

**Note**: Generated pipelines and tests are created as Jupyter notebooks (`.ipynb`) for easy reading, understanding, and interactive execution.

### Example: Data Transformation Pipeline

Here's a comprehensive example showing how the generated pipeline transforms and enriches data:

```bash
python -m etl_agent.main --user-story "Create an ETL pipeline using the telecom dataset that:
- Filters for active customers only
- Adds a new column 'customer_segment' based on data usage: 'High' if >100GB, 'Medium' if 50-100GB, 'Low' if <50GB
- Calculates 'months_as_customer' from signup_date to current date
- Adds 'revenue_tier' column: 'High Value' if monthly_bill >100, 'Standard' if 50-100, 'Basic' if <50
- Removes customers with null email addresses
- Converts phone_number format to standard format (remove dashes)
- Outputs transformed data to Parquet format"
```

**What the generated pipeline does:**
1. **Reads** the telecom customer data CSV
2. **Filters** data (active customers, non-null emails)
3. **Transforms** existing columns (phone number formatting)
4. **Adds new calculated columns** (customer_segment, months_as_customer, revenue_tier)
5. **Validates** data quality
6. **Writes** enriched data to Parquet

The generated PySpark code will include:
- Schema-aware field references using exact column names
- Data type conversions and validations
- Conditional logic for segmentation
- Date calculations
- Data quality checks
- Proper error handling and logging

This demonstrates how the agent generates production-ready ETL code that actually modifies and enriches the underlying dataset.

### Simple Transformation Example

For a simpler example showing basic data modification:

```bash
python -m etl_agent.main --user-story "Transform the telecom customer data by:
- Converting monthly_bill from USD to EUR (multiply by 0.92)
- Adding a 'usage_category' column: 'Heavy' if data_usage_gb > 100, else 'Light'
- Filtering out cancelled accounts
- Writing to a new CSV file"
```

**Generated pipeline will:**
- Read `data/telecom/customer_data.csv`
- Apply currency conversion (creates new column or modifies existing)
- Add categorical column based on usage thresholds
- Filter rows based on status
- Write transformed data to output file

The agent ensures all transformations use the correct field names and data types from the schema metadata.

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
