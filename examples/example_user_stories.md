# Example User Stories with Datasets

## Telecom Domain Examples

### Example 1: Revenue Analysis
```
As a data analyst, I want to analyze the telecom customer data to:
- Calculate total monthly revenue by subscription plan
- Identify customers with data usage above 100GB
- Generate a report showing average monthly bill by region
- Output results to a Parquet file
```

### Example 2: Customer Segmentation
```
Create an ETL pipeline using the telecom dataset that:
- Segments customers into high, medium, and low usage tiers
- Calculates customer lifetime value based on monthly bill and signup date
- Filters for active customers only
- Writes segmented data to a Delta table
```

### Example 3: Churn Analysis
```
Build a pipeline that processes the telecom customer data to:
- Identify customers at risk of churn (high data usage but low-tier plan)
- Calculate days since signup for each customer
- Flag customers with suspended or cancelled status
- Output churn risk analysis to CSV
```

## Healthcare Domain Examples

### Example 1: Cost Analysis
```
As a healthcare administrator, I want to analyze patient records to:
- Calculate total treatment costs by department
- Find the average cost per treatment type
- Identify the most expensive treatments
- Generate a summary report in Parquet format
```

### Example 2: Patient Outcomes
```
Create an ETL pipeline using the healthcare dataset that:
- Analyzes treatment outcomes by medication
- Groups patients by diagnosis code
- Calculates recovery rates by department
- Outputs enriched patient data to a Delta table
```

### Example 3: Insurance Analysis
```
Build a pipeline that processes healthcare patient records to:
- Analyze treatment costs by insurance type
- Calculate average costs for Medicare vs Private insurance
- Identify departments with highest costs per insurance type
- Output analysis results to Parquet
```

## Usage

Run the ETL agent with any of these user stories:

```bash
python -m etl_agent.main --user-story "Your user story here"
```

The agent will automatically:
1. Detect the dataset reference (telecom or healthcare)
2. Load the schema metadata
3. Generate pipeline code using the correct field names and types
4. Create tests and documentation
5. Open a PR with the generated code
