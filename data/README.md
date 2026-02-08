# Sample Datasets

This directory contains sample datasets for different domains that can be used with the ETL Agent.

## Available Datasets

### Telecom
- **File**: `telecom/customer_data.csv`
- **Metadata**: `telecom/schema.json`
- **Description**: Customer data including subscriptions, usage, and billing information

### Healthcare
- **File**: `healthcare/patient_records.csv`
- **Metadata**: `healthcare/schema.json`
- **Description**: Patient medical records with diagnoses, treatments, and outcomes

## Usage

Reference datasets in your user stories like:
- "Use the telecom dataset to..."
- "Process the healthcare patient records..."
- "ETL the customer data from telecom dataset..."

The agent will automatically load the schema and use it when generating pipelines.
