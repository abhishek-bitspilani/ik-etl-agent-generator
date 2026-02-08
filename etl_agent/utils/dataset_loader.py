"""Dataset loader for sample data and metadata."""

import json
import os
from pathlib import Path
from typing import Dict, Optional, List
from pydantic import BaseModel


class FieldSchema(BaseModel):
    """Schema definition for a single field."""
    type: str
    description: str
    nullable: bool = True
    primary_key: bool = False
    format: Optional[str] = None
    enum: Optional[List[str]] = None
    min: Optional[float] = None
    max: Optional[float] = None


class DatasetSchema(BaseModel):
    """Complete dataset schema."""
    dataset_name: str
    domain: str
    description: str
    file_path: str
    fields: Dict[str, FieldSchema]  # Renamed from 'schema' to avoid BaseModel shadowing
    sample_queries: Optional[List[str]] = None


class DatasetLoader:
    """Loads dataset metadata and provides schema information."""
    
    def __init__(self, data_dir: str = "data"):
        """Initialize the dataset loader."""
        self.data_dir = Path(data_dir)
        self._datasets: Dict[str, DatasetSchema] = {}
        self._load_all_datasets()
    
    def _load_all_datasets(self):
        """Load all available datasets."""
        if not self.data_dir.exists():
            return
        
        # Look for schema.json files in subdirectories
        for schema_file in self.data_dir.rglob("schema.json"):
            try:
                with open(schema_file, 'r') as f:
                    schema_data = json.load(f)
                    # Convert nested schema dict to FieldSchema objects
                    schema_dict = {}
                    for field_name, field_data in schema_data.get("schema", {}).items():
                        schema_dict[field_name] = FieldSchema(**field_data)
                    schema_data["fields"] = schema_dict  # Use 'fields' instead of 'schema'
                    
                    dataset_schema = DatasetSchema(**schema_data)
                    self._datasets[dataset_schema.dataset_name] = dataset_schema
            except Exception as e:
                print(f"Warning: Failed to load dataset from {schema_file}: {e}")
    
    def get_dataset(self, dataset_name: str) -> Optional[DatasetSchema]:
        """Get a dataset by name."""
        return self._datasets.get(dataset_name)
    
    def find_dataset_by_domain(self, domain: str) -> Optional[DatasetSchema]:
        """Find a dataset by domain name."""
        for dataset in self._datasets.values():
            if dataset.domain.lower() == domain.lower():
                return dataset
        return None
    
    def find_dataset_by_reference(self, text: str) -> Optional[DatasetSchema]:
        """Find a dataset by reference in text (e.g., 'telecom dataset')."""
        text_lower = text.lower()
        
        # Check for domain references
        for dataset in self._datasets.values():
            if dataset.domain.lower() in text_lower:
                return dataset
            if dataset.dataset_name.lower().replace("_", " ") in text_lower:
                return dataset
        
        # Check for common aliases
        if "telecom" in text_lower or "customer" in text_lower:
            return self.find_dataset_by_domain("telecom")
        if "healthcare" in text_lower or "patient" in text_lower or "medical" in text_lower:
            return self.find_dataset_by_domain("healthcare")
        
        return None
    
    def list_datasets(self) -> List[str]:
        """List all available dataset names."""
        return list(self._datasets.keys())
    
    def get_schema_description(self, dataset: DatasetSchema) -> str:
        """Get a formatted description of the dataset schema."""
        lines = [
            f"Dataset: {dataset.dataset_name}",
            f"Domain: {dataset.domain}",
            f"Description: {dataset.description}",
            f"File: {dataset.file_path}",
            "",
            "Schema Fields:"
        ]
        
        for field_name, field_schema in dataset.fields.items():
            field_desc = f"  - {field_name} ({field_schema.type})"
            if field_schema.primary_key:
                field_desc += " [PRIMARY KEY]"
            if not field_schema.nullable:
                field_desc += " [NOT NULL]"
            if field_schema.enum:
                field_desc += f" [Values: {', '.join(field_schema.enum)}]"
            field_desc += f": {field_schema.description}"
            lines.append(field_desc)
        
        if dataset.sample_queries:
            lines.append("")
            lines.append("Sample Queries:")
            for query in dataset.sample_queries:
                lines.append(f"  - {query}")
        
        return "\n".join(lines)
    
    def get_schema_for_prompt(self, dataset: DatasetSchema) -> str:
        """Get schema information formatted for LLM prompts."""
        schema_info = {
            "dataset_name": dataset.dataset_name,
            "domain": dataset.domain,
            "description": dataset.description,
            "file_path": dataset.file_path,
            "fields": {}
        }
        
        for field_name, field_schema in dataset.fields.items():
            schema_info["fields"][field_name] = {
                "type": field_schema.type,
                "description": field_schema.description,
                "nullable": field_schema.nullable,
                "primary_key": field_schema.primary_key
            }
            if field_schema.enum:
                schema_info["fields"][field_name]["enum"] = field_schema.enum
            if field_schema.format:
                schema_info["fields"][field_name]["format"] = field_schema.format
        
        return json.dumps(schema_info, indent=2)
