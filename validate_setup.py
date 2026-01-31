"""Validate that the ETL Agent setup is correct."""

import sys

def validate_imports():
    """Validate that all imports work correctly."""
    errors = []
    
    try:
        from etl_agent.config import get_settings
        print("✓ Config import works")
    except Exception as e:
        errors.append(f"Config import failed: {e}")
    
    try:
        from etl_agent.agent.state import AgentState, PipelineCode, TestCode
        print("✓ Agent state imports work")
    except Exception as e:
        errors.append(f"Agent state import failed: {e}")
    
    try:
        from etl_agent.generators.pipeline_generator import PipelineGenerator
        print("✓ Pipeline generator import works")
    except Exception as e:
        errors.append(f"Pipeline generator import failed: {e}")
    
    try:
        from etl_agent.generators.test_generator import TestGenerator
        print("✓ Test generator import works")
    except Exception as e:
        errors.append(f"Test generator import failed: {e}")
    
    try:
        from etl_agent.github.client import GitHubClient
        print("✓ GitHub client import works")
    except Exception as e:
        errors.append(f"GitHub client import failed: {e}")
    
    try:
        from etl_agent.agent.workflow import ETLAgentWorkflow
        print("✓ Workflow import works")
    except Exception as e:
        errors.append(f"Workflow import failed: {e}")
    
    if errors:
        print("\n❌ Errors found:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print("\n✅ All imports validated successfully!")
    return True

if __name__ == "__main__":
    success = validate_imports()
    sys.exit(0 if success else 1)
