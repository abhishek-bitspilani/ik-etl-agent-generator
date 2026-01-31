# ETL Agent Architecture

## Overview

The ETL Agent is built using LangGraph to orchestrate a multi-step workflow that converts natural language user stories into production-ready PySpark pipelines, generates tests, and creates GitHub PRs.

## Architecture Components

### 1. LangGraph Workflow (`etl_agent/agent/workflow.py`)

The core orchestration engine using LangGraph's StateGraph:

```
┌─────────────────┐
│  User Story     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Generate        │
│ Pipeline        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Generate        │
│ Tests           │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Validate        │
│ Code            │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Review          │
│ Code            │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Generate        │
│ Documentation   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Create          │
│ PR              │
└────────┬────────┘
         │
         ▼
      [END]
```

**State Management:**
- Uses `AgentState` TypedDict to track workflow progress
- Maintains pipeline code, test code, PR URL, and error states
- Each node updates the state and passes it to the next

### 2. Code Generators

#### Pipeline Generator (`etl_agent/generators/pipeline_generator.py`)
- Uses LangChain with OpenAI to generate PySpark code
- Converts natural language to executable PySpark pipelines
- Includes best practices: error handling, logging, validation

#### Test Generator (`etl_agent/generators/test_generator.py`)
- Generates comprehensive pytest test suites
- Creates unit and integration tests
- Includes mock data and fixtures

#### Review Generator (`etl_agent/generators/review_generator.py`)
- LLM-based code review for quality assurance
- Provides suggestions and quality scores
- Determines approval status

#### Documentation Generator (`etl_agent/generators/doc_generator.py`)
- Generates comprehensive Markdown documentation
- Includes architecture, usage, and troubleshooting guides
- Auto-documents pipeline functionality

### 3. Code Validation (`etl_agent/utils/validator.py`)

**CodeValidator** provides:
- Python syntax validation using AST
- PySpark import checks
- Best practices validation
- Basic linting (line length, trailing whitespace, etc.)

### 4. GitHub Integration (`etl_agent/github/client.py`)

**Features:**
- Creates branches from base branch
- Creates/updates files in repository
- Opens pull requests with generated code
- Handles branch conflicts gracefully
- Handles empty repositories

### 5. Configuration (`etl_agent/config.py`)

Uses Pydantic Settings for:
- Environment variable management
- Type-safe configuration
- Default values

## Data Flow

1. **Input**: User story (natural language)
2. **Processing**:
   - LLM generates PySpark code
   - LLM generates test code
   - GitHub API creates PR
3. **Output**: GitHub PR with pipeline and tests

## Key Design Decisions

### LangGraph Choice
- **State Management**: TypedDict provides type safety
- **Workflow Control**: Easy to add/remove steps
- **Error Handling**: Can add conditional edges for error paths
- **Extensibility**: Simple to add new nodes (e.g., code review, validation)

### LLM Integration
- Uses LangChain for abstraction
- Supports multiple LLM providers
- Configurable model and temperature
- Structured output parsing

### GitHub Integration
- Uses PyGithub for API access
- Handles file creation and updates
- Automatic branch naming
- PR body includes context

## Extension Points

### Adding New Workflow Steps

1. Create a new node function:
```python
def _new_step(self, state: AgentState) -> AgentState:
    # Your logic here
    return state
```

2. Add to graph:
```python
workflow.add_node("new_step", self._new_step)
workflow.add_edge("previous_step", "new_step")
```

### Custom Code Generators

Extend `PipelineGenerator` or `TestGenerator` to:
- Use different LLM providers
- Add custom prompts
- Implement code templates
- Add validation steps

### Enhanced Error Handling

Add conditional edges:
```python
workflow.add_conditional_edges(
    "generate_pipeline",
    self._should_continue,
    {
        "continue": "generate_tests",
        "error": END
    }
)
```

## Implemented Enhancements

1. **✅ Code Review Node**: LLM-based code review before PR
   - Reviews code quality, performance, and best practices
   - Provides suggestions and quality score
   - Approval status for PR readiness

2. **✅ Validation Node**: Syntax and linting checks
   - Python syntax validation
   - PySpark import checks
   - Best practices validation
   - Linting for code quality

3. **✅ Documentation Node**: Auto-generate pipeline docs
   - Comprehensive Markdown documentation
   - Includes architecture, usage, and troubleshooting
   - Automatically included in PR

## Future Enhancements

1. **Multi-file Support**: Handle complex pipelines with multiple files
2. **Template System**: Use code templates for common patterns
3. **Feedback Loop**: Learn from PR reviews to improve generation
4. **Interactive Mode**: Allow user to approve/reject suggestions
5. **CI/CD Integration**: Automatic pipeline deployment
