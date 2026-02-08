"""Streamlit UI for ETL Agent."""

import streamlit as st
import json
import time
import uuid
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

from etl_agent.config import get_settings
from etl_agent.agent.workflow import ETLAgentWorkflow
from etl_agent.utils.dataset_loader import DatasetLoader
from etl_agent.agent.state import AgentState
from etl_agent.utils.streamlit_logger import setup_streamlit_logging


# Page configuration
st.set_page_config(
    page_title="ETL Agent",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .status-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border-left: 4px solid;
    }
    .status-processing {
        background-color: #e3f2fd;
        border-color: #2196f3;
    }
    .status-success {
        background-color: #e8f5e9;
        border-color: #4caf50;
    }
    .status-error {
        background-color: #ffebee;
        border-color: #f44336;
    }
    .status-pending {
        background-color: #f5f5f5;
        border-color: #9e9e9e;
    }
    .workflow-step {
        padding: 0.75rem;
        margin: 0.25rem 0;
        border-radius: 0.25rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .step-icon {
        font-size: 1.2rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_dataset_loader():
    """Load dataset loader."""
    return DatasetLoader(data_dir="data")


@st.cache_resource
def load_settings():
    """Load application settings."""
    try:
        return get_settings()
    except Exception as e:
        st.error(f"Failed to load settings: {e}")
        st.info("Please ensure .env file is configured correctly")
        return None


def get_or_create_session_id():
    """Get or create a session ID and add it to URL."""
    # Check URL parameters first
    query_params = st.query_params
    session_id = query_params.get("session_id", None)
    
    if not session_id:
        # Generate new session ID
        session_id = str(uuid.uuid4())[:8]
        # Update URL with session ID
        st.query_params["session_id"] = session_id
        st.rerun()
    
    # Initialize session state for this session
    if f"session_{session_id}" not in st.session_state:
        st.session_state[f"session_{session_id}"] = {
            "workflow_state": None,
            "workflow_running": False,
            "logs": [],
            "created_at": datetime.now().isoformat()
        }
    
    return session_id


def get_session_data(session_id: str):
    """Get session data."""
    return st.session_state.get(f"session_{session_id}", {
        "workflow_state": None,
        "workflow_running": False,
        "logs": [],
        "created_at": datetime.now().isoformat()
    })


def add_log(session_id: str, level: str, message: str):
    """Add a log entry to session."""
    session_data = get_session_data(session_id)
    if "logs" not in session_data:
        session_data["logs"] = []
    
    log_entry = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "level": level,
        "message": message
    }
    session_data["logs"].append(log_entry)
    # Keep only last 100 logs
    session_data["logs"] = session_data["logs"][-100:]
    st.session_state[f"session_{session_id}"] = session_data


def get_status_color(status: str) -> str:
    """Get color for status."""
    colors = {
        "processing": "#2196f3",
        "success": "#4caf50",
        "error": "#f44336",
        "pending": "#9e9e9e"
    }
    return colors.get(status, "#9e9e9e")


def get_status_icon(status: str) -> str:
    """Get icon for status."""
    icons = {
        "processing": "⏳",
        "success": "✅",
        "error": "❌",
        "pending": "⏸️"
    }
    return icons.get(status, "⏸️")


def display_workflow_step(step_name: str, status: str, details: Optional[str] = None):
    """Display a workflow step with status."""
    color = get_status_color(status)
    icon = get_status_icon(status)
    
    status_text = {
        "processing": "Processing...",
        "success": "Completed",
        "error": "Failed",
        "pending": "Pending"
    }.get(status, "Unknown")
    
    html = f"""
    <div class="workflow-step" style="background-color: {color}20; border-left: 4px solid {color};">
        <span class="step-icon">{icon}</span>
        <div>
            <strong>{step_name}</strong>
            <div style="font-size: 0.9em; color: {color};">{status_text}</div>
            {f'<div style="font-size: 0.8em; margin-top: 0.25rem; color: #666;">{details}</div>' if details else ''}
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def display_state_info(state: AgentState):
    """Display information from agent state."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Dataset Information")
        if state.get("dataset_info"):
            dataset = state["dataset_info"]
            st.info(f"**Dataset**: {dataset.dataset_name}\n\n**Domain**: {dataset.domain}\n\n**File**: {dataset.file_path}")
        else:
            st.warning("No dataset detected")
    
    with col2:
        st.subheader("📝 Current Step")
        step = state.get("step", "unknown")
        st.code(step)


def main():
    """Main Streamlit app."""
    # Get or create session ID
    session_id = get_or_create_session_id()
    
    st.title("🚀 ETL Agent")
    st.markdown("Convert natural language user stories into PySpark data pipelines")
    
    # Display session ID in sidebar
    with st.sidebar:
        st.caption(f"**Session ID**: `{session_id}`")
        st.caption(f"🔗 Share this URL to continue this session")
        if st.button("🔄 New Session"):
            # Generate new session ID
            new_session_id = str(uuid.uuid4())[:8]
            st.query_params["session_id"] = new_session_id
            st.rerun()
    
    # Load resources
    dataset_loader = load_dataset_loader()
    settings = load_settings()
    
    if not settings:
        st.stop()
    
    # Get session data
    session_data = get_session_data(session_id)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Dataset selection
        st.subheader("📊 Select Dataset")
        datasets = dataset_loader.list_datasets()
        
        if not datasets:
            st.warning("No datasets found. Please ensure data directory exists.")
            st.stop()
        
        selected_dataset_name = st.selectbox(
            "Choose a dataset:",
            options=["None"] + datasets,
                    index=0,
                    help="Select a dataset to use for ETL operations"
        )
        
        if selected_dataset_name != "None":
            dataset = dataset_loader.get_dataset(selected_dataset_name)
            if dataset:
                with st.expander("📋 Dataset Schema", expanded=False):
                    st.text(dataset_loader.get_schema_description(dataset))
        
        st.divider()
        
        # Settings info
        st.subheader("🔧 Settings")
        st.info(f"**Model**: {settings.openai_model}\n\n**Repo**: {settings.github_repo}")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Create Pipeline", "📊 Workflow Status", "📚 Results", "📋 Logs"])
    
    with tab1:
        st.header("Create ETL Pipeline")
        
        # User story input
        user_story = st.text_area(
            "Enter your ETL user story:",
            height=150,
            placeholder="Example: Transform the telecom customer data by filtering active customers and calculating total revenue by subscription plan..."
        )
        
        # Dataset context
        if selected_dataset_name != "None":
            dataset = dataset_loader.get_dataset(selected_dataset_name)
            if dataset:
                st.info(f"💡 Using dataset: **{dataset.dataset_name}** ({dataset.domain})")
                # Auto-append dataset reference to user story if not present
                if dataset.domain.lower() not in user_story.lower() and dataset.dataset_name.lower() not in user_story.lower():
                    suggested_story = f"Using the {dataset.domain} dataset, {user_story}"
                    if st.button("💡 Add dataset reference to story"):
                        user_story = suggested_story
                        st.rerun()
        
        # Generate button
        col1, col2 = st.columns([1, 4])
        with col1:
            generate_button = st.button("🚀 Generate Pipeline", type="primary", use_container_width=True)
        
        with col2:
            if st.button("🔄 Clear", use_container_width=True):
                st.session_state.clear()
                st.rerun()
        
        if generate_button:
            if not user_story.strip():
                st.error("Please enter a user story")
            else:
                # Prepare user story with dataset reference
                final_story = user_story
                if selected_dataset_name != "None" and dataset:
                    # Ensure dataset is referenced
                    if dataset.domain.lower() not in final_story.lower():
                        final_story = f"Using the {dataset.domain} dataset, {final_story}"
                
                # Store in session
                session_data["user_story"] = final_story
                session_data["workflow_running"] = True
                session_data["workflow_state"] = None
                session_data["logs"] = []  # Clear previous logs
                st.session_state[f"session_{session_id}"] = session_data
                
                # Add initial log
                add_log(session_id, "INFO", f"Starting workflow for session {session_id}")
                add_log(session_id, "INFO", f"User story: {final_story[:100]}...")
                
                # Run workflow with logging
                log_container = st.empty()
                log_handler = setup_streamlit_logging(log_container, level=logging.INFO)
                
                try:
                    add_log(session_id, "INFO", "Initializing ETL Agent workflow...")
                    workflow = ETLAgentWorkflow(settings)
                    
                    add_log(session_id, "INFO", "Running workflow steps...")
                    result = workflow.run(final_story)
                    
                    session_data["workflow_state"] = result
                    session_data["workflow_running"] = False
                    st.session_state[f"session_{session_id}"] = session_data
                    
                    add_log(session_id, "SUCCESS", "Workflow completed successfully!")
                    if result.get("pr_url"):
                        add_log(session_id, "SUCCESS", f"PR created: {result['pr_url']}")
                    
                    st.success("Workflow completed!")
                    st.rerun()
                except Exception as e:
                    error_msg = f"Workflow failed: {str(e)}"
                    add_log(session_id, "ERROR", error_msg)
                    session_data["workflow_running"] = False
                    session_data["workflow_state"] = {
                        "error": str(e),
                        "step": "error"
                    }
                    st.session_state[f"session_{session_id}"] = session_data
                    st.error(error_msg)
    
    with tab2:
        st.header("Workflow Status")
        
        workflow_state = session_data.get("workflow_state")
        if workflow_state is None:
            st.info("👆 Start by creating a pipeline in the 'Create Pipeline' tab")
        else:
            state = workflow_state
            
            # Overall status
            if state.get("error"):
                st.error(f"❌ Error: {state['error']}")
            elif state.get("pr_url"):
                st.success("✅ Pipeline generated successfully!")
            else:
                st.info("⏳ Workflow in progress...")
            
            st.divider()
            
            # Workflow steps
            st.subheader("Workflow Steps")
            
            # Step 1: Detect Dataset
            if state.get("dataset_info"):
                display_workflow_step("1. Detect Dataset", "success", f"Found: {state['dataset_info'].dataset_name}")
            elif state.get("step") == "detect_dataset":
                display_workflow_step("1. Detect Dataset", "processing")
            else:
                display_workflow_step("1. Detect Dataset", "pending")
            
            # Step 2: Generate Pipeline
            if state.get("pipeline_code"):
                display_workflow_step("2. Generate Pipeline", "success", state["pipeline_code"].file_name)
            elif state.get("step") == "generate_pipeline":
                display_workflow_step("2. Generate Pipeline", "processing")
            elif state.get("error") and "pipeline" in state.get("error", "").lower():
                display_workflow_step("2. Generate Pipeline", "error", state.get("error"))
            else:
                display_workflow_step("2. Generate Pipeline", "pending")
            
            # Step 3: Generate Tests
            if state.get("test_code"):
                display_workflow_step("3. Generate Tests", "success", state["test_code"].file_name)
            elif state.get("step") == "generate_tests":
                display_workflow_step("3. Generate Tests", "processing")
            elif state.get("error") and "test" in state.get("error", "").lower():
                display_workflow_step("3. Generate Tests", "error", state.get("error"))
            else:
                display_workflow_step("3. Generate Tests", "pending")
            
            # Step 4: Validate Code
            if state.get("validation_result"):
                validation = state["validation_result"]
                status = "success" if validation.is_valid else "error"
                details = f"Valid: {validation.is_valid}, Issues: {len(validation.linting_issues)}"
                display_workflow_step("4. Validate Code", status, details)
            elif state.get("step") == "validate_code":
                display_workflow_step("4. Validate Code", "processing")
            else:
                display_workflow_step("4. Validate Code", "pending")
            
            # Step 5: Review Code
            if state.get("code_review"):
                review = state["code_review"]
                status = "success" if review.approved else "error"
                details = f"Score: {review.score}/100, Approved: {review.approved}"
                display_workflow_step("5. Review Code", status, details)
            elif state.get("step") == "review_code":
                display_workflow_step("5. Review Code", "processing")
            else:
                display_workflow_step("5. Review Code", "pending")
            
            # Step 6: Generate Documentation
            if state.get("documentation"):
                display_workflow_step("6. Generate Documentation", "success", state["documentation"].file_name)
            elif state.get("step") == "generate_docs":
                display_workflow_step("6. Generate Documentation", "processing")
            else:
                display_workflow_step("6. Generate Documentation", "pending")
            
            # Step 7: Create PR
            if state.get("pr_url"):
                display_workflow_step("7. Create PR", "success", state["pr_url"])
            elif state.get("step") == "create_pr":
                display_workflow_step("7. Create PR", "processing")
            elif state.get("error") and "pr" in state.get("error", "").lower():
                display_workflow_step("7. Create PR", "error", state.get("error"))
            else:
                display_workflow_step("7. Create PR", "pending")
            
            # Display state info
            st.divider()
            display_state_info(state)
    
    with tab3:
        st.header("Results")
        
        workflow_state = session_data.get("workflow_state")
        if workflow_state is None:
            st.info("👆 Generate a pipeline to see results here")
        else:
            state = workflow_state
            
            # Pipeline Code
            if state.get("pipeline_code"):
                with st.expander("📓 Pipeline Notebook", expanded=True):
                    pipeline = state["pipeline_code"]
                    st.subheader(pipeline.file_name)
                    st.caption(pipeline.description)
                    
                    # Try to parse and display notebook
                    try:
                        notebook = json.loads(pipeline.code)
                        st.json(notebook)
                        
                        # Download button
                        st.download_button(
                            label="📥 Download Notebook",
                            data=pipeline.code,
                            file_name=pipeline.file_name,
                            mime="application/json"
                        )
                    except:
                        st.code(pipeline.code, language="python")
            
            # Test Code
            if state.get("test_code"):
                with st.expander("🧪 Test Notebook", expanded=False):
                    tests = state["test_code"]
                    st.subheader(tests.file_name)
                    st.caption(tests.description)
                    
                    try:
                        notebook = json.loads(tests.code)
                        st.json(notebook)
                        
                        st.download_button(
                            label="📥 Download Test Notebook",
                            data=tests.code,
                            file_name=tests.file_name,
                            mime="application/json"
                        )
                    except:
                        st.code(tests.code, language="python")
            
            # Validation Results
            if state.get("validation_result"):
                with st.expander("🔍 Validation Results", expanded=False):
                    validation = state["validation_result"]
                    
                    if validation.is_valid:
                        st.success("✅ Code validation passed")
                    else:
                        st.error("❌ Code validation failed")
                    
                    if validation.syntax_errors:
                        st.error("**Syntax Errors:**")
                        for error in validation.syntax_errors:
                            st.code(error)
                    
                    if validation.linting_issues:
                        st.warning("**Linting Issues:**")
                        for issue in validation.linting_issues:
                            st.text(f"• {issue}")
                    
                    if validation.warnings:
                        st.info("**Warnings:**")
                        for warning in validation.warnings:
                            st.text(f"• {warning}")
            
            # Code Review
            if state.get("code_review"):
                with st.expander("📝 Code Review", expanded=False):
                    review = state["code_review"]
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Score", f"{review.score}/100" if review.score else "N/A")
                    with col2:
                        st.metric("Approved", "✅ Yes" if review.approved else "❌ No")
                    
                    st.markdown("**Review:**")
                    st.text(review.review)
                    
                    if review.suggestions:
                        st.markdown("**Suggestions:**")
                        for suggestion in review.suggestions:
                            st.text(f"• {suggestion}")
            
            # Documentation
            if state.get("documentation"):
                with st.expander("📚 Documentation", expanded=False):
                    docs = state["documentation"]
                    st.subheader(docs.file_name)
                    st.markdown(docs.content)
            
            # PR URL
            if state.get("pr_url"):
                st.success("🎉 Pull Request Created!")
                st.markdown(f"**PR URL**: [{state['pr_url']}]({state['pr_url']})")
    
    with tab4:
        st.header("📋 Execution Logs")
        st.caption(f"Session: `{session_id}` | Logs are preserved across page refreshes")
        
        logs = session_data.get("logs", [])
        
        if not logs:
            st.info("No logs yet. Start a workflow to see logs here.")
        else:
            # Log viewer with auto-scroll
            log_container = st.container()
            with log_container:
                for log_entry in logs:
                    timestamp = log_entry.get("timestamp", "")
                    level = log_entry.get("level", "INFO")
                    message = log_entry.get("message", "")
                    
                    # Color code by level
                    if level == "ERROR":
                        st.error(f"`{timestamp}` ❌ {message}")
                    elif level == "WARNING":
                        st.warning(f"`{timestamp}` ⚠️ {message}")
                    elif level == "SUCCESS":
                        st.success(f"`{timestamp}` ✅ {message}")
                    else:
                        st.info(f"`{timestamp}` ℹ️ {message}")
            
            # Auto-refresh if workflow is running
            if session_data.get("workflow_running", False):
                time.sleep(2)
                st.rerun()
            
            # Clear logs button
            if st.button("🗑️ Clear Logs"):
                session_data["logs"] = []
                st.session_state[f"session_{session_id}"] = session_data
                st.rerun()


if __name__ == "__main__":
    main()
