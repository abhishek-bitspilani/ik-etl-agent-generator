"""Streamlit-compatible logger that displays logs in UI."""

import logging
import streamlit as st
from typing import Optional
import io
import sys


class StreamlitHandler(logging.Handler):
    """Logging handler that writes to Streamlit."""
    
    def __init__(self, container=None):
        super().__init__()
        self.container = container
        self.log_buffer = []
    
    def emit(self, record):
        """Emit a log record."""
        try:
            msg = self.format(record)
            self.log_buffer.append((record.levelno, msg))
            
            # Also write to container if available
            if self.container is not None:
                with self.container:
                    if record.levelno >= logging.ERROR:
                        st.error(f"❌ {msg}")
                    elif record.levelno >= logging.WARNING:
                        st.warning(f"⚠️ {msg}")
                    elif record.levelno >= logging.INFO:
                        st.info(f"ℹ️ {msg}")
                    else:
                        st.text(f"🔍 {msg}")
        except Exception:
            self.handleError(record)
    
    def get_logs(self):
        """Get all logged messages."""
        return self.log_buffer.copy()


def setup_streamlit_logging(container=None, level=logging.INFO):
    """Set up logging for Streamlit."""
    # Create handler
    handler = StreamlitHandler(container)
    handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    # Get root logger and add handler
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)
    
    return handler
