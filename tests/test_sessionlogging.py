"""
Basic tests for the sessionlogging package.
"""
import tempfile
from pathlib import Path

import pytest

from sessionlogging import Session


class TestSession:
    """Test cases for the Session class."""
    
    def test_session_creation(self):
        """Test that a Session can be created successfully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            session = Session(
                session_name="test_session",
                log_path=temp_dir
            )
            assert session.session_name == "test_session"
            assert session.log_path == Path(temp_dir)
    
    def test_session_logging(self):
        """Test basic logging functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            session = Session(
                session_name="test_logging",
                log_path=temp_dir
            )
            
            # Test basic logging
            session.log("Test message")
            session.warn("Test warning")
            
            # Check that log file was created
            log_files = list(Path(temp_dir).glob("*.log"))
            assert len(log_files) > 0
    
    def test_phase_tracking(self):
        """Test phase tracking functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            session = Session(
                session_name="test_phases",
                log_path=temp_dir
            )
            
            with session.phase("test_phase") as phase:
                session.log("Inside phase")
                phase.add_metric("test_metric", 42)
            
            # Check that phase was recorded
            assert "test_phase" in session.phases
            assert session.phases["test_phase"].metrics["test_metric"] == 42
    
    def test_session_summary(self):
        """Test session summary generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            session = Session(
                session_name="test_summary",
                log_path=temp_dir
            )
            
            session.log("Test message")
            
            with session.phase("test_phase"):
                session.log("Phase message")
            
            # Generate summary
            summary = session.get_summary()
            
            assert summary is not None
            assert "session_info" in summary
            assert "phases" in summary
            assert summary["session_info"]["session_name"] == "test_summary"


class TestSessionInfo:
    """Test cases for SessionInfo functionality."""
    
    def test_system_info_capture(self):
        """Test that system information is captured properly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            session = Session(
                session_name="test_sysinfo",
                log_path=temp_dir
            )
            
            summary = session.get_summary()
            session_info = summary["session_info"]
            
            # Check that basic system info is present
            assert "python_version" in session_info
            assert "platform" in session_info
            assert "cpu_count" in session_info
            assert "memory_total_gb" in session_info


if __name__ == "__main__":
    pytest.main([__file__])
