
"""
Advanced logging module with performance monitoring and phase tracking.

This module provides a comprehensive logging solution that goes beyond standard
Python logging by including performance monitoring, system resource tracking,
and structured phase management. It's designed for data processing pipelines,
machine learning workflows, and other compute-intensive applications where
detailed performance analysis is valuable.

Key Features
------------
- Automatic system information capture (CPU, memory, git info, etc.)
- Phase-based execution tracking with context managers
- Real-time performance monitoring (memory and CPU usage)
- Detailed session summaries with JSON export
- Custom metrics support for domain-specific measurements
- Structured data logging with intelligent formatting
- Warning and error aggregation for easy debugging

Classes
-------
SessionInfo : dataclass
    Container for session metadata and system information.
PhaseMetrics : dataclass
    Performance metrics and statistics for individual processing phases.
Session : class
    Main logging session with performance monitoring capabilities.

Examples
--------
Basic usage:

>>> from sessionlogging import Session
>>> session = Session(session_name="data_pipeline")
>>> session.start()
>>> with session.phase("data_loading"):
...     # Your data loading code here
...     session.monitor_system_performance()
>>> session.finalize()

Advanced usage with custom metrics:

>>> session = Session("ml_training", retain_samples=True)
>>> config = {"batch_size": 32, "learning_rate": 0.001}
>>> session.start(configuration=config)
>>> with session.phase("training") as phase:
...     # Training code here
...     phase.records_processed = 10000
...     phase.custom_metrics = {"accuracy": 0.95, "loss": 0.05}
>>> session.finalize()

Notes
-----
The module requires the following optional dependencies for full functionality:
- psutil: For system performance monitoring
- gitpython: For git repository information (optional)

See Also
--------
logging : Python standard library logging module
psutil : System and process utilities
"""

import datetime
import json
import logging
import os
import platform
import psutil  # type: ignore
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import git  # type: ignore
except ImportError:
    git = None


@dataclass
class SessionInfo:
    """
    Session metadata and system information container.

    This dataclass stores comprehensive information about a logging session,
    including system specifications, environment details, and version control
    information. Used to provide context for performance monitoring and
    debugging purposes.

    Attributes
    ----------
    name : str
        The name of the session, typically describing the application or
        process.
    session_id : str
        Unique identifier for the session, usually timestamp-based.
    timestamp : str
        ISO format timestamp when the session was created.
    git_branch : str
        Current git branch name, or 'unknown'/'unavailable' if not available.
    git_commit : str
        Short hash of the current git commit (8 characters).
    python_version : str
        Python version string (e.g., '3.9.0').
    environment : str
        Environment name (e.g., 'local', 'staging', 'production').
    hostname : str
        Machine hostname where the session is running.
    os_info : str
        Operating system name and version.
    cpu_cores : int
        Number of CPU cores available on the system.
    memory_gb : float
        Total system memory in gigabytes.

    Examples
    --------
    >>> session_info = SessionInfo(
    ...     name="data_pipeline",
    ...     session_id="session_20231210_143022",
    ...     timestamp="2023-12-10T14:30:22.123456",
    ...     git_branch="main",
    ...     git_commit="a1b2c3d4",
    ...     python_version="3.9.0",
    ...     environment="production",
    ...     hostname="worker-01",
    ...     os_info="Linux 5.4.0",
    ...     cpu_cores=8,
    ...     memory_gb=32.0
    ... )
    """
    name: str
    session_id: str
    timestamp: str
    git_branch: str
    git_commit: str
    python_version: str
    environment: str
    hostname: str
    os_info: str
    cpu_cores: int
    memory_gb: float


@dataclass
class PhaseMetrics:
    """
    Performance metrics and statistics for a processing phase.

    This dataclass captures comprehensive performance metrics for a single
    phase of execution, including timing, resource usage, data processing
    statistics, and custom metrics. Used for detailed performance analysis
    and optimization.

    Attributes
    ----------
    phase_name : str
        Name of the processing phase (e.g., 'data_loading', 'preprocessing').
    start_time : float
        Unix timestamp when the phase started.
    end_time : float, optional
        Unix timestamp when the phase ended. None if phase is still running.
    duration : float, optional
        Total duration of the phase in seconds. Calculated automatically.
    memory_peak_gb : float, optional
        Peak memory usage during the phase in gigabytes.
    memory_avg_gb : float, optional
        Average memory usage during the phase in gigabytes.
    cpu_avg_percent : float, optional
        Average CPU utilization during the phase as a percentage.
    input_data_stats : dict, optional
        Dictionary containing statistics about input data (e.g., shape, size).
    output_data_stats : dict, optional
        Dictionary containing statistics about output data.
    phase_metrics : dict, optional
        Additional phase-specific metrics.
    records_processed : int, optional
        Number of data records processed during the phase.
    throughput_per_second : float, optional
        Processing throughput in records per second. Calculated automatically.
    custom_metrics : dict, optional
        User-defined custom metrics specific to the phase.
    performance_timeline : list of dict, optional
        Detailed performance samples over time. Each dict contains timestamp,
        memory_gb, and cpu_percent keys.

    Examples
    --------
    >>> phase = PhaseMetrics(
    ...     phase_name="data_processing",
    ...     start_time=1638360000.0,
    ...     records_processed=10000
    ... )
    >>> # After phase completion
    >>> phase.end_time = 1638360120.0
    >>> phase.duration = 120.0
    >>> phase.throughput_per_second = 83.33
    """
    phase_name: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    memory_peak_gb: Optional[float] = None
    memory_avg_gb: Optional[float] = None
    cpu_avg_percent: Optional[float] = None

    input_data_stats: Optional[Dict[str, Any]] = None
    output_data_stats: Optional[Dict[str, Any]] = None
    phase_metrics: Optional[Dict[str, Any]] = None
    records_processed: Optional[int] = None
    throughput_per_second: Optional[float] = None
    custom_metrics: Optional[Dict[str, Any]] = None

    performance_timeline: Optional[List[Dict[str, Any]]] = None


class Session:
    """
    Advanced logging session with performance monitoring and phase tracking.

    The Session class provides comprehensive logging capabilities with built-in
    performance monitoring, system resource tracking, and structured phase
    management. It automatically captures system information, tracks memory
    and CPU usage, and provides detailed session summaries.

    Parameters
    ----------
    session_name : str, optional
        Name for the logging session. If None, defaults to 'session'.
    session_id : str, optional
        Unique identifier for the session. If None, auto-generated with
        timestamp.
    log_level : str, default 'INFO'
        Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    output_directory : str, default 'logs'
        Directory path where log files and summaries will be stored.
    enable_console_output : bool, default True
        Whether to output log messages to console/stdout.
    enable_file_output : bool, default True
        Whether to write log messages to file.
    max_file_size_bytes : int, default 52428800
        Maximum size of log file before rotation (50MB default).
    backup_file_count : int, default 5
        Number of backup log files to keep during rotation.
    retain_samples : bool, default False
        Whether to retain detailed performance timeline samples for analysis.

    Attributes
    ----------
    name : str
        The session name.
    session_id : str
        Unique session identifier.
    output_dir : Path
        Path object for the output directory.
    retain_samples : bool
        Whether performance samples are retained.
    session_start_time : float
        Unix timestamp when session was started.
    current_phase : PhaseMetrics
        Currently active phase metrics object.
    phases : list of PhaseMetrics
        List of completed phase metrics.
    warning_messages : list of str
        Accumulated warning messages.
    error_messages : list of str
        Accumulated error messages.
    memory_usage_samples : list of float
        Memory usage samples in GB.
    cpu_usage_samples : list of float
        CPU usage samples as percentages.
    logger : logging.Logger
        The underlying Python logger instance.
    session_info : SessionInfo
        System and session metadata.

    Examples
    --------
    Basic usage:

    >>> session = Session(session_name="data_pipeline")
    >>> session.start()
    >>> with session.phase("data_loading"):
    ...     # Your data loading code here
    ...     session.monitor_system_performance()
    >>> session.finalize()

    Advanced usage with custom metrics:

    >>> session = Session(
    ...     session_name="ml_training",
    ...     log_level="DEBUG",
    ...     retain_samples=True
    ... )
    >>> session.start(configuration={"batch_size": 32, "epochs": 100})
    >>> with session.phase("training") as phase:
    ...     # Training code here
    ...     phase.records_processed = 10000
    ...     phase.custom_metrics = {"accuracy": 0.95, "loss": 0.05}
    >>> session.finalize()

    See Also
    --------
    SessionInfo : Session metadata container
    PhaseMetrics : Performance metrics for individual phases
    """

    def __init__(
        self,
        session_name: Optional[str] = None,
        session_id: Optional[str] = None,
        log_level: str = 'INFO',
        output_directory: str = 'logs',
        enable_console_output: bool = True,
        enable_file_output: bool = True,
        max_file_size_bytes: int = 50 * 1024 * 1024,
        backup_file_count: int = 5,
        retain_samples: bool = False
    ):
        """
        Initialize a new logging session.

        Creates a new Session instance with the specified configuration,
        sets up logging handlers, gathers system information, and prepares
        for performance monitoring.

        Parameters
        ----------
        session_name : str, optional
            Name for the logging session. If None, defaults to 'session'.
        session_id : str, optional
            Unique identifier for the session. If None, auto-generated
            with timestamp format 'session_YYYYMMDD_HHMMSS'.
        log_level : str, default 'INFO'
            Logging level. Valid values: 'DEBUG', 'INFO', 'WARNING',
            'ERROR', 'CRITICAL'.
        output_directory : str, default 'logs'
            Directory path where log files and summaries will be stored.
            Directory will be created if it doesn't exist.
        enable_console_output : bool, default True
            Whether to output log messages to console/stdout.
        enable_file_output : bool, default True
            Whether to write log messages to rotating log files.
        max_file_size_bytes : int, default 52428800
            Maximum size of individual log files before rotation (50MB).
        backup_file_count : int, default 5
            Number of backup log files to retain during rotation.
        retain_samples : bool, default False
            Whether to retain detailed performance timeline samples.
            When True, enables detailed performance analysis but uses
            more memory.

        Raises
        ------
        OSError
            If the output directory cannot be created.
        ValueError
            If log_level is not a valid logging level.

        Examples
        --------
        >>> # Basic session
        >>> session = Session(session_name="my_app")

        >>> # Advanced configuration
        >>> session = Session(
        ...     session_name="data_pipeline",
        ...     log_level="DEBUG",
        ...     output_directory="/var/log/myapp",
        ...     max_file_size_bytes=100 * 1024 * 1024,  # 100MB
        ...     retain_samples=True
        ... )
        """

        self.name = session_name or 'session'
        self.session_id = session_id or self._generate_session_id()
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(exist_ok=True)
        self.retain_samples = retain_samples

        self.session_start_time = None
        self.current_phase = None
        self.phases = []
        self.warning_messages = []
        self.error_messages = []
        self.memory_usage_samples = []
        self.cpu_usage_samples = []

        self.logger = self._setup_logging_handlers(
            log_level, enable_console_output, enable_file_output,
            max_file_size_bytes, backup_file_count
        )

        self.session_info = self._gather_session_info()

    def _generate_session_id(
        self,
    ) -> str:
        """
        Generate a unique session identifier based on current timestamp.

        Creates a session ID in the format 'session_YYYYMMDD_HHMMSS'
        using the current date and time. This ensures uniqueness for
        sessions created at different times.

        Returns
        -------
        str
            Unique session identifier string.

        Examples
        --------
        >>> session = Session()
        >>> session_id = session._generate_session_id()
        >>> print(session_id)  # doctest: +SKIP
        'session_20231210_143022'
        """
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        return f'session_{timestamp}'

    def _setup_logging_handlers(
        self,
        log_level: str,
        enable_console: bool,
        enable_file: bool,
        max_log_size: int,
        backup_count: int,
    ) -> logging.Logger:
        """
        Configure and return a logger with console and/or file handlers.

        Creates a logger instance with the specified configuration including
        console output, rotating file handlers, and formatting. This is an
        internal method called during session initialization.

        Parameters
        ----------
        log_level : str
            Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        enable_console : bool
            Whether to add a console handler for stdout output.
        enable_file : bool
            Whether to add a rotating file handler.
        max_log_size : int
            Maximum size in bytes for log files before rotation.
        backup_count : int
            Number of backup files to keep during rotation.

        Returns
        -------
        logging.Logger
            Configured logger instance ready for use.
        """

        logger = logging.getLogger(f'{self.name}_{self.session_id}')
        logger.setLevel(getattr(logging, log_level.upper()))

        logger.handlers.clear()

        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        if enable_file:
            log_file = self.output_dir / f'{self.name}_{self.session_id}.log'
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_log_size,
                backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def _gather_session_info(
        self,
    ) -> SessionInfo:
        """
        Collect comprehensive system and session information.

        Gathers system specifications, git repository information,
        Python version, and environment details to create a SessionInfo
        object for context and debugging purposes.

        Returns
        -------
        SessionInfo
            Dataclass containing all gathered session metadata including
            system specs, git info, Python version, and environment details.

        Notes
        -----
        - Git information requires gitpython package and a git repository
        - Falls back to 'unknown' or 'unavailable' if information cannot
          be gathered
        - System memory is converted from bytes to gigabytes for readability
        """

        if git:
            try:
                repo = git.Repo(search_parent_directories=True)
                git_branch = repo.active_branch.name
                git_commit = repo.head.commit.hexsha[:8]
            except Exception:
                git_branch = 'unknown'
                git_commit = 'unknown'
        else:
            git_branch = 'unavailable'
            git_commit = 'unavailable'

        return SessionInfo(
            name=self.name,
            session_id=self.session_id,
            timestamp=datetime.datetime.now().isoformat(),
            git_branch=git_branch,
            git_commit=git_commit,
            python_version=sys.version.split()[0],
            environment=os.environ.get('ENVIRONMENT', 'local'),
            hostname=platform.node(),
            os_info=f'{platform.system()} {platform.release()}',
            cpu_cores=psutil.cpu_count(),
            memory_gb=round(psutil.virtual_memory().total / (1024**3), 2)
        )

    def start(
        self,
        configuration: Optional[Dict] = None,
    ):
        """
        Start the logging session and log system information.

        Initializes the session start time and logs comprehensive system
        information including session metadata, git information, system
        specifications, and optional configuration parameters.

        Parameters
        ----------
        configuration : dict, optional
            Configuration dictionary to log at session start. Can contain
            any key-value pairs representing application settings, model
            parameters, or other relevant configuration.

        Examples
        --------
        >>> session = Session(session_name="data_pipeline")
        >>> config = {
        ...     "batch_size": 1000,
        ...     "input_file": "/data/input.csv",
        ...     "model_type": "RandomForest"
        ... }
        >>> session.start(configuration=config)

        Notes
        -----
        This method should be called before any phase operations or logging.
        It sets the session_start_time which is used for calculating total
        execution duration in the final summary.
        """

        self.session_start_time = time.time()

        self.logger.info('=' * 60)
        self.logger.info(f'{self.session_info.name.upper()} SESSION STARTED')
        self.logger.info('=' * 60)

        info = self.session_info
        self.logger.info(f'Session Name: {info.name}')
        self.logger.info(f'Session ID: {info.session_id}')
        self.logger.info(f'Timestamp: {info.timestamp}')
        self.logger.info(f'Git Branch: {info.git_branch}')
        self.logger.info(f'Git Commit: {info.git_commit}')
        self.logger.info(f'Python Version: {info.python_version}')
        self.logger.info(f'Environment: {info.environment}')
        self.logger.info(f'Host: {info.hostname}')
        self.logger.info(f'OS: {info.os_info}')
        self.logger.info(f'CPU Cores: {info.cpu_cores}')
        self.logger.info(f'Memory: {info.memory_gb} GB')

        if configuration:
            self.logger.info('=' * 30 + ' CONFIGURATION ' + '=' * 30)
            self._log_configuration_dict(configuration)

        self.logger.info('=' * 60)

    def _log_configuration_dict(
        self,
        config_dict: Dict,
        prefix: str = '',
    ):
        """
        Recursively log a configuration dictionary with proper indentation.

        Helper method to log nested dictionary structures with proper
        indentation for readability. Used internally by the start() method.

        Parameters
        ----------
        config_dict : dict
            Dictionary to log recursively.
        prefix : str, default ''
            Current indentation prefix for nested levels.
        """

        for key, value in config_dict.items():
            if isinstance(value, dict):
                self.logger.info(f'{prefix}{key}:')
                self._log_configuration_dict(value, prefix + '  ')
            else:
                self.logger.info(f'{prefix}{key}: {value}')

    @contextmanager
    def phase(
        self,
        phase_name: str,
        expected_records: Optional[int] = None,
    ):
        """
        Context manager for tracking a processing phase with metrics.

        Creates a PhaseMetrics object to track timing, resource usage,
        and custom metrics for a distinct processing phase. Automatically
        calculates duration, memory usage, CPU utilization, and throughput.

        Parameters
        ----------
        phase_name : str
            Name of the processing phase (e.g., 'data_loading',
            'preprocessing', 'model_training').
        expected_records : int, optional
            Expected number of records to process. Currently unused but
            reserved for future progress tracking features.

        Yields
        ------
        PhaseMetrics
            The phase metrics object that can be updated with custom
            metrics, record counts, and data statistics during execution.

        Examples
        --------
        Basic phase tracking:

        >>> session = Session("my_app")
        >>> session.start()
        >>> with session.phase("data_loading"):
        ...     # Your data loading code here
        ...     session.monitor_system_performance()

        Advanced phase tracking with custom metrics:

        >>> with session.phase("model_training") as phase:
        ...     # Training code here
        ...     phase.records_processed = 10000
        ...     phase.custom_metrics = {
        ...         "accuracy": 0.95,
        ...         "loss": 0.05,
        ...         "f1_score": 0.93
        ...     }
        ...     phase.input_data_stats = {"shape": (10000, 50)}
        ...     phase.output_data_stats = {"shape": (10000, 1)}

        Notes
        -----
        - The context manager automatically handles phase start/end timing
        - Memory and CPU samples are cleared at phase start
        - Performance statistics are calculated automatically at phase end
        - Throughput is calculated if records_processed is set
        - All phases are stored in the session.phases list for summary
        """

        phase_metrics = PhaseMetrics(
            phase_name=phase_name,
            start_time=time.time()
        )

        if self.retain_samples:
            phase_metrics.performance_timeline = []

        self.current_phase = phase_metrics
        start_msg = '=' * 20 + f' {phase_name} STARTED ' + '=' * 20
        self.logger.info(start_msg)

        self.memory_usage_samples.clear()
        self.cpu_usage_samples.clear()

        try:
            yield phase_metrics
        except Exception as e:
            self.log_error_with_context(e, phase=phase_name,)
            raise
        finally:
            phase_metrics.end_time = time.time()
            phase_metrics.duration = (
                phase_metrics.end_time - phase_metrics.start_time
            )

            if phase_metrics.records_processed and phase_metrics.duration:
                phase_metrics.throughput_per_second = (
                    phase_metrics.records_processed / phase_metrics.duration
                )

            if self.memory_usage_samples:
                phase_metrics.memory_peak_gb = max(self.memory_usage_samples)
                total_memory = sum(self.memory_usage_samples)
                phase_metrics.memory_avg_gb = (
                    total_memory / len(self.memory_usage_samples)
                )

            if self.cpu_usage_samples:
                total_cpu = sum(self.cpu_usage_samples)
                phase_metrics.cpu_avg_percent = (
                    total_cpu / len(self.cpu_usage_samples)
                )

            self.phases.append(phase_metrics)
            self.current_phase = None

            duration_msg = (
                f'{phase_name} completed in '
                f'{phase_metrics.duration:.2f}s'
            )
            self.logger.info(duration_msg)

            if phase_metrics.records_processed:
                throughput_msg = (
                    f'Records processed: {phase_metrics.records_processed:,}'
                )
                if phase_metrics.throughput_per_second:
                    rate = phase_metrics.throughput_per_second
                    throughput_msg += f' ({rate:.1f} records/sec)'
                self.logger.info(throughput_msg)

            if phase_metrics.memory_peak_gb:
                memory_msg = (
                    f'Memory usage - Peak: '
                    f'{phase_metrics.memory_peak_gb:.2f}GB, '
                    f'Avg: {phase_metrics.memory_avg_gb:.2f}GB'
                )
                self.logger.info(memory_msg)

            if phase_metrics.cpu_avg_percent:
                cpu_msg = (
                    f'CPU usage - Avg: {phase_metrics.cpu_avg_percent:.1f}%'
                )
                self.logger.info(cpu_msg)

            if phase_metrics.custom_metrics:
                self.logger.info('Custom metrics:')
                for key, value in phase_metrics.custom_metrics.items():
                    if isinstance(value, float):
                        self.logger.info(f'  {key}: {value:.4f}')
                    else:
                        self.logger.info(f'  {key}: {value}')

            end_msg = '=' * 20 + f' {phase_name} COMPLETED ' + '=' * 19
            self.logger.info(end_msg)

    def log_data_summary(
        self,
        data_info: Dict[str, Any,],
        title: Optional[str] = None,
    ):
        """
        Log structured data information with formatted output.

        Logs a dictionary of data information with intelligent formatting
        for different data types. Numbers are formatted with commas,
        nested dictionaries are indented properly, and large collections
        are summarized.

        Parameters
        ----------
        data_info : dict
            Dictionary containing data information to log. Keys should be
            descriptive names, values can be various types including
            numbers, strings, lists, or nested dictionaries.
        title : str, optional
            Custom title for the data summary. If None, defaults to
            'Data Information:'.

        Examples
        --------
        >>> session = Session("my_app")
        >>> session.start()
        >>> data_info = {
        ...     "dataset_size": 1000000,
        ...     "columns": ["id", "name", "value"],
        ...     "shape": (1000000, 3),
        ...     "memory_usage_mb": 250.5,
        ...     "metadata": {
        ...         "source": "database",
        ...         "created": "2023-12-10"
        ...     }
        ... }
        >>> session.log_data_summary(data_info, "Dataset Information")

        Notes
        -----
        - Numeric values with size-related keys are formatted with commas
        - Float values are displayed with 4 decimal places
        - Lists/tuples longer than 5 items show preview with count
        - Nested dictionaries are recursively formatted with indentation
        """

        if title:
            self.logger.info(f'{title}:')
        else:
            self.logger.info('Data Information:')

        self._log_data_dict(data_info, prefix='  ')

    def log_pipeline_step(
        self,
        step_name: str,
        input_data: Optional[Dict[str, Any]] = None,
        output_data: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ):
        """
        Log a pipeline step with input/output data and metrics.

        Logs information about a discrete pipeline step including its name,
        input data characteristics, output data characteristics, and
        associated metrics. Useful for tracking data transformations.

        Parameters
        ----------
        step_name : str
            Name of the pipeline step (e.g., 'feature_extraction',
            'data_cleaning', 'model_inference').
        input_data : dict, optional
            Dictionary containing information about input data such as
            shape, size, columns, or other relevant characteristics.
        output_data : dict, optional
            Dictionary containing information about output data.
        metrics : dict, optional
            Dictionary containing step-specific metrics such as
            processing time, accuracy, or other performance indicators.

        Examples
        --------
        >>> session = Session("data_pipeline")
        >>> session.start()
        >>> session.log_pipeline_step(
        ...     "feature_extraction",
        ...     input_data={"shape": (1000, 50), "columns": 50},
        ...     output_data={"shape": (1000, 25), "features": 25},
        ...     metrics={"processing_time": 1.5, "reduction_ratio": 0.5}
        ... )

        Notes
        -----
        - All parameters except step_name are optional
        - Data dictionaries are formatted using the same logic as
          log_data_summary
        - This method is useful for tracking data flow through processing
          pipelines
        """

        self.logger.info(f'Pipeline Step: {step_name}')

        if input_data:
            self.logger.info('  Input Data:')
            self._log_data_dict(input_data, prefix='    ')

        if output_data:
            self.logger.info('  Output Data:')
            self._log_data_dict(output_data, prefix='    ')

        if metrics:
            self.logger.info('  Metrics:')
            self._log_data_dict(metrics, prefix='    ')

    def _log_data_dict(
        self,
        data: Dict[str, Any],
        prefix: str = '  ',
    ):
        """
        Recursively log a dictionary with intelligent formatting.

        Helper method that logs dictionary contents with special formatting
        for different data types. Numbers are formatted with commas,
        floats show 4 decimal places, and large collections are summarized.

        Parameters
        ----------
        data : dict
            Dictionary to log with intelligent formatting.
        prefix : str, default '  '
            Indentation prefix for the current level.

        Notes
        -----
        - Size-related keys (count, length, size, etc.) are formatted with
          commas
        - Float values are displayed with 4 decimal places
        - Large lists/tuples (>5 items) show preview with item count
        - Nested dictionaries are recursively formatted with increased
          indentation
        - Shape tuples are formatted as "width x height" format
        """

        for key, value in data.items():
            if isinstance(value, (int, float)) and key.lower() in [
                'count', 'length', 'size', 'records', 'rows', 'items'
            ]:
                self.logger.info(f'{prefix}{key}: {value:,}')
            elif isinstance(value, float):
                self.logger.info(f'{prefix}{key}: {value:.4f}')
            elif isinstance(value, (list, tuple)):
                if len(value) <= 5:
                    self.logger.info(f'{prefix}{key}: {value}')
                else:
                    preview = f'[{len(value)} items] {list(value[:3])}...'
                    self.logger.info(f'{prefix}{key}: {preview}')
            elif isinstance(value, dict):
                self.logger.info(f'{prefix}{key}:')
                self._log_data_dict(value, prefix + '  ')
            elif isinstance(value, tuple) and len(value) == 2:
                self.logger.info(f'{prefix}{key}: {value[0]} x {value[1]}')
            else:
                self.logger.info(f'{prefix}{key}: {value}')

    def update_phase_metrics(
        self,
        records_processed: Optional[int] = None,
        custom_metrics: Optional[Dict[str, Any]] = None,
        input_stats: Optional[Dict[str, Any]] = None,
        output_stats: Optional[Dict[str, Any]] = None,
    ):
        """
        Update metrics for the currently active phase.

        Updates various metrics for the current phase including record
        counts, custom metrics, and data statistics. Must be called
        within an active phase context.

        Parameters
        ----------
        records_processed : int, optional
            Number of records processed during this phase. Used for
            calculating throughput.
        custom_metrics : dict, optional
            Dictionary of custom metrics specific to the phase (e.g.,
            accuracy, loss, f1_score). Values can be numeric or string.
        input_stats : dict, optional
            Statistics about input data (e.g., shape, size, columns).
        output_stats : dict, optional
            Statistics about output data (e.g., shape, size, columns).

        Warnings
        --------
        UserWarning
            If called outside of an active phase context.

        Examples
        --------
        >>> session = Session("ml_pipeline")
        >>> session.start()
        >>> with session.phase("training"):
        ...     # Training code here
        ...     session.update_phase_metrics(
        ...         records_processed=50000,
        ...         custom_metrics={
        ...             "accuracy": 0.95,
        ...             "loss": 0.05,
        ...             "epochs": 100
        ...         },
        ...         input_stats={"shape": (50000, 784)},
        ...         output_stats={"shape": (50000, 10)}
        ...     )

        Notes
        -----
        - Custom metrics are merged with existing metrics if called multiple
          times within the same phase
        - Throughput (records/second) is calculated automatically at phase
          end if records_processed is set
        - Input/output stats replace previous values if updated multiple times
        """

        if not self.current_phase:
            warning_msg = (
                "No active phase to update. Use within a "
                "'with session.phase()' block"
            )
            self.logger.warning(warning_msg)
            return

        if records_processed is not None:
            self.current_phase.records_processed = records_processed

        if custom_metrics:
            if self.current_phase.custom_metrics is None:
                self.current_phase.custom_metrics = {}
            self.current_phase.custom_metrics.update(custom_metrics)

        if input_stats:
            self.current_phase.input_data_stats = input_stats

        if output_stats:
            self.current_phase.output_data_stats = output_stats

    def log_values(
        self,
        values: Dict[str, Any],
        title: Optional[str] = None,
    ):
        """
        Log a dictionary of values with optional title and context.

        Logs key-value pairs with intelligent formatting and automatic
        title generation based on the current phase context if no title
        is provided.

        Parameters
        ----------
        values : dict
            Dictionary of values to log. Keys should be descriptive names,
            values can be any type.
        title : str, optional
            Custom title for the values section. If None, automatically
            generates title based on current phase or uses 'Values:'.

        Examples
        --------
        >>> session = Session("my_app")
        >>> session.start()
        >>> session.log_values({
        ...     "model_accuracy": 0.95,
        ...     "training_loss": 0.05,
        ...     "epochs_completed": 100
        ... }, title="Training Results")

        >>> # Within a phase (automatic title)
        >>> with session.phase("evaluation"):
        ...     session.log_values({
        ...         "test_accuracy": 0.93,
        ...         "precision": 0.94,
        ...         "recall": 0.92
        ...     })  # Title will be "evaluation - Values:"

        Notes
        -----
        - Uses the same formatting logic as log_data_summary
        - Automatically includes phase name in title if called within a phase
        - Useful for logging intermediate results or final metrics
        """

        if title:
            self.logger.info(f'{title}:')
        elif self.current_phase:
            phase_name = self.current_phase.phase_name
            self.logger.info(f'{phase_name} - Values:')
        else:
            self.logger.info('Values:')

        self._log_data_dict(values, prefix='  ')

    def log_warning_message(
        self,
        message: str,
        **kwargs,
    ):
        """
        Log a warning message with optional context information.

        Logs a warning message and stores it for inclusion in the session
        summary. Additional context can be provided via keyword arguments.

        Parameters
        ----------
        message : str
            The warning message to log.
        **kwargs
            Additional context information to include with the warning
            (e.g., file_path, line_number, variable_name).

        Examples
        --------
        >>> session = Session("my_app")
        >>> session.start()
        >>> session.log_warning_message(
        ...     "Missing values detected in data",
        ...     column="age",
        ...     missing_count=25,
        ...     total_rows=1000
        ... )

        Notes
        -----
        - Warning messages are stored in session.warning_messages
        - All warnings are included in the final session summary
        - Context kwargs are formatted as key=value pairs in the message
        """

        warning_msg = f'{message}'
        if kwargs:
            warning_msg += f' - {kwargs}'

        self.warning_messages.append(warning_msg)
        self.logger.warning(warning_msg)

    def log_error_with_context(
        self,
        error: Exception,
        phase: Optional[str] = None,
        **kwargs,
    ):
        """
        Log an error with contextual information and full traceback.

        Logs an error message with phase information and additional context,
        stores the error for session summary, and includes a full traceback
        for debugging.

        Parameters
        ----------
        error : Exception
            The exception object that was raised.
        phase : str, optional
            Name of the phase where the error occurred. If None, uses
            'unknown phase'.
        **kwargs
            Additional context information to include with the error
            (e.g., file_path, record_id, iteration_number).

        Examples
        --------
        >>> session = Session("my_app")
        >>> session.start()
        >>> try:
        ...     # Some operation that might fail
        ...     result = 1 / 0
        ... except Exception as e:
        ...     session.log_error_with_context(
        ...         e,
        ...         phase="calculation",
        ...         operation="division",
        ...         values={"numerator": 1, "denominator": 0}
        ...     )

        Notes
        -----
        - Error messages are stored in session.error_messages for summary
        - Full Python traceback is logged for debugging
        - Context kwargs are formatted as key=value pairs in the log
        """

        phase_name = phase or 'unknown phase'
        error_msg = f'Error in {phase_name}: {str(error)}'
        if kwargs:
            error_msg += f' - Context: {kwargs}'

        self.error_messages.append(error_msg)
        self.logger.error(error_msg)
        self.logger.error(f'Traceback:\n{traceback.format_exc()}')

    def monitor_system_performance(
        self,
    ):
        """
        Capture current system performance metrics.

        Samples current memory and CPU usage and stores them for analysis.
        If sample retention is enabled and a phase is active, stores detailed
        timeline data. Automatically logs warnings for high resource usage.

        Notes
        -----
        - Memory usage is captured in gigabytes
        - CPU usage is captured as percentage (0-100)
        - Logs warning if memory usage exceeds 80% of total system memory
        - Logs warning if CPU usage exceeds 90%
        - Timeline samples include timestamp, memory_gb, and cpu_percent
        - Should be called periodically during long-running operations

        Examples
        --------
        >>> session = Session("my_app")
        >>> session.start()
        >>> with session.phase("processing"):
        ...     for i in range(1000):
        ...         # Your processing code here
        ...         if i % 100 == 0:  # Monitor every 100 iterations
        ...             session.monitor_system_performance()

        Warnings
        --------
        High memory or CPU usage will trigger warning log messages but
        will not interrupt execution.
        """

        try:
            memory_gb = psutil.virtual_memory().used / (1024**3)
            self.memory_usage_samples.append(memory_gb)

            cpu_percent = psutil.cpu_percent(interval=None)
            self.cpu_usage_samples.append(cpu_percent)

            if (self.retain_samples and self.current_phase and
                    self.current_phase.performance_timeline is not None):
                sample_data = {
                    'timestamp': time.time(),
                    'memory_gb': memory_gb,
                    'cpu_percent': cpu_percent
                }
                self.current_phase.performance_timeline.append(sample_data)

            memory_threshold = self.session_info.memory_gb * 0.8
            if memory_gb > memory_threshold:
                usage_pct = memory_gb / self.session_info.memory_gb * 100
                warning_msg = (
                    f'High memory usage: {memory_gb:.2f}GB '
                    f'({usage_pct:.1f}%)'
                )
                self.log_warning_message(warning_msg)

            if cpu_percent > 90:
                cpu_warning = f'High CPU usage: {cpu_percent:.1f}%'
                self.log_warning_message(cpu_warning)

        except Exception as e:
            self.logger.debug(f'Performance monitoring error: {e}')

    def log_file_operation(
        self,
        operation_type: str,
        file_path: str,
        file_size_mb: Optional[float] = None,
    ):
        """
        Log a file operation with path and optional size information.

        Logs file operations such as reading, writing, loading, or saving
        with the file path and optional size information for tracking
        data movement and I/O operations.

        Parameters
        ----------
        operation_type : str
            Type of file operation (e.g., 'read', 'write', 'load', 'save',
            'delete', 'created').
        file_path : str
            Path to the file being operated on.
        file_size_mb : float, optional
            Size of the file in megabytes. If provided, will be included
            in the log message.

        Examples
        --------
        >>> session = Session("data_pipeline")
        >>> session.start()
        >>> session.log_file_operation("read", "/data/input.csv", 150.5)
        >>> session.log_file_operation("write", "/output/results.json", 25.2)
        >>> session.log_file_operation("load", "/models/trained_model.pkl")

        Notes
        -----
        - Useful for tracking data I/O in processing pipelines
        - File size is formatted to 1 decimal place if provided
        - Can be used to track both input and output files
        """

        msg = f'File {operation_type}: {file_path}'
        if file_size_mb:
            msg += f' ({file_size_mb:.1f}MB)'
        self.logger.info(msg)

    def finalize(
        self,
    ):
        """
        Finalize the session and generate comprehensive summary.

        Calculates final performance statistics, logs a detailed summary
        including all phases, performance metrics, warnings, and errors.
        Saves a JSON summary file with complete session data.

        Notes
        -----
        - Aggregates all performance samples from phases and session level
        - Calculates total execution time since session.start()
        - Logs performance summary with memory and CPU statistics
        - Shows phase breakdown with duration and percentage of total time
        - Lists all warnings and errors encountered
        - Saves detailed JSON summary to output directory

        Examples
        --------
        >>> session = Session("data_pipeline")
        >>> session.start()
        >>> with session.phase("loading"):
        ...     # Loading code
        ...     pass
        >>> with session.phase("processing"):
        ...     # Processing code
        ...     pass
        >>> session.finalize()  # Logs summary and saves JSON file

        Warnings
        --------
        UserWarning
            If session was not properly started with session.start().

        FileNotFoundError
            If unable to save session summary to output directory.
        """

        if not self.session_start_time:
            self.logger.warning('Session was not properly started')
            return

        total_duration = time.time() - self.session_start_time

        all_memory_samples = []
        all_cpu_samples = []

        for phase in self.phases:
            if phase.performance_timeline:
                for sample in phase.performance_timeline:
                    all_memory_samples.append(sample['memory_gb'])
                    all_cpu_samples.append(sample['cpu_percent'])

        all_memory_samples.extend(self.memory_usage_samples)
        all_cpu_samples.extend(self.cpu_usage_samples)

        peak_memory_gb = max(all_memory_samples) if all_memory_samples else 0
        if all_memory_samples:
            avg_memory_gb = sum(all_memory_samples) / len(all_memory_samples)
        else:
            avg_memory_gb = 0
        if all_cpu_samples:
            avg_cpu_percent = sum(all_cpu_samples) / len(all_cpu_samples)
        else:
            avg_cpu_percent = 0

        perf_header = '=' * 25 + ' PERFORMANCE SUMMARY ' + '=' * 25
        self.logger.info(perf_header)
        formatted_duration = self._format_time_duration(total_duration)
        duration_msg = f'Total execution time: {formatted_duration}'
        memory_msg = (
            f'Memory usage: Peak={peak_memory_gb:.2f}GB, '
            f'Average={avg_memory_gb:.2f}GB'
        )
        cpu_msg = (
            f'CPU utilization: Average={avg_cpu_percent:.1f}% '
            f'({self.session_info.cpu_cores} cores)'
        )
        self.logger.info(duration_msg)
        self.logger.info(memory_msg)
        self.logger.info(cpu_msg)

        if self.phases:
            self.logger.info('Phase breakdown:')
            for phase in self.phases:
                percentage = (phase.duration / total_duration) * 100
                phase_duration = self._format_time_duration(phase.duration)
                phase_msg = (
                    f'  {phase.phase_name}: {phase_duration} '
                    f'({percentage:.1f}%)'
                )
                self.logger.info(phase_msg)

        if self.warning_messages or self.error_messages:
            issue_header = (
                '=' * 25 + ' WARNINGS & ISSUES ' + '=' * 25
            )
            self.logger.info(issue_header)
            for warning in self.warning_messages:
                self.logger.info(f'[WARNING] {warning}')
            for error in self.error_messages:
                self.logger.info(f'[ERROR] {error}')

            if not self.error_messages:
                self.logger.info('No critical errors encountered')

        self.logger.info('=' * 68)
        self.logger.info(f'{self.session_info.name.upper()} SESSION COMPLETED')
        self.logger.info('=' * 68)

        self._save_session_summary(
            total_duration, peak_memory_gb, avg_memory_gb, avg_cpu_percent,
        )

    def _format_time_duration(
        self,
        seconds: float,
    ) -> str:
        """
        Format a duration in seconds to human-readable string.

        Converts a duration in seconds to a human-readable format with
        appropriate units (seconds, minutes and seconds, or hours, minutes
        and seconds).

        Parameters
        ----------
        seconds : float
            Duration in seconds to format.

        Returns
        -------
        str
            Formatted duration string with appropriate units.

        Examples
        --------
        >>> session = Session("my_app")
        >>> session._format_time_duration(45.7)
        '45.7s'
        >>> session._format_time_duration(125.3)
        '2m 5.3s'
        >>> session._format_time_duration(3665.0)
        '1h 1m 5.0s'
        """

        if seconds < 60:
            return f'{seconds:.1f}s'
        elif seconds < 3600:
            minutes = int(seconds // 60)
            remaining_seconds = seconds % 60
            return f'{minutes}m {remaining_seconds:.1f}s'
        else:
            hours = int(seconds // 3600)
            remaining_minutes = int((seconds % 3600) // 60)
            remaining_seconds = seconds % 60
            return f'{hours}h {remaining_minutes}m {remaining_seconds:.1f}s'

    def _save_session_summary(
        self,
        total_duration: float,
        peak_memory_gb: float,
        avg_memory_gb: float,
        avg_cpu_percent: float,
    ):
        """
        Save comprehensive session summary to JSON file.

        Creates and saves a detailed JSON summary containing session
        information, performance metrics, phase details, warnings,
        and errors to the output directory.

        Parameters
        ----------
        total_duration : float
            Total session duration in seconds.
        peak_memory_gb : float
            Peak memory usage across all phases in gigabytes.
        avg_memory_gb : float
            Average memory usage across all samples in gigabytes.
        avg_cpu_percent : float
            Average CPU usage across all samples as percentage.

        Notes
        -----
        - Summary file is named '{session_name}_{session_id}_summary.json'
        - Contains complete session metadata, phase metrics, and timelines
        - Uses default=str in json.dump to handle datetime objects
        - Logs error message if file cannot be saved
        """

        try:
            summary = {
                'session_info': asdict(self.session_info),
                'performance': {
                    'total_duration_seconds': total_duration,
                    'peak_memory_gb': peak_memory_gb,
                    'avg_memory_gb': avg_memory_gb,
                    'avg_cpu_percent': avg_cpu_percent
                },
                'phases': [
                    {
                        'name': phase.phase_name,
                        'duration_seconds': phase.duration,
                        'memory_peak_gb': phase.memory_peak_gb,
                        'memory_avg_gb': phase.memory_avg_gb,
                        'cpu_avg_percent': phase.cpu_avg_percent,
                        'records_processed': phase.records_processed,
                        'throughput_per_second': phase.throughput_per_second,
                        'input_data_stats': phase.input_data_stats,
                        'output_data_stats': phase.output_data_stats,
                        'custom_metrics': phase.custom_metrics,
                        'performance_timeline': phase.performance_timeline
                    }
                    for phase in self.phases
                ],
                'warnings': self.warning_messages,
                'errors': self.error_messages
            }

            summary_filename = f'{self.name}_{self.session_id}_summary.json'
            summary_file = self.output_dir / summary_filename
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)

            self.logger.info(f'Session summary saved: {summary_file}')

        except Exception as e:
            self.logger.error(f'Failed to save session summary: {e}')

    def get_performance_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance statistics from all phases.

        Compiles and returns detailed performance statistics from all
        completed phases including timing, resource usage, throughput,
        and timeline data if available.

        Returns
        -------
        dict
            Dictionary containing performance statistics with the following
            structure:

            - 'phase_count' : int
                Number of completed phases
            - 'total_records_processed' : int
                Sum of records processed across all phases
            - 'phases' : list of dict
                List of per-phase statistics, each containing:
                - 'name' : str
                    Phase name
                - 'duration_seconds' : float
                    Phase duration
                - 'memory_peak_gb' : float
                    Peak memory usage
                - 'memory_avg_gb' : float
                    Average memory usage
                - 'cpu_avg_percent' : float
                    Average CPU usage
                - 'records_processed' : int
                    Records processed in phase
                - 'throughput_per_second' : float
                    Processing throughput
                - 'sample_count' : int (if timeline available)
                    Number of performance samples
                - 'memory_timeline_peak' : float (if timeline available)
                    Peak memory from timeline samples
                - 'memory_timeline_avg' : float (if timeline available)
                    Average memory from timeline samples
                - 'cpu_timeline_peak' : float (if timeline available)
                    Peak CPU from timeline samples
                - 'cpu_timeline_avg' : float (if timeline available)
                    Average CPU from timeline samples

        Examples
        --------
        >>> session = Session("my_app", retain_samples=True)
        >>> session.start()
        >>> with session.phase("processing") as phase:
        ...     phase.records_processed = 1000
        >>> stats = session.get_performance_statistics()
        >>> print(f"Processed {stats['total_records_processed']} records")
        >>> print(f"Across {stats['phase_count']} phases")

        Notes
        -----
        - Returns empty dict if no phases have been completed
        - Timeline statistics only available if retain_samples=True
        - All timing values are in seconds
        - Memory values are in gigabytes
        - CPU values are percentages (0-100)
        """

        if not self.phases:
            return {}

        stats = {
            'phase_count': len(self.phases),
            'total_records_processed': 0,
            'phases': []
        }

        for phase in self.phases:
            phase_stats = {
                'name': phase.phase_name,
                'duration_seconds': phase.duration,
                'memory_peak_gb': phase.memory_peak_gb,
                'memory_avg_gb': phase.memory_avg_gb,
                'cpu_avg_percent': phase.cpu_avg_percent,
                'records_processed': phase.records_processed,
                'throughput_per_second': phase.throughput_per_second
            }

            if phase.performance_timeline:
                timeline_data = phase.performance_timeline
                phase_stats['sample_count'] = len(timeline_data)

                memory_values = [s['memory_gb'] for s in timeline_data]
                cpu_values = [s['cpu_percent'] for s in timeline_data]

                if memory_values:
                    phase_stats['memory_timeline_peak'] = max(memory_values)
                    avg_memory = sum(memory_values) / len(memory_values)
                    phase_stats['memory_timeline_avg'] = avg_memory

                if cpu_values:
                    phase_stats['cpu_timeline_peak'] = max(cpu_values)
                    avg_cpu = sum(cpu_values) / len(cpu_values)
                    phase_stats['cpu_timeline_avg'] = avg_cpu

            stats['phases'].append(phase_stats)

            if phase.records_processed:
                stats['total_records_processed'] += phase.records_processed

        return stats

    def get_log_file_path(
        self,
    ) -> Optional[Path]:
        """
        Get the path to the main log file for this session.

        Returns the full path to the session's log file. The filename
        follows the pattern '{session_name}_{session_id}.log'.

        Returns
        -------
        Path or None
            Path object pointing to the log file. Returns None if file
            output is disabled or path cannot be determined.

        Examples
        --------
        >>> session = Session("my_app")
        >>> log_path = session.get_log_file_path()
        >>> print(log_path)  # doctest: +SKIP
        logs/my_app_session_20231210_143022.log
        >>>
        >>> # Check if log file exists
        >>> if log_path and log_path.exists():
        ...     print(f"Log file size: {log_path.stat().st_size} bytes")

        Notes
        -----
        - The returned path may not exist until logging begins
        - Log files use rotating file handlers, so additional .1, .2, etc.
          files may exist for backups
        - Path is relative to the session's output_directory
        """

        return self.output_dir / f'{self.name}_{self.session_id}.log'
