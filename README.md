
<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">Advanced Performance Logging</h3>

  <p align="center">
    A comprehensive Python logging package with performance monitoring, phase tracking, and system resource analysis.
    <br />
    <strong>Performance Monitoring • System Tracking • Phase Management</strong>
    <br />
    <br />
    <a href="#usage">View Usage Examples</a>
    &middot;
    <a href="https://github.com/HugoRyderMindshare/msww-sessionlogging/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    &middot;
    <a href="https://github.com/HugoRyderMindshare/msww-sessionlogging/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#key-features">Key Features</a></li>
        <li><a href="#use-cases">Use Cases</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#api-reference">API Reference</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Advanced Performance Logging is a comprehensive Python package that extends standard logging capabilities with detailed performance monitoring, system resource tracking, and structured phase management. It's designed for data processing pipelines, machine learning workflows, and other compute-intensive applications where detailed performance analysis is crucial.

### Key Features

- **Automatic System Information Capture**: CPU, memory, git info, hostname, and environment details
- **Phase-Based Execution Tracking**: Context managers for structured workflow monitoring
- **Real-Time Performance Monitoring**: Memory and CPU usage tracking with customizable sampling
- **Detailed Session Summaries**: Comprehensive reports with JSON export capabilities
- **Custom Metrics Support**: Domain-specific measurements and KPI tracking
- **Structured Data Logging**: Intelligent formatting for complex data structures
- **Warning and Error Aggregation**: Centralized collection for easy debugging and analysis

### Use Cases

- **Data Pipeline Monitoring**: Track performance across ETL operations and data transformations
- **Machine Learning Workflows**: Monitor training phases, model performance, and resource utilization
- **System Performance Analysis**: Detailed profiling of compute-intensive applications
- **Debugging and Optimization**: Identify bottlenecks and performance regression points
- **Compliance and Auditing**: Comprehensive logging for regulatory requirements
- **Development and Testing**: Performance benchmarking and system resource validation

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

- [![Python 3.8+][Python]][Python-url]
- [![PSUtil][PSUtil]][PSUtil-url]
- [![GitPython][GitPython]][GitPython-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

- Python 3.8 or higher
- Required packages: `psutil` for system monitoring
- Optional: `gitpython` for git repository information

### Installation

#### From PyPI (when available)

```bash
pip install msww-sessionlogging
```

#### From Source

1. Clone the repository
   ```bash
   git clone https://github.com/HugoRyderMindshare/msww-sessionlogging.git
   cd logging
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Install the package
   ```bash
   pip install -e .
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

### Basic Session Usage

```python
from sessionlogging import Session

# Create and start a basic session
session = Session(session_name="data_pipeline")
session.start()

# Your application code here
session.logger.info("Starting data processing")

# Use phase tracking for structured monitoring
with session.phase("data_loading"):
    # Data loading code
    session.monitor_system_performance()
    session.logger.info("Loaded 10,000 records")

with session.phase("data_processing"):
    # Data processing code
    session.logger.info("Processing complete")

# Finalize and generate summary
session.finalize()
```

### Advanced Session with Custom Configuration

```python
from sessionlogging import Session

# Advanced configuration
session = Session(
    session_name="ml_training",
    log_level="DEBUG",
    output_directory="/var/log/ml_training",
    retain_samples=True,  # Keep detailed performance samples
    max_file_size_bytes=100 * 1024 * 1024  # 100MB log files
)

# Start with configuration context
config = {
    "batch_size": 32,
    "learning_rate": 0.001,
    "epochs": 100
}
session.start(configuration=config)

# Training phase with custom metrics
with session.phase("training") as phase:
    for epoch in range(100):
        # Training code here
        session.logger.debug(f"Epoch {epoch} completed")
        
        # Update phase metrics
        phase.records_processed = 10000 * (epoch + 1)
        phase.custom_metrics = {
            "accuracy": 0.95 + (epoch * 0.001),
            "loss": 0.5 - (epoch * 0.005)
        }
        
        # Monitor system performance
        session.monitor_system_performance()

session.finalize()
```

### Performance Monitoring

```python
# Monitor system performance during execution
session = Session("performance_test", retain_samples=True)
session.start()

with session.phase("computation") as phase:
    # Your compute-intensive code
    for i in range(1000):
        # Some processing
        if i % 100 == 0:
            session.monitor_system_performance()
    
    # Add custom performance metrics
    phase.custom_metrics = {
        "iterations_completed": 1000,
        "average_processing_time": 0.05
    }

# Get detailed performance timeline
session.finalize()
```

### Data Statistics Logging

```python
import pandas as pd
from sessionlogging import Session

session = Session("data_analysis")
session.start()

# Load some data
df = pd.read_csv("data.csv")

with session.phase("data_exploration") as phase:
    # Log input data statistics using log_data_summary
    session.log_data_summary(
        {"rows": len(df), "columns": len(df.columns)},
        title="Input Dataset Stats"
    )
    
    # Process data
    processed_df = df.dropna()
    
    # Log output statistics  
    session.log_data_summary(
        {"rows": len(processed_df), "columns": len(processed_df.columns)},
        title="Cleaned Dataset Stats"
    )
    
    # Update phase metrics
    phase.input_data_stats = {
        "rows": len(df),
        "columns": len(df.columns),
        "missing_values": df.isnull().sum().sum()
    }
    phase.output_data_stats = {
        "rows": len(processed_df),
        "columns": len(processed_df.columns),
        "missing_values": 0
    }
    phase.records_processed = len(df)

session.finalize()
```

### Error and Warning Tracking

```python
session = Session("error_tracking_demo")
session.start()

try:
    with session.phase("risky_operation"):
        # Some operation that might generate warnings/errors
        session.logger.warning("This is a warning message")
        session.logger.error("This is an error message")
        
        # Errors and warnings are automatically collected
        # for summary reporting
        
except Exception as e:
    session.logger.error(f"Unexpected error: {str(e)}")

# Summary will include all warnings and errors
session.finalize()
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- API REFERENCE -->
## API Reference

### Core Classes

#### `Session`

The main class for advanced logging with performance monitoring and phase tracking.

**Constructor Parameters:**
- `session_name` (str, optional): Name for the logging session
- `session_id` (str, optional): Unique identifier (auto-generated if not provided)
- `log_level` (str, default 'INFO'): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
- `output_directory` (str, default 'logs'): Directory for log files and summaries
- `enable_console_output` (bool, default True): Enable console output
- `enable_file_output` (bool, default True): Enable file output
- `max_file_size_bytes` (int, default 50MB): Maximum log file size before rotation
- `backup_file_count` (int, default 5): Number of backup files to retain
- `retain_samples` (bool, default False): Whether to keep detailed performance samples

**Key Methods:**
- `start(configuration=None)` - Initialize and start the logging session
- `phase(phase_name)` - Context manager for tracking execution phases
- `finalize()` - End session and generate comprehensive summary
- `monitor_system_performance()` - Capture current system performance metrics
- `update_phase_metrics(**kwargs)` - Update metrics for current phase
- `get_log_file_path()` - Get the path to the session's log file
- `get_performance_statistics()` - Get comprehensive performance statistics

**Logging Methods:**
- `logger.debug(message)`, `logger.info(message)`, `logger.warning(message)`, `logger.error(message)`, `logger.critical(message)` - Standard logging methods via the logger attribute
- `log_values(values_dict, title=None)` - Log key-value pairs in structured format
- `log_data_summary(data_info, title=None)` - Log comprehensive data summary
- `log_pipeline_step(step_name, input_data=None, output_data=None, metrics=None)` - Log pipeline step details
- `log_warning_message(message, **kwargs)` - Log a warning message
- `log_error_with_context(exception, **kwargs)` - Log an error with context information
- `log_file_operation(operation, file_path, size_mb=None)` - Log file operations

#### `SessionInfo`

Dataclass containing session metadata and system information.

**Attributes:**
- `name` (str): Session name
- `session_id` (str): Unique session identifier
- `timestamp` (str): ISO format creation timestamp
- `git_branch` (str): Current git branch
- `git_commit` (str): Short git commit hash
- `python_version` (str): Python version string
- `environment` (str): Environment name
- `hostname` (str): Machine hostname
- `os_info` (str): Operating system information
- `cpu_cores` (int): Number of CPU cores
- `memory_gb` (float): Total system memory in GB

#### `PhaseMetrics`

Dataclass for tracking performance metrics during execution phases.

**Attributes:**
- `phase_name` (str): Name of the execution phase
- `start_time` (float): Unix timestamp when phase started
- `end_time` (float, optional): Unix timestamp when phase ended
- `duration` (float, optional): Total phase duration in seconds
- `memory_peak_gb` (float, optional): Peak memory usage in GB
- `memory_avg_gb` (float, optional): Average memory usage in GB
- `cpu_avg_percent` (float, optional): Average CPU utilization percentage
- `input_data_stats` (dict, optional): Input data statistics
- `output_data_stats` (dict, optional): Output data statistics
- `records_processed` (int, optional): Number of records processed
- `throughput_per_second` (float, optional): Processing throughput
- `custom_metrics` (dict, optional): User-defined custom metrics
- `performance_timeline` (list, optional): Detailed performance samples over time

### Usage Patterns

#### Context Manager for Phases

```python
with session.phase("data_processing") as phase:
    # Your processing code
    phase.records_processed = 1000
    phase.custom_metrics = {"accuracy": 0.95}
```

#### Performance Monitoring

```python
# Automatic monitoring during phases
with session.phase("training"):
    session.monitor_system_performance()  # Manual capture
```

#### Custom Metrics

```python
session.update_phase_metrics(
    records_processed=5000,
    custom_metrics={
        "model_accuracy": 0.92,
        "training_loss": 0.15
    }
)
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

- [x] Core logging session with performance monitoring
- [x] Phase-based execution tracking with context managers
- [x] System resource monitoring (CPU, memory)
- [x] Automatic system information capture
- [x] Structured data logging capabilities
- [x] Session summaries with JSON export
- [ ] Advanced statistical analysis of performance data
- [ ] Integration with popular monitoring tools (Prometheus, Grafana)
- [ ] Real-time performance alerts and thresholds
- [ ] Database backend for storing session data
- [ ] Web-based dashboard for session analysis
- [ ] Advanced visualization capabilities
- [ ] Distributed logging across multiple processes
- [ ] Integration with cloud monitoring services

See the [open issues](https://github.com/HugoRyderMindshare/msww-sessionlogging/issues) for a full list of proposed features and known issues.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

### Development Process

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Make your changes
4. Run tests to ensure everything works (`pytest`)
5. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
6. Push to the Branch (`git push origin feature/AmazingFeature`)
7. Open a Pull Request

### Testing

This project uses pytest for testing. Make sure to run the full test suite:

```bash
pytest tests/
```

### Code Style

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Include comprehensive docstrings for all public functions and classes
- Add tests for new functionality
- Ensure performance monitoring doesn't significantly impact application performance

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Top contributors:

<a href="https://github.com/HugoRyderMindshare/msww-sessionlogging/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=HugoRyderMindshare/msww-sessionlogging" alt="contrib.rocks image" />
</a>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Hugo Ryder - [@HugoRyderMindshare](https://linkedin.com/in/hugoryder) - hugo.ryder@mindshareworld.com

Project Link: [https://github.com/HugoRyderMindshare/msww-sessionlogging](https://github.com/HugoRyderMindshare/msww-sessionlogging)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [PSUtil](https://psutil.readthedocs.io/) - Cross-platform library for system and process monitoring
* [GitPython](https://gitpython.readthedocs.io/) - Python library for interacting with Git repositories
* [Python Logging](https://docs.python.org/3/library/logging.html) - Python standard library logging module
* [Mindshare Worldwide](https://mindshareworld.com/) - Supporting organization

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/HugoRyderMindshare/msww-sessionlogging.svg?style=for-the-badge
[contributors-url]: https://github.com/HugoRyderMindshare/msww-sessionlogging/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/HugoRyderMindshare/msww-sessionlogging.svg?style=for-the-badge
[forks-url]: https://github.com/HugoRyderMindshare/msww-sessionlogging/network/members
[stars-shield]: https://img.shields.io/github/stars/HugoRyderMindshare/msww-sessionlogging.svg?style=for-the-badge
[stars-url]: https://github.com/HugoRyderMindshare/msww-sessionlogging/stargazers
[issues-shield]: https://img.shields.io/github/issues/HugoRyderMindshare/msww-sessionlogging.svg?style=for-the-badge
[issues-url]: https://github.com/HugoRyderMindshare/msww-sessionlogging/issues
[license-shield]: https://img.shields.io/github/license/HugoRyderMindshare/msww-sessionlogging.svg?style=for-the-badge
[license-url]: https://github.com/HugoRyderMindshare/msww-sessionlogging/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/hugoryder
[Python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://www.python.org/
[PSUtil]: https://img.shields.io/badge/psutil-%23013243.svg?style=for-the-badge&logo=python&logoColor=white
[PSUtil-url]: https://psutil.readthedocs.io/
[GitPython]: https://img.shields.io/badge/GitPython-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white
[GitPython-url]: https://gitpython.readthedocs.io/
