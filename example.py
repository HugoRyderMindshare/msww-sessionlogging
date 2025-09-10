#!/usr/bin/env python3
"""
Example usage of the msww-sessionlogging package.

This script demonstrates the basic functionality of the Session class
including logging, phase tracking, and performance monitoring.
"""

import random
import time

from sessionlogging import Session


def example_data_processing(
):
    """
    Simulate some data processing work.
    """

    time.sleep(random.uniform(0.1, 0.5))

    return random.randint(1, 100)


def main(
):
    """
    """

    # Create a session
    session = Session(
        session_name='example_workflow',
        log_path='./logs'
    )

    session.log('Starting example workflow')

    # Example 1: Basic logging
    session.log('This is an info message')
    session.warn('This is a warning message')

    # Example 2: Phase tracking
    with session.phase('data_loading') as phase:
        session.log('Loading data...')
        time.sleep(0.2)

        # Add custom metrics
        phase.add_metric('records_loaded', 1000)
        phase.add_metric('file_size_mb', 25.5)

        session.log('Data loading complete')

    # Example 3: Processing phase with error handling
    with session.phase('data_processing') as phase:
        session.log('Processing data...')

        try:
            for i in range(5):
                result = example_data_processing()
                session.log(f"Processed item {i+1}, result: {result}")
                phase.add_metric(f"item_{i+1}_result", result)

            phase.add_metric('items_processed', 5)
            phase.add_metric('success_rate', 1.0)

        except Exception as e:
            session.error(f'Processing failed: {e}')
            phase.add_metric('success_rate', 0.0)

    # Example 4: Analysis phase
    with session.phase('analysis') as phase:
        session.log('Analyzing results...')
        time.sleep(0.1)

        # Simulate analysis results
        phase.add_metric('average_score', 85.7)
        phase.add_metric('confidence', 0.92)

        session.log('Analysis complete')

    # Get and display session summary
    summary = session.get_summary()
    session.log('Session completed successfully')

    print('\n' + '='*50)
    print('SESSION SUMMARY')
    print('='*50)
    print(f'Session: {summary["session_info"]["session_name"]}')
    duration = summary["session_info"]["duration_seconds"]
    print(f'Duration: {duration:.2f} seconds')
    print(f'Total phases: {len(summary["phases"])}')

    for phase_name, phase_data in summary['phases'].items():
        print(f'\nPhase \'{phase_name}\':')
        print(f'  Duration: {phase_data["duration_seconds"]:.3f}s')
        print(f'  Peak memory: {phase_data["peak_memory_mb"]:.1f} MB')
        if phase_data['metrics']:
            print('  Metrics:')
            for metric, value in phase_data['metrics'].items():
                print(f'    {metric}: {value}')

    # Export detailed summary to JSON
    session.export_summary('example_session_summary.json')
    print('\nDetailed summary exported to: example_session_summary.json')
    print(f'Logs saved to: {session.log_file}')


if __name__ == '__main__':
    main()
