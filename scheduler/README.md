# Context-Aware Timeline Scheduler (CATS) for Data Pipelines

This project is the CATS a pipeline scheduler that uses dry run results to estimate hardware requirements and timelines for pipelines. The scheduler can generate and display timelines based on different input volumes, deadlines, and budgets.

## Prerequisites

- Python 3.6 or higher
- `pip` (Python package installer)

## Installation

1. Clone the repository:

    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. Install the required Python libraries:

    ```sh
    pip install -r requirements.txt
    ```

## Running CATS for the spot welding data preparation pipeline

1. Run the `main.py` script:

    ```sh
    python main.py --display_timelines 1
    ```

    The `--display_timelines` argument is optional. Set it to `1` to display the generated timelines, or `0` to skip displaying them.

## Files

- [main.py](main.py): The main script to run the scheduler.
- [requirements.txt](requirements.txt): The list of required Python libraries.
- [resources.csv](resources.csv): The CSV file containing resource descriptions.
- [step_metrics.csv](step_metrics.csv), [step_performance_metrics.csv](step_performance_metrics.csv), [deployment_metrics.csv](deployment_metrics.csv): CSV files containing dry run metrics for the steps. 

The rest of the repository contains the implementation of the CATS scheduler, organized as a modular, object-oriented Python project.

## Output

The generated timelines will be saved as CSV files in the scheduler directory with filenames in the format:
timeline_metrics3_{current_time}_deadline{deadline}_budget{budget}_input{input_volume_MB}MB_maxscalability{max_scalability}.csv

> **Note:** The values of the variables in the format are set within the code in `main.py`.
