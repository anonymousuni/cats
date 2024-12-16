# CATS scheduler

This repository contains the code and instructions associated with replicating the results from the paper on scheduling data pipelines on Comuting Continuum resources using the Context-Aware Timeline Scheduler (CATS). It is organized into three main folders: `scheduler`, `experiment`, and `pipeline`.

## Repository Structure

### Scheduler

The `scheduler` folder contains the CATS scheduler and code to perform scheduling on a target infrastructure as described in the paper. This includes:

- The main scheduling script (`main.py`)
- Required libraries and dependencies (`requirements.txt`)
- Implementation of the CATS scheduler, organized as an object-oriented Python project

For detailed instructions on how to use the scheduler, refer to the `README.md` file within the `scheduler` folder.

### Experiment

The `experiment` folder contains instructions on infrastructure setup to replicate the environment described in the paper. It also includes instructions on how to perform dry runs of the pipeline and perform deployments according to the schedules produced by the CATS scheduler and serialized as CSV files.

For detailed instructions on setting up the infrastructure and performing experiments, refer to the `README.md` file within the `experiment` folder.

### Pipeline

The `pipeline` folder contains the source code for the pipeline under study. It also includes files that can be used to build the container images used to implement the pipeline. Each sub-folder within the `pipeline` folder contains a `Dockerfile` and related files for building the container images for each step of the pipeline.

For detailed instructions on building and running the pipeline steps, refer to the `README.md` file within the `pipeline` folder.

## Getting Started

To get started with the repository, follow these steps:

1. **Clone the Repository**:

    ```sh
    git clone <repository-url>
    ```

2. **Navigate to the Desired Folder**:

    Depending on your goal, navigate to the appropriate folder (`scheduler`, `experiment`, or `pipeline`) and follow the instructions provided in the respective `README.md` file.

## License

This project code will be licensed under the MIT License.

## Contact

For any questions or issues, please contact the this repository's owner.
