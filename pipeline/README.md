# Data Pipeline Steps

This folder contains the code for building the data pipeline steps as containers. Each sub-folder contains a `Dockerfile` that can be used to build the image of a step.

## Structure

- `<step_name>/`
    - `Dockerfile` - used to build the image
    - `main.sh` - The main script that performs the data processing step and logging step timeline-related metrics.
    - `record.sh` - A script that runs `main.sh` and uses `psrecord` to monitor and log the performance metrics (CPU and memory usage) of the step.
    - `rabbitmqadmin` - A tool for managing RabbitMQ, used in `main.sh` to create queues and send messages.
    - `requirements.txt` - A file listing the required Python libraries for running the scripts.


## Building Docker Images

To build the Docker image for a specific step, navigate to the sub-folder of that step and run the following command:

```sh
docker build -t <image-name> .