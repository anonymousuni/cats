#!/bin/bash

# Mapping of step names to Docker images
declare -A IMAGE_MAP
IMAGE_MAP=(
  ["01-01-retrieve"]="nikolayn/bosch-retrieve-meta-v2"
  ["01-02-prepare-and-store"]="nikolayn/bosch-prepare-and-store-meta-v2"
  ["02-01-retrieve"]="nikolayn/bosch-retrieve-main-v2"
  ["02-02-slice"]="nikolayn/bosch-slice-main-v2"
  ["02-03-prepare"]="nikolayn/bosch-prepare-main-v2"
)

# CSV output file
CSV_FILE="/fog1/log/deployment_metrics.csv"

# Get the node name from the environment variable
NODE_NAME=${NODE_NAME:-"unknown_node"}

# Check if CSV file exists and is empty, if so create it with headers
if [ ! -f "$CSV_FILE" ] || [ ! -s "$CSV_FILE" ]; then
  sudo echo "step_name,node_name,average_download_time_seconds,average_instance_start_time_seconds" > "$CSV_FILE"
fi

# Loop through all steps in the IMAGE_MAP
for STEP_NAME in "${!IMAGE_MAP[@]}"; do
  IMAGE_NAME=${IMAGE_MAP[$STEP_NAME]}

  # Initialize variables for averaging
  total_download_time=0
  total_instance_start_time=0
  runs=10

  for ((i=1; i<=runs; i++)); do
    echo "Run #$i for $STEP_NAME ($IMAGE_NAME) on node $NODE_NAME"

    # Record start time for download
    START_TIME=$(date +%s.%N)

    # Download the Docker image
    sudo docker pull "$IMAGE_NAME"

    # Record end time for download
    END_TIME=$(date +%s.%N)

    # Calculate the download time
    DOWNLOAD_TIME=$(echo "$END_TIME - $START_TIME" | bc)
    total_download_time=$(echo "$total_download_time + $DOWNLOAD_TIME" | bc)

    # Remove the downloaded image
    sudo docker rmi "$IMAGE_NAME" > /dev/null 2>&1

    # Record start time for instance start
    START_INSTANCE_TIME=$(date +%s.%N)

    # Start a container from the downloaded image
    sudo docker pull "$IMAGE_NAME"
    sudo docker run -d --name temp_container "$IMAGE_NAME"

    # Record end time for instance start
    END_INSTANCE_TIME=$(date +%s.%N)

    # Calculate the instance start time
    INSTANCE_START_TIME=$(echo "$END_INSTANCE_TIME - $START_INSTANCE_TIME" | bc)
    total_instance_start_time=$(echo "$total_instance_start_time + $INSTANCE_START_TIME" | bc)

    # Stop and remove the container
    sudo docker stop temp_container > /dev/null 2>&1
    sudo docker rm temp_container > /dev/null 2>&1

    # Remove the image again after running
    sudo docker rmi "$IMAGE_NAME" > /dev/null 2>&1

  done

  # Calculate average times
  avg_download_time=$(echo "$total_download_time / $runs" | bc -l)
  avg_instance_start_time=$(echo "$total_instance_start_time / $runs" | bc -l)

  # Append the result to the CSV file
  sudo echo "$STEP_NAME,$NODE_NAME,$avg_download_time,$avg_instance_start_time" >> "$CSV_FILE"

  # Print result
  echo "Step: $STEP_NAME, Node: $NODE_NAME"
  echo "Average download time: $avg_download_time seconds"
  echo "Average instance start time: $avg_instance_start_time seconds"

done

echo "Results saved to $CSV_FILE"