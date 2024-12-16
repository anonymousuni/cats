#!/bin/bash

# Define the required variables
export STEP_NAME="02-03-prepare"
export RESOURCE_NAME=${NODE_NAME:-"default_node_name"}  # Use NODE_NAME environment variable or a default value if not set
export DRY_RUN_ID=${DRY_RUN_ID:-0}  # Read from environment variable or default to 0
LOG_DIR="/log"
LOG_FILE="$LOG_DIR/step_metrics.csv"

# RabbitMQ connection details
RABBITMQ_HOST=${RABBITMQ_HOST:-"10.0.2.7"}
RABBITMQ_PORT=${RABBITMQ_PORT:-15672}
RABBITMQ_USER=${RABBIT_USER:-"guest"}
RABBITMQ_PASS=${RABBITMQ_PASS:-"guest"}
INPUT_QUEUE_NAME="MQ2_2_$DRY_RUN_ID"
OUTPUT_QUEUE_NAME="MQ2_3_$DRY_RUN_ID"

echo "Starting the retrieval process..."

# Define the maximum number of retries
MAX_RETRIES=${MAX_RETRIES:-5}
RETRY_COUNT=0

# Define the destination directory
LOCAL_WORK_DIR="/work"

# Initialize the counter for NUM_OUTPUTS
NUM_OUTPUTS=0

# Initialize the variable for COPY_TIME
START_TIME=$(date +%s%3N)
# Retrieve the message from RabbitMQ with retry logic
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  echo "Retrieving message from RabbitMQ queue $INPUT_QUEUE_NAME (Attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)..."

  MESSAGE=$(rabbitmqadmin get queue=$INPUT_QUEUE_NAME ackmode=ack_requeue_false --format=raw_json \
    --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" --host="$RABBITMQ_HOST" --port="$RABBITMQ_PORT"  | jq -r '.[0].payload')

  # Check if a message was retrieved
  if [ "$MESSAGE" != "null" ] && [ -n "$MESSAGE" ]; then
  # if [ -n "$MESSAGE" ]; then
    echo "Message retrieved successfully."
    echo "Message content: { $MESSAGE }"
    RETRY_COUNT=0  # Reset the retry count
    MAX_RETRIES=5  # Reset the max retries - since we are getting messages, we should not wait more than 5 times

    # Parse the received message to extract the values
    DATA=$(echo $MESSAGE | jq -r '.DATA')
    SOURCE=$(echo $MESSAGE | jq -r '.SOURCE')

    # Check if SOURCE is the same as RESOURCE_NAME and set SOURCE to "out" if true
    if [ "$SOURCE" == "$RESOURCE_NAME" ]; then
      SOURCE="out"
    fi
    # Set the SOURCE_DIR based on the message
    SOURCE_DIR="/$SOURCE/$DATA"
    echo "SOURCE_DIR set to $SOURCE_DIR"

    # Ensure SOURCE_DIR is set
    if [ -z "$SOURCE_DIR" ] || [ "$SOURCE_DIR" == "//" ] || [ "$SOURCE" == "null" ] || [ "$DATA" == "null" ]; then
      echo "Error getting message contents from queue. Exiting."
      exit 1
    fi

    # Check if SOURCE is the same as RESOURCE_NAME and set SOURCE to "out" if true
    if [ "$SOURCE" == "$RESOURCE_NAME" ]; then
      SOURCE="out"
    fi

    # If SOURCE is "out", set LOCAL_WORK_DIR to SOURCE_DIR and skip copying
    if [ "$SOURCE" == "out" ]; then
      LOCAL_WORK_DIR="$SOURCE_DIR"
      echo "SOURCE is 'out', setting LOCAL_WORK_DIR to SOURCE_DIR: $LOCAL_WORK_DIR"
      COPY_TIME=0
      WORK_PATH="$LOCAL_WORK_DIR"
    else
      # Create the destination directory if it does not exist
      if [ ! -d "$LOCAL_WORK_DIR" ]; then
        echo "Creating destination directory $LOCAL_WORK_DIR..."
        mkdir -p "$LOCAL_WORK_DIR"
      fi

      # Measure the time to copy the data in milliseconds
      START_COPY_TIME=$(date +%s%3N)
      echo "Copying data from $SOURCE_DIR to $LOCAL_WORK_DIR..."
      cp -r "$SOURCE_DIR" "$LOCAL_WORK_DIR"/
      END_COPY_TIME=$(date +%s%3N)
      COPY_TIME=$((END_COPY_TIME - START_COPY_TIME))
      WORK_PATH="/work/$DATA"
      echo "Data copied successfully to $LOCAL_WORK_DIR."
      echo "Time taken to copy data: $COPY_TIME milliseconds"
    fi

    # Create the RabbitMQ queue if it does not exist
    echo "Creating RabbitMQ queue $OUTPUT_QUEUE_NAME if it does not exist..."
    rabbitmqadmin declare queue name=$OUTPUT_QUEUE_NAME durable=true \
      --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" --host="$RABBITMQ_HOST" --port="$RABBITMQ_PORT"

    # CODE TO EXECUTE
    python3 prepare.py $WORK_PATH /work /out $OUTPUT_QUEUE_NAME

    # Increment the counter for NUM_OUTPUTS
    NUM_OUTPUTS=$((NUM_OUTPUTS + 1))
  else
    echo "No message retrieved from RabbitMQ queue $INPUT_QUEUE_NAME."
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 1  # Wait for 1 second before retrying
  fi
done

# Measure the total time taken in milliseconds
END_TIME=$(date +%s%3N)
TOTAL_TIME=$((END_TIME - START_TIME))
# Calculate the size of the files that were copied
echo "Calculating the size of the extracted files..."

# Calculate the total data volume
TOTAL_INPUT_DATA_VOLUME=$(du -cb "$LOCAL_WORK_DIR" "$LOCAL_WORK_DIR" | grep total | awk '{print $1}')
TOTAL_OUTPUT_DATA_VOLUME=$(du -cb "/out" "/out" | grep total | awk '{print $1}')

# Calculate the average data volume
if [ "$NUM_OUTPUTS" -gt 0 ]; then
  export INPUT_DATA_VOLUME=$((TOTAL_INPUT_DATA_VOLUME ))
  export OUTPUT_DATA_VOLUME=$((TOTAL_OUTPUT_DATA_VOLUME / NUM_OUTPUTS))
else
  export INPUT_DATA_VOLUME=0
  export OUTPUT_DATA_VOLUME=0
fi
NUM_INPUTS=$NUM_OUTPUTS

# Print the variables
echo "STEP_NAME: $STEP_NAME"
echo "RESOURCE_NAME: $RESOURCE_NAME"
echo "NUM_INPUTS: $NUM_INPUTS"
echo "INPUT_DATA_VOLUME: $INPUT_DATA_VOLUME bytes"
echo "NUM_OUTPUTS: $NUM_OUTPUTS"
echo "OUTPUT_DATA_VOLUME: $OUTPUT_DATA_VOLUME bytes"
echo "DRY_RUN_ID: $DRY_RUN_ID"
echo "TOTAL_TIME: $TOTAL_TIME milliseconds"
echo "DATA_TRANSMISSION_TIME: $COPY_TIME milliseconds"

CURRENT_TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Add an entry to the step_metrics.csv file
echo "Logging metrics to $LOG_FILE..."
if [ ! -f "$LOG_FILE" ]; then
  echo "DRY_RUN_ID,CURRENT_TIMESTAMP,STEP_NAME,RESOURCE_NAME,NUM_INPUTS,INPUT_DATA_VOLUME,NUM_OUTPUTS,OUTPUT_DATA_VOLUME,STEP_PROCESSING_TIME,DATA_TRANSMISSION_TIME" > "$LOG_FILE"
fi
echo "$DRY_RUN_ID,$CURRENT_TIMESTAMP,$STEP_NAME,$RESOURCE_NAME,$NUM_INPUTS,$INPUT_DATA_VOLUME,$NUM_OUTPUTS,$OUTPUT_DATA_VOLUME,$TOTAL_TIME,$COPY_TIME" >> "$LOG_FILE"
echo "Metrics logged successfully."

# Check if the maximum number of retries was reached
if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "Failed to retrieve message after $MAX_RETRIES attempts."
  exit 1
fi