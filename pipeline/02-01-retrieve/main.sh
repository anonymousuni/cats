#!/bin/bash

# Set the source and destination directories
SOURCE_DIR="/DataSource"
DEST_DIR="/out"
LOG_DIR="/log"
LOG_FILE="$LOG_DIR/step_metrics.csv"

# Define the required variables
export STEP_NAME="02-01-retrieve"
export RESOURCE_NAME=${NODE_NAME:-"default_node_name"}  # Use NODE_NAME environment variable or a default value if not set
export NUM_INPUTS=1
export NUM_OUTPUTS=1
export DRY_RUN_ID=${DRY_RUN_ID:-0}  # Read from environment variable or default to 0

# RabbitMQ connection details
RABBITMQ_HOST=${RABBITMQ_HOST:-"10.0.2.7"}
RABBITMQ_PORT=${RABBITMQ_PORT:-15672}
RABBITMQ_USER=${RABBITMQ_USER:-"guest"}
RABBITMQ_PASS=${RABBITMQ_PASS:-"guest"}
QUEUE_NAME="MQ2_1_$DRY_RUN_ID"

echo "Starting the retrieval process..."

# Find the first tar.bz2 archive in the source directory
echo "Searching for tar.bz2 archive in $SOURCE_DIR..."
ARCHIVE_NAME=$(ls "$SOURCE_DIR"/*.tar.bz2 | head -n 1)

# Check if an archive was found
if [ -z "$ARCHIVE_NAME" ]; then
  echo "No tar.bz2 archive found in $SOURCE_DIR."
  exit 1
fi

echo "Found archive: $ARCHIVE_NAME"
BASE_NAME=$(basename "$ARCHIVE_NAME" .tar.bz2)

# # List the contents of the tar.bz2 archive to find the paths of the specific files
# echo "Listing contents of $ARCHIVE_NAME..."
# tar -tvf "$ARCHIVE_NAME" > archive_contents.txt

# # Search for the specific files within the archive
# MAIN_PATH=$(grep "main.csv" archive_contents.txt | awk '{print $6}')
# FEEDBACK_CURVES_PATH=$(grep "feedback_curves/" archive_contents.txt | awk '{print $6}' | head -n 1)

# # Check if the specific files were found
# if [ -z "$MAIN_PATH" ] || [ -z "$FEEDBACK_CURVES_PATH" ]; then
#   echo "Required files not found in the archive."
#   exit 1
# fi

# echo "Found main.csv at: $MAIN_PATH"
# echo "Found feedback_curves/ at: $FEEDBACK_CURVES_PATH"

# Measure the time to decompress and copy the data in milliseconds
START_DECOMPRESS_TIME=$(date +%s%3N)
echo "Extracting files from $ARCHIVE_NAME to $DEST_DIR..."
tar -xvf "$ARCHIVE_NAME" -C "$DEST_DIR" "$BASE_NAME/main.csv" "$BASE_NAME/feedback_curves/"
END_DECOMPRESS_TIME=$(date +%s%3N)
DECOMPRESS_TIME=$((END_DECOMPRESS_TIME - START_DECOMPRESS_TIME))

# Calculate the size of the files that were copied
echo "Calculating the size of the extracted files..."
export INPUT_DATA_VOLUME=$(du -cb "$DEST_DIR" "$DEST_DIR" | grep total | awk '{print $1}')
export OUTPUT_DATA_VOLUME=$INPUT_DATA_VOLUME

# Measure the time to move the data to the output directory in milliseconds
START_MOVE_TIME=$(date +%s%3N)
echo "Moving extracted files to the destination directory..."

CURRENT_TIMESTAMP=$(date +"%Y%m%d%H%M%S")
SUBDIR=$(find "$DEST_DIR" -mindepth 1 -maxdepth 1 -type d -name 'data_*M')
# Check if a sub-directory was found
if [ -z "$SUBDIR" ]; then
  echo "No sub-directory found in $DEST_DIR."
  exit 1
fi
# Rename the sub-directory to the desired format
NEW_NAME="$STEP_NAME-out-$CURRENT_TIMESTAMP"
mv "$SUBDIR" "$DEST_DIR/$NEW_NAME"
END_MOVE_TIME=$(date +%s%3N)
MOVE_TIME=$((END_MOVE_TIME - START_MOVE_TIME))

echo "Sub-directory renamed to $NEW_NAME"

echo "Extraction and copy completed successfully."

# Print the variables
echo "STEP_NAME: $STEP_NAME"
echo "RESOURCE_NAME: $RESOURCE_NAME"
echo "NUM_INPUTS: $NUM_INPUTS"
echo "INPUT_DATA_VOLUME: $INPUT_DATA_VOLUME bytes"
echo "NUM_OUTPUTS: $NUM_OUTPUTS"
echo "OUTPUT_DATA_VOLUME: $OUTPUT_DATA_VOLUME bytes"
echo "DRY_RUN_ID: $DRY_RUN_ID"
echo "DECOMPRESS_TIME: $DECOMPRESS_TIME milliseconds"
echo "MOVE_TIME: $MOVE_TIME milliseconds"

# Add an entry to the step_metrics.csv file
echo "Logging metrics to $LOG_FILE..."
if [ ! -f "$LOG_FILE" ]; then
  echo "DRY_RUN_ID,CURRENT_TIMESTAMP,STEP_NAME,RESOURCE_NAME,NUM_INPUTS,INPUT_DATA_VOLUME,NUM_OUTPUTS,OUTPUT_DATA_VOLUME,STEP_PROCESSING_TIME,DATA_TRANSMISSION_TIME" > "$LOG_FILE"
fi
echo "$DRY_RUN_ID,$CURRENT_TIMESTAMP,$STEP_NAME,$RESOURCE_NAME,$NUM_INPUTS,$INPUT_DATA_VOLUME,$NUM_OUTPUTS,$OUTPUT_DATA_VOLUME,$DECOMPRESS_TIME,$MOVE_TIME" >> "$LOG_FILE"
echo "Metrics logged successfully."

# Create the RabbitMQ queue if it does not exist
echo "Creating RabbitMQ queue $QUEUE_NAME if it does not exist..."
rabbitmqadmin declare queue name=$QUEUE_NAME durable=true \
  --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" --host="$RABBITMQ_HOST" --port="$RABBITMQ_PORT"

# Send a message to RabbitMQ
MESSAGE="{\"DATA\": \"$NEW_NAME\", \"SOURCE\": \"$RESOURCE_NAME\"}"
echo "Sending message to RabbitMQ queue $QUEUE_NAME..."
rabbitmqadmin publish routing_key=$QUEUE_NAME payload="$MESSAGE" \
  --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" --host="$RABBITMQ_HOST" --port="$RABBITMQ_PORT"
echo "Message sent successfully."