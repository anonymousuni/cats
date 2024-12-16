#!/bin/bash

# Set the log directory and file
LOG_DIR="/log"
LOG_FILE="$LOG_DIR/step_performance_metrics.csv"

# Define the required variables
export DRY_RUN_ID=${DRY_RUN_ID:-0}  # Read from environment variable or default to 0
export RESOURCE_NAME=${NODE_NAME:-"default_node_name"}  # Use NODE_NAME environment variable or a default value if not set
export STEP_NAME="01-01-retrieve"
CURRENT_TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Set paths
SCRIPT_TO_RUN="./main.sh"
OUTPUT_LOG="psrecord_output.txt"

# Start your script and monitor it with psrecord
psrecord "$SCRIPT_TO_RUN" --log "$OUTPUT_LOG" --include-children --interval 0.1

# Analyze the log file for maximum and average CPU and maximum memory usage
MAX_CPU=$(awk '{if(NR>1 && $2>max){max=$2}} END {print max}' "$OUTPUT_LOG")
AVG_CPU=$(awk '{sum+=$2; count++} END {if(count>0) print sum/count; else print 0}' "$OUTPUT_LOG")
MAX_MEM=$(awk '{if(NR>1 && $3>max){max=$3}} END {print max}' "$OUTPUT_LOG")

echo "Maximum CPU usage: $MAX_CPU %"
echo "Average CPU usage: $AVG_CPU %"
echo "Maximum memory usage: $MAX_MEM MB"

# Add an entry to the step_performance_metrics.csv file
echo "Logging performance metrics to $LOG_FILE..."
if [ ! -f "$LOG_FILE" ]; then
  echo "DRY_RUN_ID,CURRENT_TIMESTAMP,STEP_NAME,RESOURCE_NAME,MAX_CPU,AVG_CPU,MAX_MEM" > "$LOG_FILE"
fi
echo "$DRY_RUN_ID,$CURRENT_TIMESTAMP,$STEP_NAME,$RESOURCE_NAME,$MAX_CPU,$AVG_CPU,$MAX_MEM" >> "$LOG_FILE"
echo "Performance metrics logged successfully."