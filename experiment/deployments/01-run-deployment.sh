#!/bin/bash

# Check if the CSV file is provided as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <csv_file>"
    exit 1
fi

# CSV File and Timing Config
csv_file="$1"
nodes=("Fog2" "Fog3" "Cloud1" "Cloud2" "Cloud3" "Cloud4" "Cloud5" "Cloud6" "Cloud7" "Cloud8")
namespace="default"  # Namespace to run the jobs in
output_csv="deployment_times.csv"  # Output CSV file to save the total time and deployment CSV name
step_times_csv="step_times.csv"  # Output CSV file to save the start time, end time, and total time for each step
dry_run_id=1

# Extract the number of MBs from the csv_file string
input_mbs=$(echo "$csv_file" | grep -oP 'input\K[0-9]+(?=MB)')

# Global counter for job postfix
job_counter=1

# Function to delete container images on the target host
delete_container_images() {
    local node=$1
    echo "Deleting all container images on $node..."
    ssh ubuntu@$node "sudo k3s crictl rmi --all"
}

clean_out() {
    local node=$1
    echo "Cleaning output folder on $node..."
    ssh ubuntu@$node "sudo rm -rf /out/*"
}

# Function to update DRY_RUN_ID in the YAML file using yq
update_dry_run_id_in_yaml() {
    local job=$1
    local dry_run_id=$2
    local yaml_file="${job}.yaml"

    echo "Updating DRY_RUN_ID in $yaml_file with value $dry_run_id..."

    # Use yq to update the DRY_RUN_ID environment variable in the YAML file
    yq e '(.spec.template.spec.containers[].env[] | select(.name == "DRY_RUN_ID") | .value) = "'"$dry_run_id"'"' -i $yaml_file
}

# Function to update DRY_RUN_ID in the YAML file using yq
update_max_retries_in_yaml() {
    local job=$1
    local dry_run_id=$2
    local yaml_file="${job}.yaml"

    echo "Updating MAX_RETRIES in $yaml_file with value 5000..."

    # Use yq to update the DRY_RUN_ID environment variable in the YAML file
    yq e '(.spec.template.spec.containers[].env[] | select(.name == "MAX_RETRIES") | .value) = "'"5000"'"' -i $yaml_file
}

# Function to patch node affinity for an existing job
patch_node_affinity() {
    local job=$1
    local node=$2

    echo "Patching node affinity for $job to use node $node..."
    yq e '.spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values[0] = "'"$node"'"' -i $job.yaml
}

# Function to update the job name with a postfix number
update_job_name_with_postfix() {
    local job=$1
    local postfix=$2
    local yaml_file="${job}.yaml"

    echo "Updating job name in $yaml_file with postfix $postfix..."

    # Use yq to update the job name in the YAML file
    yq e '.metadata.name = "'"$job"'" + "-'"$postfix"'"' -i $yaml_file
}

# Function to start a job
deploy_job() {
    local job=$1
    local node=$2
    local postfix=$3

    echo "Deploying job $job on node $node with postfix $postfix..."
    patch_node_affinity $job $node
    update_job_name_with_postfix $job $postfix
    kubectl apply -f ${job}.yaml -n $namespace
}

# Function to wait for a job to complete
wait_for_job() {
    local job=$1
    echo "Waiting for job $job to complete..."
    kubectl wait --for=condition=complete job/$job -n $namespace --timeout=60000s
    if [ $? -ne 0 ]; then 
        echo "Job $job failed to complete within the timeout."
        exit 1
    fi
    echo "Job $job completed successfully."
}

# Delete container images on all nodes
for node in "${nodes[@]}"; do
    delete_container_images $node
    clean_out $node
done

kubectl delete jobs --all -n $namespace

# Record the start time
start_time=$(date +%s)

# Variables to track the job with the highest end_position
max_end_position=-1
max_end_position_job=""

# Initialize the step times CSV file
if [ ! -f "$step_times_csv" ]; then
    echo "dry_run_id,job_name,start_time,end_time,total_time,input_mbs" > "$step_times_csv"
fi

# Variable to store the start time of the first job
first_job_start_time=-1

# Main Loop: Read from CSV and deploy jobs
while IFS=',' read -r job start_position end_position node cpu mem; do
    # Skip header or source rows
    if [[ "$job" == "source" || "$job" == "Step Name" ]]; then
        continue
    fi

    # Convert end_position to a floating point number
    end_position=$(echo "$end_position" | tr -d '\r' | awk '{printf "%.2f", $1}')

    # Determine corresponding job YAML
    yaml_file="deploy-${job}.yaml"
    # echo "Processing $yaml_file..."
    echo "job: $job, start_position: $start_position, end_position: $end_position, node: $node, cpu: $cpu, mem: $mem"
    if [[ ! -f "$yaml_file" ]]; then
        echo "YAML file $yaml_file does not exist. Skipping."
        continue
    fi

    # Schedule the job deployment based on start_position
    (
        sleep "$start_position"
        echo "Deploying $job..."
        update_dry_run_id_in_yaml "deploy-${job}" "$dry_run_id"
        update_max_retries_in_yaml "deploy-${job}" "$dry_run_id"
        deploy_job "deploy-${job}" "$node" "$job_counter"
        
        # Record the start time for the job
        job_start_time=$(date +%s)
        
        # Set the first job start time if not already set
        if [ $first_job_start_time -eq -1 ]; then
            first_job_start_time=$job_start_time
        fi
        
        # Calculate the relative start time for the job
        relative_start_time=$((job_start_time - first_job_start_time))
        
        # Wait for the job to complete
        wait_for_job "deploy-${job}-$job_counter"
        
        # Record the end time for the job
        job_end_time=$(date +%s)
        
        # Calculate the total time for the job
        job_total_time=$((job_end_time - job_start_time))
        
        # Save the job times to the step times CSV file
        echo "$dry_run_id,$job,$relative_start_time,$job_end_time,$job_total_time,$input_mbs" >> "$step_times_csv"
    ) &

    # Track the job with the highest end_position
    if (( $(echo "$end_position > $max_end_position" | bc -l) )); then
        max_end_position=$end_position
        max_end_position_job="deploy-${job}-$job_counter"
    fi

    # Increment the job counter
    job_counter=$((job_counter + 1))
done < "$csv_file"

# Wait for all background jobs to complete
wait

# Record the end time and calculate the total time of execution
end_time=$(date +%s)
total_time=$((end_time - start_time))

# Output the total time of execution
echo "Total time of execution: $total_time seconds"

# Save the total_time, dry_run_id, input_mbs, and the name of the deployment CSV file to the output CSV file
if [ ! -f "$output_csv" ]; then
    echo "dry_run_id,deployment_csv,total_time,input_mbs" > "$output_csv"
fi
echo "$dry_run_id,$csv_file,$total_time,$input_mbs" >> "$output_csv"

# Increment the dry_run_id and update it in the script file
next_dry_run_id=$((dry_run_id + 1))
sed -i "s/^dry_run_id=[0-9]*/dry_run_id=$next_dry_run_id/" "$0"

echo "All deployments completed."