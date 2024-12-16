#!/bin/bash

# Define the jobs and the nodes to cycle through
jobs=("deploy-01-01" "deploy-01-02" "deploy-02-01" "deploy-02-02" "deploy-02-03")
nodes=("fog2" "cloud1" "cloud2" "cloud3" "cloud4" "cloud5" "cloud6" "cloud7" "cloud8")
namespace="default"  # Namespace to run the jobs in
dry_run_id=11

# Directories
input_dir="/home/ubuntu/samples/"
out_dir="/out"
done_dir="/done"

# Function to modify the job name by appending a postfix
modify_job_name() {
    local yaml_file="$1.yaml"  # The YAML file to modify
    local postfix=$2    # The postfix to append to the job name

    echo "Modifying the job name in $yaml_file by appending -$postfix..."

    # Use yq to append the postfix to the job name in the YAML file
    yq e '
      .metadata.name = .metadata.name + "-'"$postfix"'"' \
      -i $yaml_file
}

# Function to update DRY_RUN_ID in the YAML file using yq
update_dry_run_id_in_yaml() {
    local job=$1
    local yaml_file="${job}.yaml"

    echo "Updating DRY_RUN_ID in $yaml_file with value $dry_run_id..."

    # Use yq to update the DRY_RUN_ID environment variable in the YAML file
    yq e '
      (.spec.template.spec.containers[].env[] | select(.name == "DRY_RUN_ID") | .value) = "'"$dry_run_id"'"' \
      -i $yaml_file
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

# Function to patch node affinity for an existing job
patch_node_affinity() {
    local job=$1
    local node=$2
    
    echo "Deleting job $job..."
    kubectl delete job $job -n $namespace

    echo "Patching node affinity for $job to use node $node..."
    yq e '.spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values[0] = "'"$node"'"' -i $job.yaml

    echo "Applying job $job with node affinity..."
    kubectl apply -f $job.yaml -n $namespace
}

# Function to move the current .tar.bz2 file in /out to /done
move_current_out_to_done() {
    if [ -n "$(ls -A $out_dir/*.tar.bz2 2>/dev/null)" ]; then
        echo "Moving current .tar.bz2 file from $out_dir to $done_dir..."
        mv $out_dir/*.tar.bz2 $done_dir/
    fi
}

# Function to move the next .tar.bz2 file from input_dir to out_dir
move_next_input_to_out() {
    local next_file=$(ls -1 $input_dir/*.tar.bz2 2>/dev/null | head -n 1)
    if [ -n "$next_file" ]; then
        echo "Moving $next_file to $out_dir..."
        mv "$next_file" "$out_dir/"
    else
        echo "No more .tar.bz2 files to process in $input_dir."
        exit 0
    fi
}

# Main loop: cycle through each .tar.bz2 file in input_dir
while [ -n "$(ls -A $input_dir/*.tar.bz2 2>/dev/null)" ]; do
    # Move the current .tar.bz2 file in /out to /done
    move_current_out_to_done

    # Move the next .tar.bz2 file from input_dir to out_dir
    move_next_input_to_out

    # Get the base name of the newly moved file
    new_file=$(ls -1 $out_dir/*.tar.bz2 2>/dev/null | head -n 1)
    BASE_NAME=$(basename "$new_file" .tar.bz2)
    echo "Processing file: $BASE_NAME"

    # Loop through each node and apply all jobs to each node sequentially
    for node in "${nodes[@]}"; do
        echo "Starting job cycle for node $node..."

        # Loop through each job and patch it with the current node affinity
        for job in "${jobs[@]}"; do
            update_dry_run_id_in_yaml $job
            modify_job_name $job $node
            patch_node_affinity $job $node
            wait_for_job "$job-$node"
            # TODO how do we take care of files??
        done
        # Increment the DRY_RUN_ID for the next dry run
        dry_run_id=$((dry_run_id + 1))

        # Run script to clean all outputs
        /home/ubuntu/invoke_scripts_remotely.sh
        # TODO this needs to only be for the node

        echo "Job cycle completed for node $node."
    done
done

echo "All jobs completed successfully for all nodes."