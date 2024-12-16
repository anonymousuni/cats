from pipelines.pipeline import *
from resources.computing_resources import *
from scheduling.estimations import *
from dry_runs.dry_run import *
from scheduling.timeline import *
import matplotlib.pyplot as plt
from scheduling.timeline_scheduler import TimelineScheduler, ForcedDeployment
from dry_runs.dry_run_generator import DryRunGenerator
import csv
from typing import Dict, List
import datetime
import argparse

# Argument parsing
parser = argparse.ArgumentParser(description='Pipeline Scheduler')
parser.add_argument('--display_timelines', type=int, default=0, help='Set to 1 to display pipelines, 0 otherwise')
args = parser.parse_args()

def plot_timeline(timeline):
    event_positions = [event.position for event in timeline.events]
    event_names = [event.name for event in timeline.events]

    plt.figure(figsize=(10, 2))
    plt.plot(event_positions, [1]*len(event_names), 'o', markersize=12, color='blue')
    plt.yticks([])

    for i, (event, pos) in enumerate(zip(event_names, event_positions)):
        plt.text(pos, 1.5, event, ha='center', fontsize=12)

    plt.title('Timeline')
    plt.xlabel('Time')
    plt.xlim(min(event_positions) - 1, max(event_positions) + 1)
    plt.ylim(0, 3)
    plt.grid(True, which='both', linestyle='--', axis='x')

    plt.show()

def parse_resources_csv(file_path, network_graph: NetworkGraph) -> List[SimpleComputingResource]:
    resources: List[SimpleComputingResource] = []

    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            instance_type = row['Instance type']
            name = row['Name']
            memory_capacity = float(row['Memory capacity (GiB)'])
            num_cpus = int(row['Number of CPUs'])
            cpu_frequency = float(row['CPU frequency (GHz)'])
            num_nodes = int(row['Number of nodes'])
            node_type = row['Node type']
            availability_zone = row['AZ']
            on_demand_hourly_rate = float(row['On-demand hourly rate (USD)'])

            for _ in range(num_nodes):
                if node_type == 'EC2':
                    resource = AmazonOnDemandEC2Instance(
                        name=name,
                        num_cpus=num_cpus,
                        cpu_frequency=cpu_frequency,
                        ram_capacity=memory_capacity,
                        availability_zone=availability_zone,
                        on_demand_price_per_hour=on_demand_hourly_rate
                    )
                    # Add the new resource to the network graph
                    network_graph.add_node(resource, [])

                    # Connect the new resource to all existing nodes in the network graph
                    for existing_node in network_graph.nodes:
                        if existing_node is not resource:  # Avoid self-connection
                            if node_type == 'EC2':
                                if isinstance(existing_node, AmazonOnDemandEC2Instance):
                                    network_graph.add_edge(resource, existing_node, 1000)
                                    network_graph.add_edge(existing_node, resource, 1000)
                                elif isinstance(existing_node, SimpleComputingResource):
                                    network_graph.add_edge(resource, existing_node, 50)
                                    network_graph.add_edge(existing_node, resource, 50)
                else:
                    resource = SimpleComputingResource(
                        name=name,
                        num_cpus=num_cpus,
                        cpu_frequency=cpu_frequency,
                        ram_capacity=memory_capacity,
                        total_price=0  # Assuming total_price is 0 for SimpleComputingResource
                    )

                    # Add the new resource to the network graph
                    network_graph.add_node(resource, [])

                    # Connect the new resource to all existing nodes in the network graph
                    for existing_node in network_graph.nodes:
                        if existing_node is not resource:  # Avoid self-connection
                            if node_type == 'Fog':
                                if isinstance(existing_node, AmazonOnDemandEC2Instance):
                                    network_graph.add_edge(resource, existing_node, 50)
                                    network_graph.add_edge(existing_node, resource, 50)
                                elif isinstance(existing_node, SimpleComputingResource):
                                    network_graph.add_edge(resource, existing_node, 1000)
                                    network_graph.add_edge(existing_node, resource, 1000)

                resources.append(resource)

    return resources

def read_csv(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def find_resource_by_name(resources: List[SimpleComputingResource], name):
    for resource in resources:
        if resource.name == name:
            return resource
    return None

def find_step_by_name(steps, name):
    for step in steps:
        if step.name == name:
            return step
    return None

def read_deployment_metrics(file_path):
    deployment_metrics = {}
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            step_name = row['step_name'].lower()
            node_name = row['node_name'].lower()
            average_instance_start_time_seconds = float(row['average_instance_start_time_seconds'])
            deployment_metrics[(step_name, node_name)] = average_instance_start_time_seconds
    return deployment_metrics

def populate_dry_runs(step_metrics_file, step_performance_metrics_file, deployment_metrics_file, pipeline: Pipeline, eligible_resources: List[SimpleComputingResource], fog1: SimpleComputingResource):
    step_metrics = read_csv(step_metrics_file)
    step_performance_metrics = read_csv(step_performance_metrics_file)
    deployment_metrics = read_deployment_metrics(deployment_metrics_file)
    
    dry_runs: Dict[int, DryRun] = {}
    zero_timeline = StepExecutionTimeline(
        provisioning_and_deployment_time=1.0,
        data_transmission_time=0.0,
        step_processing_time=0.0
    )

    for metric in step_metrics:
        for step in pipeline.steps:
            if step.name == 'source':
                source = step
                break

        dry_run_id = int(metric['DRY_RUN_ID'])
        step_name = metric['STEP_NAME']
        resource_name = metric['RESOURCE_NAME']

        # We set the input size to the total input to the whole pipeline, i.e., the input that to the 01-01-retrieve step
        resource = find_resource_by_name(eligible_resources, resource_name)
        step = find_step_by_name(pipeline.steps, step_name)
        
        if not resource or not step:
            continue
        
        if dry_run_id not in dry_runs:
            dry_runs[dry_run_id] = DryRun(pipeline)
        
        # Get the provisioning and deployment time from the deployment metrics
        provisioning_and_deployment_time = deployment_metrics.get((step_name, resource_name), 0.0)
        
        timeline = StepExecutionTimeline(
            provisioning_and_deployment_time=provisioning_and_deployment_time,
            data_transmission_time=float(metric['DATA_TRANSMISSION_TIME']) / 1000,
            step_processing_time=float(metric['STEP_PROCESSING_TIME']) / 1000  
        )
        
        # We set the input size to the total input to the whole pipeline, i.e., the input that to the 02-01-retrieve step
        if step_name == "02-01-retrieve":
            input_size = float(metric['INPUT_DATA_VOLUME']) / (1024 * 1024)
            source_dry_run_result = StepDryRunResult(
                step=source,
                resource=fog1,
                num_inputs=0,
                input_data_volume=input_size,
                avg_cpu_percentage=0.0,
                max_cpu_percentage=0.0,
                max_memory_usage=0.0,
                timeline=zero_timeline,
                num_outputs=0,
                avg_output_size=0.0,
                pipeline_input_volume=input_size
            )
            dry_runs[dry_run_id].add_step_dry_run(source_dry_run_result)

        step_dry_run_result = StepDryRunResult(
            step=step,
            resource=resource,
            num_inputs=int(metric['NUM_INPUTS']),
            input_data_volume=float(metric['INPUT_DATA_VOLUME']) / (1024 * 1024),  # Convert bytes to megabytes
            avg_cpu_percentage=0,  # Placeholder, will be updated later
            max_cpu_percentage=0,  # Placeholder, will be updated later
            max_memory_usage=0,  # Placeholder, will be updated later
            timeline=timeline,
            num_outputs=int(metric['NUM_OUTPUTS']),
            avg_output_size=float(metric['OUTPUT_DATA_VOLUME']) / (1024 * 1024)  # Convert bytes to megabytes
        )
        
        dry_runs[dry_run_id].add_step_dry_run(step_dry_run_result)

    for metric in step_performance_metrics:
        dry_run_id = int(metric['DRY_RUN_ID'])
        step_name = metric['STEP_NAME']
        resource_name = metric['RESOURCE_NAME']
        
        resource = find_resource_by_name(eligible_resources, resource_name)
        step = find_step_by_name(pipeline.steps, step_name)

        if not resource or not step:
            continue
        
        for step_dry_run in dry_runs[dry_run_id].step_dry_runs:
            if step_dry_run.step == step and step_dry_run.resource == resource:
                step_dry_run.avg_cpu_percentage = float(metric['AVG_CPU']) if metric['AVG_CPU'] else 0.0
                step_dry_run.max_cpu_percentage = float(metric['MAX_CPU']) if metric['MAX_CPU'] else 0.0
                step_dry_run.max_memory_usage = float(metric['MAX_MEM']) if metric['MAX_MEM'] else 0.0
    
    return dry_runs

# Create pipeline steps
source = DataSource("source")
retrieve_1_1 = BatchStep("01-01-retrieve")
process_1_2 = BatchStep("01-02-prepare-and-store")

retrieve_2_1 = BatchStep("02-01-retrieve")
slice_2_2 = ProducerStep("02-02-slice")
process_2_3 = ConsumerStep("02-03-prepare")

# Create pipeline
pipeline = Pipeline()

pipeline.add_connection(source, retrieve_1_1)
pipeline.add_connection(retrieve_1_1, process_1_2)

pipeline.add_connection(source, retrieve_2_1)
pipeline.add_connection(retrieve_2_1, slice_2_2)
pipeline.add_connection(slice_2_2, process_2_3)

pipeline.add_dependency("asynchronous", process_2_3, slice_2_2)
pipeline.add_dependency("synchronous", process_2_3, process_1_2)

fog1 = SimpleComputingResource("", 0, 0, 0, 0)
network_graph = NetworkGraph()
resources_desc_file_path = 'resources.csv'
resources = parse_resources_csv(resources_desc_file_path, network_graph=network_graph)
for resource in resources:
    if(resource.name == "fog1"):
        fog1=resource

# Data source is on fog1 node
forced_deployment = ForcedDeployment(source, fog1)

# We disable any scheduling from occuring in fog1 node
fog1.disable_scheduling()

eligible_resources = network_graph.get_eligible_computing_resources()

dry_runs = populate_dry_runs('step_metrics.csv', 'step_performance_metrics.csv', 'deployment_metrics.csv', pipeline, eligible_resources, fog1)


# Create a zero-time StepExecutionTimeline
zero_timeline = StepExecutionTimeline(
    provisioning_and_deployment_time=1.0,
    data_transmission_time=0.0,
    step_processing_time=0.0
)

# Add synthetic dry runs to the list of dry runs
all_dry_runs = list(dry_runs.values())

input_volumes = [1229, 2483, 3694, 4914, 6143, 7388, 12296, 24664, 36955]

deadline = 1000
budget = 100
prefix = 'metrics3'
for input_vol in input_volumes:
    deadline = 1000
    budget = 100
    input_volume_MB = input_vol
    max_scalability = 1
    prefix = 'metrics3'

    scheduler = TimelineScheduler(pipeline, network_graph, all_dry_runs, deadline, budget, input_volume_MB, [forced_deployment], max_scalability)
    resulting_timelines = scheduler.schedule()
    for timeline in resulting_timelines:
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"timeline_{prefix}_{current_time}_deadline{deadline}_budget{budget}_input{input_volume_MB}MB_maxscalability{max_scalability}.csv"
        timeline.serialize_to_csv(file_name)
        if args.display_timelines == 1:
            timeline.display_timeline()

    scheduler.max_scalability = 6
    max_scalability = 6
    resulting_timelines = scheduler.schedule()
    for timeline in resulting_timelines:
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"timeline_{prefix}_{current_time}_deadline{deadline}_budget{budget}_input{input_volume_MB}MB_maxscalability{max_scalability}.csv"
        timeline.serialize_to_csv(file_name)
        if args.display_timelines == 1:
            timeline.display_timeline()

