import random
from dry_runs.dry_run import *
from pipelines.pipeline import *
from resources.computing_resources import *


class DryRunGenerator:
    """
    This class generates a DryRun for a given pipeline.

    Attributes:
        pipeline (Pipeline): The pipeline for which to generate the DryRun.
        resource (SimpleComputingResource): The resource on which the pipeline will be run.
        target_input_data_volume (float): The input data volume that the pipeline would have if extrapolating from the references using linear extrapolation using input_volume as a variable.

    Methods:
        generate_random_dry_run() -> DryRun:
            Generates a DryRun with random StepDryRunResults for each step in the pipeline.
        generate_targeted_step_dry_run_result(step: PipelineStep, reference_input_volume: float, target_input_volume: float, target_num_inputs: int, target_num_outputs: int, target_avg_cpu_percentage: float, target_max_cpu_percentage: float, target_max_memory_usage: float, target_timeline: StepExecutionTimeline, target_avg_output_size: float) -> StepDryRunResult:
            Generates a StepDryRunResult with reference values based on the given target values and reference input volume.
        generate_reference_timeline(target_input_volume: float, reference_input_volume: float, target_timeline: StepExecutionTimeline) -> StepExecutionTimeline:
            Generates a StepExecutionTimeline with reference values based on the given target values and reference input volume.
    """

    def __init__(self, pipeline: Pipeline, resource: SimpleComputingResource, target_input_data_volume: float):
        self.pipeline = pipeline
        self.resource = resource
        self.target_input_data_volume = target_input_data_volume
        self.target_step_dry_run_results = []

    def generate_random_dry_run(self) -> DryRun:
        """
        Generates a DryRun with random StepDryRunResults for each step in the pipeline using a target_input_data_volume as reference. The target_input_data_volume is the input data volume that the pipeline would have if extrapolating from the references using linear extrapolation using input_volume as a variable.

        Returns:
            DryRun: The generated DryRun object.
        """
        dry_run = DryRun(self.pipeline)
        
        # Obtain the maximum values of CPU and memory for the resource
        max_cpu_percentage_in_resource = self.resource.num_cpus * 100.0
        max_memory_in_resource = self.resource.ram_capacity * 1024.0

        # Set the minimum values of CPU and memory for the target values
        min_cpu_percentage = max_cpu_percentage_in_resource / 3.0
        min_memory_usage = max_memory_in_resource / 2.0

        for step in self.pipeline.steps:
            target_step_dry_run_result = next((result for result in self.target_step_dry_run_results if result.step == step), None)

            if target_step_dry_run_result is None:
                target_num_inputs = None
                target_avg_cpu_percentage = random.uniform(min_cpu_percentage, max_cpu_percentage_in_resource)
                target_max_cpu_percentage = random.uniform(target_avg_cpu_percentage, max_cpu_percentage_in_resource)
                target_max_memory_usage = random.uniform(min_memory_usage, max_memory_in_resource)
                target_timeline = self.generate_random_timeline()
                target_num_outputs = None
                target_avg_output_size = random.uniform(self.target_input_data_volume, self.target_input_data_volume*2.0)
                
                # Determine the type of pipeline step and set the number of inputs and outputs accordingly
                if isinstance(step, DataSource):
                    target_num_inputs = 0
                    target_num_outputs = 1
                if isinstance(step, DataSink):
                    target_num_inputs = 1
                    target_num_outputs = 0
                if isinstance(step, BatchStep):
                    target_num_inputs = 1
                    target_num_outputs = 1
                if isinstance(step, ProducerStep):
                    target_num_inputs = 1
                    target_num_outputs = random.randint(2, 10)
                if isinstance(step, ConsumerStep):
                    target_num_inputs = random.randint(2, 10)
                    print(f"NUM_INPUTS: {target_num_inputs}")
                    target_num_outputs = 1
            
                target_step_dry_run_result = StepDryRunResult(step, self.resource, target_num_inputs, self.target_input_data_volume, target_avg_cpu_percentage, target_max_cpu_percentage, target_max_memory_usage, target_timeline, target_num_outputs, target_avg_output_size)
                self.target_step_dry_run_results.append(target_step_dry_run_result)

            reference_input_volume = random.uniform(1.0, self.target_input_data_volume)
            reference_step_dry_run_result = self.generate_targeted_step_dry_run_result(step, self.resource, reference_input_volume, self.target_input_data_volume, target_step_dry_run_result.num_inputs, target_step_dry_run_result.num_outputs, target_step_dry_run_result.avg_cpu_percentage, target_step_dry_run_result.max_cpu_percentage, target_step_dry_run_result.max_memory_usage, target_step_dry_run_result.timeline, target_step_dry_run_result.avg_output_size)
            dry_run.add_step_dry_run(reference_step_dry_run_result)
            # print(f"REFERENCE - Name: '{step.name}', Input Volume: {reference_input_volume}, Num Inputs: {reference_step_dry_run_result.num_inputs}, Num Outputs: {reference_step_dry_run_result.num_outputs}, Avg CPU: {reference_step_dry_run_result.avg_cpu_percentage}, Max CPU: {reference_step_dry_run_result.max_cpu_percentage}, Max Memory: {reference_step_dry_run_result.max_memory_usage}, Timeline: {reference_step_dry_run_result.timeline}, Avg Output Size: {reference_step_dry_run_result.avg_output_size}")

            # print(f"TARGET - Name: '{step.name}', Input Volume: {target_step_dry_run_result.input_data_volume}, Num Inputs: {target_step_dry_run_result.num_inputs}, Num Outputs: {target_step_dry_run_result.num_outputs}, Avg CPU: {target_step_dry_run_result.avg_cpu_percentage}, Max CPU: {target_step_dry_run_result.max_cpu_percentage}, Max Memory: {target_step_dry_run_result.max_memory_usage}, Timeline: {target_step_dry_run_result.timeline}, Avg Output Size: {target_step_dry_run_result.avg_output_size}")

        return dry_run
    
    def generate_random_timeline(self) -> StepExecutionTimeline:
        """
        Generates a StepExecutionTimeline with random values for provisioning_and_deployment_time, data_transmission_time, and step_processing_time.

        Returns:
            StepExecutionTimeline: The generated StepExecutionTimeline object.
        """
        provisioning_and_deployment_time = random.uniform(1.0, 100.0)
        data_transmission_time = random.uniform(1.0, 100.0)
        step_processing_time = random.uniform(1.0, 100.0)

        return StepExecutionTimeline(provisioning_and_deployment_time, data_transmission_time, step_processing_time)

    def generate_targeted_step_dry_run_result(self, step: PipelineStep, resource: SimpleComputingResource, reference_input_volume: float, target_input_volume: float, target_num_inputs: int, target_num_outputs: int, target_avg_cpu_percentage: float, target_max_cpu_percentage: float, target_max_memory_usage: float, target_timeline: StepExecutionTimeline, target_avg_output_size: float) -> StepDryRunResult:
        """
        Generates a StepDryRunResult with reference values based on the given target values and reference input volume. The target values are the values that the step would have if extrapolating from the references using linear extrapolation using input_volume as a variable.

        Args:
            step (PipelineStep): The step for which to generate the StepDryRunResult.
            resource (SimpleComputingResource): The computing resource to be used for the StepDryRunResult.
            reference_input_volume (float): The reference input volume.
            target_input_volume (float): The target input volume.
            target_num_inputs (int): The target number of inputs.
            target_num_outputs (int): The target number of outputs.
            target_avg_cpu_percentage (float): The target average CPU percentage.
            target_max_cpu_percentage (float): The target maximum CPU percentage.
            target_max_memory_usage (float): The target maximum memory usage.
            target_timeline (StepExecutionTimeline): The target timeline.
            target_avg_output_size (float): The target average output size.

        Returns:
            StepDryRunResult: The generated StepDryRunResult object.
        """
        ratio = target_input_volume / reference_input_volume if reference_input_volume != 0 else 0

        reference_num_inputs = math.ceil(target_num_inputs / ratio) if ratio != 0 else 0
        reference_num_outputs = math.ceil(target_num_outputs / ratio) if ratio != 0 else 0
        reference_avg_cpu_percentage = target_avg_cpu_percentage / ratio if ratio != 0 else 0
        reference_max_cpu_percentage = target_max_cpu_percentage / ratio if ratio != 0 else 0
        reference_max_memory_usage = target_max_memory_usage / ratio if ratio != 0 else 0
        reference_avg_output_size = target_avg_output_size / ratio if ratio != 0 else 0

        # if step.name == 'Sink 1':
        #     print('Sink 1 Target input volume: ' + str(target_input_volume))
        #     print('Sink 1 Target MAX CPU: ' + str(target_max_cpu_percentage))
        #     print('Sink 1 Target MAX memory: ' + str(target_max_memory_usage))

        reference_timeline = self.generate_reference_timeline(target_input_volume, reference_input_volume, target_timeline)

        return StepDryRunResult(step, resource, reference_num_inputs, reference_input_volume, reference_avg_cpu_percentage, reference_max_cpu_percentage, reference_max_memory_usage, reference_timeline, reference_num_outputs, reference_avg_output_size)
    
    def generate_reference_timeline(self, target_input_volume: float, reference_input_volume: float, target_timeline: StepExecutionTimeline) -> StepExecutionTimeline:
        """
        Generates a StepExecutionTimeline with reference values based on the given target values and reference input volume. The target values are the values that the step would have if extrapolating from the references using linear extrapolation using input_volume as a variable.

        Args:
            target_input_volume (float): The target input volume.
            reference_input_volume (float): The reference input volume.
            target_timeline (StepExecutionTimeline): The target timeline.

        Returns:
            StepExecutionTimeline: The generated StepExecutionTimeline object.
        """
        ratio = target_input_volume / reference_input_volume if reference_input_volume != 0 else 0

        reference_provisioning_and_deployment_time = target_timeline.provisioning_and_deployment_time / ratio if ratio != 0 else 0
        reference_data_transmission_time = target_timeline.data_transmission_time / ratio if ratio != 0 else 0
        reference_step_processing_time = target_timeline.step_processing_time / ratio if ratio != 0 else 0

        return StepExecutionTimeline(reference_provisioning_and_deployment_time, reference_data_transmission_time, reference_step_processing_time)