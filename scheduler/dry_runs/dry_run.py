from pipelines.pipeline import *
from resources.computing_resources import *

class StepDryRunResult:
    """
    This class represents the result of a dry run of a PipelineStep.

    Attributes:
        step (PipelineStep): The PipelineStep object that was tested.
        resource (SimpleComputingResource): The resource on which the step was tested.
        num_inputs (int): The number of inputs for the step during the dry run.
        input_data_volume (float): The input data volume of the dry run in megabytes.
        avg_cpu_percentage (float): The average CPU usage in percentage during the dry run.
        max_cpu_percentage (float): The maximum CPU usage in percentage during the dry run.
        max_memory_usage (float): The maximum memory usage during the dry run in megabytes.
        timeline (StepExecutionTimeline): The timeline for executing the step.
        num_outputs (int): The number of outputs produced by the step during the dry run.
        avg_output_size (float): The average size of an output produced by the step during the dry run in megabytes.
        pipeline_input_volume (float): The input data volume for the pipeline in megabytes.
    """
    
    def __init__(self, step: PipelineStep, resource: SimpleComputingResource, num_inputs: int, input_data_volume: float, 
                 avg_cpu_percentage: float, max_cpu_percentage: float, max_memory_usage: float,
                 timeline: StepExecutionTimeline, num_outputs: int, avg_output_size: float, pipeline_input_volume: float = 0.0):
        self.step = step
        self.resource = resource
        self.input_data_volume = input_data_volume
        self.avg_cpu_percentage = avg_cpu_percentage
        self.max_cpu_percentage = max_cpu_percentage
        self.max_memory_usage = max_memory_usage
        self.timeline = timeline
        self.num_outputs = num_outputs
        self.avg_output_size = avg_output_size
        self.num_inputs = num_inputs
        self.pipeline_input_volume = pipeline_input_volume

    def __repr__(self):
        return f"StepDryRun of {self.step.__class__.__name__} on {self.resource.__class__.__name__}"

class DryRun:
    """
    Represents a collection of StepDryRuns in a single DryRun of a pipeline.

    Attributes:
        step_dry_runs (list of StepDryRun): List of StepDryRun instances.
        pipeline (Pipeline): The pipeline with which this DryRun is associated.
    """
    def __init__(self, pipeline: Pipeline):
        self.step_dry_runs: List[StepDryRunResult] = []
        self.pipeline = pipeline
        self.pipeline_input_volume = 0.0

    def add_step_dry_run(self, step_dry_run_result: StepDryRunResult):
        
        """Adds a StepDryRun instance to the DryRun if the step is in the pipeline."""
        if step_dry_run_result.step in self.pipeline.steps:
            self.step_dry_runs.append(step_dry_run_result)
        else:
            raise ValueError(f"Step {step_dry_run_result.step} is not in the associated pipeline.")
        self.pipeline_input_volume = self.get_dry_run_pipeline_input_volume()
        for step_dry_run in self.step_dry_runs:
            step_dry_run.pipeline_input_volume = self.pipeline_input_volume

    def get_dry_run_pipeline_input_volume(self) -> float:
        """
        Returns the sum of input_data_volume variables of each StepDryRunResult that has a step associated with a type DataSource.

        Returns:
            float: The total input data volume for DataSource steps.
        """
        
        if self.pipeline_input_volume > 0.0:
            return self.pipeline_input_volume
        
        total_input_volume = 0.0
        for step_dry_run in self.step_dry_runs:
            if isinstance(step_dry_run.step, DataSource):
                total_input_volume += step_dry_run.input_data_volume
        return total_input_volume

    def __repr__(self):
        return f"DryRun with {len(self.step_dry_runs)} StepDryRuns"
