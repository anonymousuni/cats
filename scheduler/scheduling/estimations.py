from math import ceil
from typing import List, Optional
from dry_runs.dry_run import DryRun, StepDryRunResult
from pipelines.pipeline import *
from resources.computing_resources import NetworkGraph, SimpleComputingResource
from sklearn.linear_model import LinearRegression
import numpy as np
from scipy.optimize import nnls
from sklearn.ensemble import RandomForestRegressor
 
class StepTimelineEstimation:
    """
    Represents an estimation of the timeline of a step on a given resource.

    Attributes:
        input_volume (float): The input volume of the pipeline.
        pipeline_step (PipelineStep): The step for which the estimation is made.
        resource (SimpleComputingResource): The resource on which the estimation is made.
        timeline_estimation (StepExecutionTimeline): The estimated timeline for the step.
        previous_resource (Optional[SimpleComputingResource]): The resource where the previous step has been deployed for the estimation. Defaults to None.
    """
    def __init__(self, input_volume: float, pipeline_step: PipelineStep, resource: SimpleComputingResource, timeline_estimation: StepExecutionTimeline, previous_resource: Optional[SimpleComputingResource] = None):
        """
        Initialize a StepTimelineEstimation.

        Args:
            input_volume (float): The input volume of the pipeline.
            pipeline_step (PipelineStep): The step for which the estimation is made.
            resource (SimpleComputingResource): The resource on which the estimation is made.
            timeline_estimation (StepExecutionTimeline): The estimated timeline for the step.
            previous_resource (SimpleComputingResource, optional): The resource where the previous step has been deployed for the estimation. Defaults to None.
        """
        self.input_volume = input_volume
        self.pipeline_step = pipeline_step
        self.resource = resource
        self.timeline_estimation = timeline_estimation
        self.previous_resource = previous_resource
    
    def __repr__(self):
        return f"StepTimelineEstimation(input_volume={self.input_volume}, pipeline_step={self.pipeline_step}, resource={self.resource}, timeline_estimation={self.timeline_estimation}, previous_resource={self.previous_resource})"

    def __eq__(self, other):
        if not isinstance(other, StepTimelineEstimation):
            return NotImplemented

        return (self.input_volume == other.input_volume and
                self.pipeline_step == other.pipeline_step and
                self.resource == other.resource and
                self.timeline_estimation == other.timeline_estimation and
                self.previous_resource == other.previous_resource)

class StepTimelineEstimator:
    """
    Class for estimating the timeline of a step on a given resource.

    Attributes:
        pipeline (Pipeline): The pipeline for which the estimation is made.
        dry_runs (List[DryRun]): A list of dry run results for the pipeline.

    """
    def __init__(self, dry_runs: List[DryRun]):
        self.dry_runs = dry_runs
   
    def estimate_input_volume(self, matching_dry_run_results: List[StepDryRunResult], pipeline_input_volume: float) -> float:
        """
        Estimate the input volume for a step given a list of matching dry runs and the input volume for the pipeline using Non-Negative Least Squares (NNLS).
    
        Args:
            matching_dry_run_results: A list of dry run results for the step that match the target computing resource.
            pipeline_input_volume: The input volume for the pipeline.
    
        Returns:
            The estimated input volume for the step.
    
        Raises:
            ValueError: If no matching dry runs are found.
        """
        if not matching_dry_run_results:
            raise ValueError("No matching dry runs found")
    
        # Extract pipeline input volumes and step input volumes from the dry run results
        pipeline_input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        step_input_volumes = np.array([result.input_data_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0])
    
        if len(pipeline_input_volumes) == 0 or len(step_input_volumes) == 0:
            return 0.0  # Handle the case where there are no valid matching results
    
        # Add a column of ones to pipeline_input_volumes to account for the intercept
        A = np.hstack([pipeline_input_volumes, np.ones((pipeline_input_volumes.shape[0], 1))])
    
        # Perform non-negative least squares
        x, _ = nnls(A, step_input_volumes)
    
        # Estimate the input volume for the step
        estimated_input_volume = np.dot(np.array([pipeline_input_volume, 1]), x)
    
        return max(estimated_input_volume, 0.0)

    def estimate_number_of_inputs(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> int:
        """
        Estimate the number of inputs for a step given a list of matching dry runs and the input volume for the step using Non-Negative Least Squares (NNLS).
        
        Args:
            matching_dry_run_results: A list of dry run results for the step that match the target computing resource.
            input_volume: The input volume for the step.
            
        Returns:
            The estimated number of inputs for the step.
            
        Raises:
            ValueError: If no matching dry runs are found.
        """
        if not matching_dry_run_results:
            raise ValueError("No matching dry runs found")
        
        # Extract input volumes and number of inputs from the dry run results
        input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        num_inputs = np.array([result.num_inputs for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        
        if len(input_volumes) == 0 or len(num_inputs) == 0:
            return 0  # Handle the case where there are no valid matching results
        
        # Add a column of ones to input_volumes to account for the intercept
        A = np.hstack([input_volumes, np.ones((input_volumes.shape[0], 1))])
        
        # Perform non-negative least squares
        x, _ = nnls(A, num_inputs)
        
        # Estimate the number of inputs for the given input volume
        estimated_number_of_inputs = ceil(np.dot(np.array([input_volume, 1]), x))
        
        # Ensure the result is non-negative
        return max(estimated_number_of_inputs, 1)

    def estimate_number_of_outputs(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> int:
        """
        Estimate the number of outputs for a step given a list of matching dry runs and the target pipeline input volume for the step using Non-Negative Least Squares (NNLS).
        
        Args:
            matching_dry_run_results: A list of dry run results for the step that match the target computing resource.
            input_volume: The input volume for the step.
            
        Returns:
            The estimated number of outputs for the step.
            
        Raises:
            ValueError: If no matching dry runs are found.
        """
        if not matching_dry_run_results:
            raise ValueError("No matching dry runs found")
        
        # Extract input volumes and number of outputs from the dry run results
        input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        num_outputs = np.array([result.num_outputs for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        
        if len(input_volumes) == 0 or len(num_outputs) == 0:
            return 0  # Handle the case where there are no valid matching results
        
        # Add a column of ones to input_volumes to account for the intercept
        A = np.hstack([input_volumes, np.ones((input_volumes.shape[0], 1))])
        
        # Perform non-negative least squares
        x, _ = nnls(A, num_outputs)
        
        # Estimate the number of outputs for the given input volume
        estimated_number_of_outputs = ceil(np.dot(np.array([input_volume, 1]), x))
        
        # Ensure the result is non-negative
        return max(estimated_number_of_outputs, 1)

    def estimate_provisioning_and_deployment_time(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> float:
        """
        This function estimates the provisioning and deployment time for a given input volume based on prior dry run results using Non-Negative Least Squares (NNLS).
    
        Args:
            matching_dry_run_results (List[StepDryRunResult]): The dry run results for the step on the resource.
            input_volume (float): The input volume of data for the step.
    
        Returns:
            float: The estimated provisioning and deployment time.
        """
        
        # Extract input volumes and provisioning times from the dry run results
        input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        provisioning_times = np.array([result.timeline.get_provisioning_and_deployment_time() for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        
        if len(input_volumes) == 0 or len(provisioning_times) == 0:
            return 0.0  # Handle the case where there are no valid matching results
        
        # Normalize the input volumes
        input_volumes_mean = np.mean(input_volumes)
        input_volumes_std = np.std(input_volumes)
        input_volumes_normalized = (input_volumes - input_volumes_mean) / input_volumes_std
    
        # Add a column of ones to input_volumes to account for the intercept
        A = np.hstack([input_volumes_normalized, np.ones((input_volumes_normalized.shape[0], 1))])
        
        # Perform non-negative least squares
        x, _ = nnls(A, provisioning_times)
        
        # Estimate the provisioning and deployment time for the given input volume
        input_volume_normalized = (input_volume - input_volumes_mean) / input_volumes_std
        estimated_provisioning_time = np.dot(np.array([input_volume_normalized, 1]), x)
        
        # Ensure the result is non-negative
        return max(estimated_provisioning_time, 0.0)

    def estimate_data_transmission_time(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> float:
        """
        This function estimates the data transmission time for a given input volume based on prior dry run results using weighted linear regression.
    
        Args:
            matching_dry_run_results (List[StepDryRunResult]): The dry run results for the step on the resource.
            input_volume (float): The input volume of data for the step.
    
        Returns:
            float: The estimated data transmission time.
        """
        
        # Extract input volumes and data transmission times from the dry run results
        input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        data_transmission_times = np.array([result.timeline.get_data_transmission_time() for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        weights = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        
        if len(input_volumes) == 0 or len(data_transmission_times) == 0:
            return 0.0  # Handle the case where there are no valid matching results
        
        # Perform weighted linear regression
        model = LinearRegression()
        model.fit(input_volumes, data_transmission_times, sample_weight=weights)
        
        # Estimate the data transmission time for the given input volume
        estimated_data_transmission_time = model.predict(np.array([[input_volume]]))[0]
        
        return max(estimated_data_transmission_time, 0.0)

    def estimate_step_processing_time(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> float:
        """
        This function estimates the step processing time for a given input volume based on prior dry run results using Non-Negative Least Squares (NNLS).
    
        Args:
            matching_dry_run_results (List[StepDryRunResult]): The dry run results for the step on the resource.
            input_volume (float): The input volume of data for the step.
    
        Returns:
            float: The estimated step processing time.
        """
        
        # Extract input volumes and step processing times from the dry run results
        input_volumes = np.array([result.pipeline_input_volume for result in matching_dry_run_results if result.pipeline_input_volume != 0]).reshape(-1, 1)
        step_processing_times = np.array([result.timeline.get_step_processing_time() for result in matching_dry_run_results if result.pipeline_input_volume != 0])
        
        if len(input_volumes) == 0 or len(step_processing_times) == 0:
            return 0.0  # Handle the case where there are no valid matching results
        
        # Add a column of ones to input_volumes to account for the intercept
        A = np.hstack([input_volumes, np.ones((input_volumes.shape[0], 1))])
        
        # Perform non-negative least squares
        x, _ = nnls(A, step_processing_times)
        
        # Estimate the step processing time for the given input volume
        estimated_step_processing_time = np.dot(np.array([input_volume, 1]), x)
        

        return max(estimated_step_processing_time, 0.0)
    
    def estimate_timeline(self, step: PipelineStep, resource: SimpleComputingResource, pipeline_input_volume: float) -> StepTimelineEstimation:
        """
        Estimate the timeline of a step on a given resource.

        Args:
            step (PipelineStep): The step to estimate.
            resource (SimpleComputingResource): The resource on which the step will run.
            input_volume (float): The input volume of data for the pipeline in MB.

        Returns:
            StepTimelineEstimation: The estimated timeline of the step on the resource or None if timeline could not be estimated.

        """
        # Flatten the list of StepDryRunResult from all DryRuns
        # Extract all StepDryRunResult objects from the dry runs
        all_dry_run_results = []
        for dry_run in self.dry_runs:
            for dry_run_result in dry_run.step_dry_runs:
                all_dry_run_results.append(dry_run_result)

        # Look for dry run results for the step on the resource
        matching_dry_run_results = [result for result in all_dry_run_results if result.step == step and result.resource == resource]

        if not matching_dry_run_results:
            raise ValueError(f"No dry run results found for step '{step}' on resource '{resource}'")

        # Use the dry run results to estimate the timeline
        estimated_provisioning_and_deployment_time = self.estimate_provisioning_and_deployment_time(matching_dry_run_results, pipeline_input_volume)
        try:
            estimated_data_transmission_time = self.estimate_data_transmission_time(matching_dry_run_results, pipeline_input_volume)
        except ValueError:
            return None
        estimated_step_processing_time = self.estimate_step_processing_time(matching_dry_run_results, pipeline_input_volume)


        number_of_inputs = 1
        number_of_outputs = 1
        if isinstance(step, (ConsumerStep, DataSink)):
            number_of_inputs = self.estimate_number_of_inputs(matching_dry_run_results, pipeline_input_volume)
        if isinstance(step, (ProducerStep, ConsumerStep)):
            number_of_outputs = self.estimate_number_of_outputs(matching_dry_run_results, pipeline_input_volume)
        
        # Construct the timeline based on the step type
        if isinstance(step, BatchStep):
            estimated_timeline = BatchStepExecutionTimeline(provisioning_and_deployment_time=estimated_provisioning_and_deployment_time, 
                                                           data_transmission_time=estimated_data_transmission_time, 
                                                           step_processing_time=estimated_step_processing_time)
        elif isinstance(step, ProducerStep):
            estimated_timeline = ProducerStepExecutionTimeline(provisioning_and_deployment_time=estimated_provisioning_and_deployment_time, 
                                                               data_transmission_time=estimated_data_transmission_time, 
                                                               average_time_to_produce_output=estimated_step_processing_time / number_of_outputs,
                                                               number_of_produced_outputs=number_of_outputs)  # Assuming multiple outputs
        elif isinstance(step, ConsumerStep):
            estimated_timeline = ConsumerStepExecutionTimeline(provisioning_and_deployment_time=estimated_provisioning_and_deployment_time, 
                                                               average_data_transmission_time=estimated_data_transmission_time,
                                                               average_data_processing_time=estimated_step_processing_time / number_of_inputs,
                                                               number_of_transmitted_inputs=number_of_inputs,  # Assuming 1 input
                                                               number_of_produced_outputs=number_of_outputs)  # Assuming 1 output
        elif isinstance(step, DataSink):
            estimated_timeline = DataSinkExecutionTimeline(provisioning_and_deployment_time=estimated_provisioning_and_deployment_time, 
                                                           data_transmission_time=estimated_data_transmission_time, 
                                                           number_of_transmitted_inputs=number_of_inputs)  # Assuming 1 input
        elif isinstance(step, DataSource):
            estimated_timeline = DataSourceExecutionTimeline(provisioning_and_deployment_time=estimated_provisioning_and_deployment_time)
        else:
            raise ValueError("Step type is not supported for timeline estimation.")

        return StepTimelineEstimation(pipeline_input_volume, step, resource, estimated_timeline)

class ContextAwareStepTimelineEstimator(StepTimelineEstimator):
    """
    Class for estimating the timeline of a step on a given resource, taking into account the context.

    Attributes:
        pipeline (Pipeline): The pipeline for which the estimation is made.
        dry_runs (List[DryRun]): A list of dry run results for the pipeline.
        current_resource (SimpleComputingResource): The resource where the current step is deployed.
        previous_resource (SimpleComputingResource): The resource where the previous step was deployed.
        network_graph (NetworkGraph): The hardware network graph.
    """

    def __init__(self, dry_runs: List[DryRun], current_resource: SimpleComputingResource, previous_resource: SimpleComputingResource, network_graph: NetworkGraph):
        super().__init__(dry_runs)
        self.current_resource = current_resource
        self.previous_resource = previous_resource
        self.network_graph = network_graph

    def estimate_data_transmission_time(self, matching_dry_run_results: List[StepDryRunResult], input_volume: float) -> float:
        """
        This function estimates the data transmission time for a given input volume based on the bandwidth between the current and previous resources.

        It first retrieves the bandwidth between the current and previous resources from the network graph. 
        Then, it calculates the data transmission time by dividing the input volume by the bandwidth.

        Args:
            matching_dry_run_results (List[StepDryRunResult]): The dry run results for the step on the resource. 
            input_volume (float): The input volume of data for the step.

        Returns:
            float: The estimated data transmission time in seconds.
        """
        # Get the bandwidth between the current and previous resource
        bandwidth = self.network_graph.get_bandwidth(self.previous_resource, self.current_resource)
        
        step = matching_dry_run_results[0].step

        # Resources are the same, no data transmission time
        if bandwidth == 0:
            return 0

        if bandwidth is None:
            raise ValueError("No connection found between the current and previous resources")

        # Estimate the input volume for the step
        step_estimated_input_volume = self.estimate_input_volume(matching_dry_run_results, input_volume)
        
        # Calculate the data transmission time based on the bandwidth
        # Convert bandwidth from Mbps to MBps
        bandwidth_in_MBps = bandwidth / 8
        data_transmission_time = step_estimated_input_volume / bandwidth_in_MBps

        return data_transmission_time
    
    def estimate_timeline(self, step: PipelineStep, resource: SimpleComputingResource, pipeline_input_volume: float) -> StepTimelineEstimation:
        """
        Estimate the timeline of a step on a given resource given that the previous step has been deployed
        on the resource set for the "previous_resource" class variable of this class.

        Args:
            step (PipelineStep): The step to estimate.
            resource (SimpleComputingResource): The resource on which the step will run.
            input_volume (float): The input volume of data for the pipeline in MB.

        Returns:
            StepTimelineEstimation: The estimated timeline of the step on the resource with a reference to the previous resource.

        """
        # Call the superclass's method
        result = super().estimate_timeline(step, resource, pipeline_input_volume)

        if result is None:
            return None

        # Set the previous_resource parameter
        result.previous_resource = self.previous_resource

        return result

class StepHardwareRequirementsEstimation:
    """
    Represents an estimation of the hardware requirements of a step on a given resource.

    Attributes:
        input_volume (float): The input volume of the pipeline.
        pipeline_step (PipelineStep): The step for which the estimation is made.
        resource (SimpleComputingResource): The resource on which the estimation is made.
        cpu_reservation (float): The estimated CPU reservation for the step.
        memory_reservation (float): The estimated memory reservation for the step.
    """
    def __init__(self, pipeline_input_volume: float, pipeline_step: PipelineStep, resource: SimpleComputingResource, cpu_reservation: float, memory_reservation: float):
        """
        Initialize a StepHardwareRequirementsEstimation.

        Args:
            input_volume (float): The input volume of the pipeline.
            pipeline_step (PipelineStep): The step for which the estimation is made.
            resource (SimpleComputingResource): The resource on which the estimation is made.
            cpu_reservation (float): The estimated CPU reservation for the step.
            memory_reservation (float): The estimated memory reservation for the step.
        """
        self.input_volume = pipeline_input_volume
        self.pipeline_step = pipeline_step
        self.resource = resource
        self.cpu_reservation = cpu_reservation
        self.memory_reservation = memory_reservation

    def __repr__(self):
        return f"StepHardwareRequirementsEstimation(input_volume={self.input_volume}, pipeline_step={self.pipeline_step}, resource={self.resource}, cpu_reservation={self.cpu_reservation}, memory_reservation={self.memory_reservation})"

    def __eq__(self, other):
        if not isinstance(other, StepHardwareRequirementsEstimation):
            return NotImplemented
        return (self.input_volume == other.input_volume and 
                self.pipeline_step == other.pipeline_step and 
                self.resource == other.resource and 
                self.cpu_reservation == other.cpu_reservation and 
                self.memory_reservation == other.memory_reservation)

class StepHardwareRequirementsEstimator:
    """
    This class estimates the hardware requirements for each step in a pipeline.

    Attributes:
        dry_runs (List[DryRun]): A list of dry run objects which include StepDryRunResult objects.
            These results are used to analyze the hardware requirements of the steps in the pipeline.
        pipeline (Pipeline): The pipeline for which to estimate the hardware requirements.
            Each step in the pipeline will be analyzed individually.

    Methods:
        estimate_ram_requirements(step: PipelineStep, resource: SimpleComputingResource, input_volume: float) -> float:
            Estimates the RAM requirements for a given step on a given resource for a given input volume.
            The estimation is based on the max_memory_usage of the step's dry run results on the resource.

        estimate_cpu_requirements(step: PipelineStep, resource: SimpleComputingResource, input_volume: float) -> float:
            Estimates the CPU requirements (in percent) for a given step on a given resource for a given input volume.
            The estimation is based on the avg_cpu_percentage and max_cpu_percentage of the step's dry run results on the resource. 

        estimate_hardware_requirements(step: PipelineStep, resource: SimpleComputingResource, input_volume: float) -> StepHardwareRequirementsEstimation:
            Creates an instance of StepHardwareRequirementsEstimation with the results of the functions estimate_ram_requirements and estimate_cpu_requirements.
    """

    def __init__(self, dry_runs: List[DryRun]):
        """
        Initializes a new StepHardwareRequirementsEstimator.

        Args:
            dry_runs (List[DryRun]): A list of dry run objects which include StepDryRunResult objects.
                These results are used to analyze the hardware requirements of the steps in the pipeline.
        """
        self.dry_runs = dry_runs
    
    def estimate_ram_requirements(self, step: PipelineStep, resource: SimpleComputingResource, pipeline_input_volume: float) -> float:
        """
        Estimates the RAM requirements for a given step and resource based on the input volume of a pipeline using a Random Forest model.
    
        Args:
            step (PipelineStep): The step for which to estimate the RAM requirements.
            resource (SimpleComputingResource): The computing resource for which to estimate the RAM requirements.
            pipeline_input_volume (float): The input volume for which to estimate the RAM requirements.
    
        Returns:
            float: The estimated RAM requirements.
            
        Raises:
            ValueError: If no dry run results are found for the given step and resource.
        """
        # Extract all StepDryRunResult objects from the dry runs
        all_dry_run_results = [result for dry_run in self.dry_runs for result in dry_run.step_dry_runs]
    
        # Filter the results for the given step and resource
        filtered_results = [result for result in all_dry_run_results if result.step == step and result.resource == resource]
    
        if not filtered_results:
            raise ValueError("No dry run results found for the given step and resource.")
    
        # Prepare the data for the Random Forest model
        input_volumes = []
        ram_usages = []
        for result in filtered_results:
            if result.pipeline_input_volume > 0:
                input_volumes.append(result.pipeline_input_volume)
                ram_usages.append(result.max_memory_usage)
    
        if not input_volumes:
            raise ValueError("No valid dry run results with positive input volume found for the given step and resource.")
    
        # Convert lists to numpy arrays
        input_volumes = np.array(input_volumes).reshape(-1, 1)
        ram_usages = np.array(ram_usages)
    
        # Train the Random Forest model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(input_volumes, ram_usages)
    
        # Estimate the RAM requirements for the given pipeline input volume
        estimated_ram = model.predict(np.array([[pipeline_input_volume]]))[0]
    
        return max(estimated_ram, 0.0)
    
    def estimate_cpu_requirements(self, step: PipelineStep, resource: SimpleComputingResource, pipeline_input_volume: float) -> float:
        """
        Estimates the CPU requirements for a given step and resource based on the input volume of a pipeline using a Random Forest model.
    
        Args:
            step (PipelineStep): The step for which to estimate the CPU requirements.
            resource (SimpleComputingResource): The computing resource for which to estimate the CPU requirements.
            pipeline_input_volume (float): The input volume for which to estimate the CPU requirements.
    
        Returns:
            float: The estimated CPU requirements.
    
        Raises:
            ValueError: If no dry run results are found for the given step and resource.
        """
        # Extract all StepDryRunResult objects from the dry runs
        all_dry_run_results = [result for dry_run in self.dry_runs for result in dry_run.step_dry_runs]
    
        # Filter the results for the given step and resource
        filtered_results = [result for result in all_dry_run_results if result.step == step and result.resource == resource]
    
        if not filtered_results:
            raise ValueError("No dry run results found for the given step and resource.")
    
        # Prepare the data for the Random Forest model
        input_volumes = []
        cpu_usages = []
        for result in filtered_results:
            if result.pipeline_input_volume > 0:
                input_volumes.append(result.pipeline_input_volume)
                # Use the average of avg_cpu_percentage and max_cpu_percentage
                avg_cpu_usage = (result.avg_cpu_percentage + result.max_cpu_percentage) / 2
                cpu_usages.append(avg_cpu_usage)
    
        if not input_volumes:
            raise ValueError("No valid dry run results with positive input volume found for the given step and resource.")
    
        # Convert lists to numpy arrays
        input_volumes = np.array(input_volumes).reshape(-1, 1)
        cpu_usages = np.array(cpu_usages)
    
        # Train the Random Forest model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(input_volumes, cpu_usages)
    
        # Estimate the CPU requirements for the given pipeline input volume
        estimated_cpu = model.predict(np.array([[pipeline_input_volume]]))[0]
    
        return max(estimated_cpu, 0.0)

    def estimate_hardware_requirements(self, step: PipelineStep, resource: SimpleComputingResource, pipeline_input_volume: float) -> StepHardwareRequirementsEstimation:
        """
        Estimates the hardware requirements for a given pipeline step.

        Args:
            step (PipelineStep): The pipeline step for which to estimate the hardware requirements.
            resource (SimpleComputingResource): The computing resource on which the step will be executed.
            input_volume (float): The input volume for the step.

        Returns:
            StepHardwareRequirementsEstimation: The estimated hardware requirements for the step.
        """
        try:
            estimated_ram = self.estimate_ram_requirements(step, resource, pipeline_input_volume)
            estimated_cpu = self.estimate_cpu_requirements(step, resource, pipeline_input_volume)
        except ValueError:
            return None
        
        return StepHardwareRequirementsEstimation(pipeline_input_volume, step, resource, estimated_cpu, estimated_ram)

