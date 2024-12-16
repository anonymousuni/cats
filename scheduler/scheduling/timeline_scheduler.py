from pipelines.pipeline import *
from resources.computing_resources import *
from dry_runs.dry_run import DryRun
from typing import List
from scheduling.estimations import ContextAwareStepTimelineEstimator, StepHardwareRequirementsEstimation, StepHardwareRequirementsEstimator
from scheduling.timeline import *
import copy
from itertools import permutations, product
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class ForcedDeployment:
    """
    Represents a forced deployment of a step on a specific computing resource.

    Attributes:
        step (PipelineStep): The step to be deployed.
        resource (SimpleComputingResource): The resource on which the step is to be deployed.
    """

    def __init__(self, step: PipelineStep, resource: SimpleComputingResource):
        """
        Initializes a new instance of the ForcedDeployment class.

        Args:
            step (PipelineStep): The step to be deployed.
            resource (SimpleComputingResource): The resource on which the step is to be deployed.
        """
        self.step = step
        self.resource = resource

class CandidateSchedule:
    def __init__(self, candidate_schedules_for_previous_levels: List['CandidateSchedule'], level: List[PipelineStep], network_graph: NetworkGraph, budget: float, deadline: float, step_timeline_estimations: List[StepTimelineEstimation], step_resource_requirements_estimations: List[StepHardwareRequirementsEstimation], pipeline: Pipeline, timeline: Timeline, forced_deployments: List[ForcedDeployment], maximum_scalability: int = None):
        """
        Initializes a new instance of the CandidateSchedule class.

        Args:
            candidate_schedules_for_previous_levels (List[CandidateSchedule]): The candidate schedules for the previous levels.
            level (List[PipelineStep]): The level of the candidate schedule.
            network_graph (NetworkGraph): The network graph associated with the candidate schedule.
            budget (float): The budget for the schedule.
            deadline (float): The deadline for the schedule.
            step_timeline_estimations (List[StepTimelineEstimation]): The list of step timeline estimations.
            step_resource_requirements_estimations (List[StepResourceRequirementsEstimation]): The list of step resource requirements estimations.
            pipeline (Pipeline): The pipeline that is being scheduled.
            forced_deployments (List[ForcedDeployment]): The list of forced deployments.
            maximum_scalability (int, optional): The maximum scalability for steps. Defaults to None.
        """
        self.candidate_schedules_for_previous_levels = candidate_schedules_for_previous_levels
        self.level = level
        self.network_graph = network_graph
        self.budget = budget
        self.deadline = deadline
        self.step_timeline_estimations = step_timeline_estimations
        self.step_resource_requirements_estimations = step_resource_requirements_estimations
        self.pipeline = pipeline
        self.timeline = timeline or Timeline()
        self.forced_deployments = forced_deployments
        self.duplicates_found = 0
        self.best_timelines: List[Timeline] = [] # Best timelines for the current level
        self.best_timeline_score = float('inf') # Best score for the current level
        self.maximum_scalability = maximum_scalability

    def get_best_timelines(self) -> List[Timeline]:
        # Create an initial list of candidate timelines based on previous levels
        initial_candidate_timelines = []
        if self.candidate_schedules_for_previous_levels:
            for candidate_schedule in self.candidate_schedules_for_previous_levels:
                initial_candidate_timelines.append(candidate_schedule.timeline)
        else:
            # If there are no candidate schedules from previous levels, create a new timeline as a baseline
            initial_candidate_timelines.append(self.timeline)

        # Create a copy of the level to keep track of the steps that have been scheduled
        level_copy = copy.copy(self.level)
        while len(level_copy):
            eligible_resources = self.network_graph.get_eligible_computing_resources()
            # Get a step to schedule that has all prerequisites ready
            ready_steps = self.get_steps_with_prerequisite_ready(level_copy)

            # Create a list to store futures
            futures = []
            self.best_timelines = []
            self.best_timeline_score = float('inf')
            # Iterate over each candidate timeline and calculate the timeline for each possible permutation of steps and resources
            with ThreadPoolExecutor(max_workers=12) as executor:
                for candidate_timeline in initial_candidate_timelines:
                    for steps_permutation in permutations(ready_steps, len(ready_steps)):
                        for resources_product in product(eligible_resources, repeat=len(ready_steps)):
                            # Convert resources_product to a list to allow modifications
                            resources_list = list(resources_product)
                            steps_list = list(steps_permutation)
                            resources_list_for_scaling_copy = []
                            steps_list_for_scaling_copy = []
                            scalable_steps: List[PipelineStep] = []

                            # Check if any step in steps_permutation is in the forced_deployments
                            for i, step in enumerate(steps_permutation):
                                for forced_deployment in self.forced_deployments:
                                    if step == forced_deployment.step:
                                        # Replace the resource in resources_list with the resource from the forced_deployment
                                        resources_list[i] = forced_deployment.resource

                                if self.pipeline.is_step_scalable(step):
                                    scalable_steps.append(step)

                            if len(scalable_steps) > 0:
                                # Make a copy of the timeline, steps_list, and resources_list for scaling
                                resources_list_for_scaling = copy.copy(resources_list)
                                steps_list_for_scaling = copy.copy(steps_list)
                                candidate_timeline_copy = copy.copy(candidate_timeline)

                            # Submit the timeline calculation to the executor
                            futures.append(executor.submit(self.calculate_timeline, candidate_timeline, steps_list, resources_list))

                            for scalable_step in scalable_steps:
                                # Get the total inputs of the scalable step
                                scalable_step_total_input_count = self.get_step_timeline_estimation(scalable_step, eligible_resources[0], eligible_resources[0]).timeline_estimation.number_of_transmitted_inputs

                                max_scalability = self.calculate_max_scalability(candidate_timeline_copy, scalable_step, resources_list_for_scaling[steps_list_for_scaling.index(scalable_step)])
                                
                                if self.maximum_scalability is not None:
                                    # Scalability has been disabled, leave the loop
                                    if max_scalability <=1:
                                        break
                                    # We have restricted the maximum scalability of the Candidate Schedule, overwrite the max_scalability value
                                    if self.maximum_scalability < max_scalability:
                                        max_scalability = self.maximum_scalability
                                for scale_amount in range(2, max_scalability):
                                    for scaled_resources_product in product(eligible_resources, repeat=(scale_amount - 1)):
                                        steps_list_for_scaling_copy = copy.copy(steps_list_for_scaling)
                                        resources_list_for_scaling_copy = copy.copy(resources_list_for_scaling)

                                        # Convert resources_product to a list to allow modifications
                                        scaled_resources_list = list(scaled_resources_product)

                                        # Add instances of scalable_step
                                        steps_list_for_scaling_copy.extend([scalable_step] * (scale_amount - 1))

                                        # Add the scaled resources to the resources_list for scaling
                                        resources_list_for_scaling_copy.extend(scaled_resources_list)

                                        # Submit the timeline calculation to the executor
                                        futures.append(executor.submit(self.calculate_timeline, candidate_timeline_copy, steps_list_for_scaling_copy, resources_list_for_scaling_copy, scalable_step, scale_amount, scaled_step_remaining_inputs=scalable_step_total_input_count, scaled_step_remaining_instances=scale_amount))


                # Process the results as they complete
                for future in as_completed(futures):
                    calculated_timeline = future.result()
                    if calculated_timeline is not None:
                        self.best_timelines.append(calculated_timeline)
                        best_timelines = self.get_best_scoring_candidate_timelines(self.best_timelines)
                        best_score = self.calculate_total_timeline_score(best_timelines[0])
                        self.best_timelines = best_timelines
                        self.best_timeline_score = best_score
                        # if calculated_timeline not in calculated_timelines:
                        # calculated_timelines.append(calculated_timeline)

            # Remove the scheduled steps from the level
            for step in ready_steps:
                level_copy.remove(step)

            initial_candidate_timelines = self.best_timelines

        # Set the value of the timelines attribute to the best candidate timelines from the ones that were calculated
        # best_scoring_timelines = self.get_best_scoring_candidate_timelines(initial_candidate_timelines)

        return self.best_timelines

    def get_best_scoring_candidate_timelines(self, candidate_timelines: List[Timeline]) -> List[Timeline]:
        """
        Gets the best candidate timelines from a list of candidate timelines.

        Args:
            candidate_timelines (List[Timeline]): The list of candidate timelines to choose from.

        Returns:
            List[Timeline]: The best candidate timelines from the list.
        """
        best_score = float('inf')
        best_candidate_timelines: List[Timeline] = []
        for candidate_timeline in candidate_timelines:
            # Determine which is the candidate timeline
            score = self.calculate_total_timeline_score(candidate_timeline)

            # If the score is lower than the current best score, update the best candidates
            if score < best_score:
                best_score = score
                best_candidate_timelines = [candidate_timeline]
            elif score == best_score:
                if candidate_timeline not in best_candidate_timelines:
                    best_candidate_timelines.append(candidate_timeline)
        return best_candidate_timelines

    def get_steps_with_prerequisite_ready(self, steps_list):
        """
        Identifies and returns the steps from the given list that have all their prerequisites met.

        Args:
            steps_list (list): A list of steps to check for readiness.

        Returns:
            list: A list of steps that have all their prerequisites met and are ready to be executed.

        The function works as follows:
        1. It initializes an empty list `ready_steps` to store the steps that are ready.
        2. It retrieves the dependencies of the pipeline from `self.pipeline.dependencies`.
        3. For each step in `steps_list`, it checks if there are any dependencies where the step is the dependent step.
        4. If a dependency is found, it checks if the prerequisite step of the dependency is also in `steps_list`.
        5. If the prerequisite step is found, it breaks the inner loop and continues to the next step.
        6. If no prerequisite step is found for any dependency, it adds the step to the `ready_steps` list.
        7. Finally, it returns the list of steps that are ready to be executed.

        Example:
            steps_list = [step1, step2, step3]
            ready_steps = get_steps_with_prerequisite_ready(steps_list)
        """
        ready_steps = []
        step_dependencies = self.pipeline.dependencies
        for step in steps_list:
            for dependency in step_dependencies:
                # Check if the dependent_step of the dependency is the same as the step
                if dependency.dependent_step.same_as(step):
                    # Iterate over each source_step in steps_list
                    for source_step in steps_list:
                        # Check if the prerequisite_step of the dependency is the same as the source_step
                        if dependency.prerequisite_step.same_as(source_step):
                            # If it is, break the loop
                            break
                    else:
                        # If the loop didn't break (i.e., no matching source_step was found), continue to the next dependency
                        continue
                    # If the loop did break, break the outer loop as well
                    break
            else:
                # If the outer loop didn't break (i.e., no incoming dependencies were found in steps_list), add the step to the list of ready steps
                ready_steps.append(step)
        # Return all steps that match the criteria
        return ready_steps

    def calculate_timeline_time_fraction(self, timeline: Timeline):
        return timeline.calculate_timeline_total_time() / self.deadline

    def calculate_timeline_cost_fraction(self, timeline: Timeline):
        total_cost = timeline.calculate_total_data_transmission_cost() + timeline.calculate_timeline_resource_cost()
        return total_cost / self.budget

    def calculate_total_timeline_score(self, timeline: Timeline) -> float:
        """
        Calculates the total score of the timeline.

        Args:
            timeline (Timeline): The timeline to calculate the score for.

        Returns:
            float: The calculated score for the timeline.
        """
        time_fraction = self.calculate_timeline_time_fraction(timeline)
        cost_fraction = self.calculate_timeline_cost_fraction(timeline)

        return time_fraction + cost_fraction

    def get_step_timeline_estimation(self, step: PipelineStep, resource: SimpleComputingResource, previous_resource: SimpleComputingResource) -> StepTimelineEstimation:
        """
        Gets the step timeline estimation for a given step and resource.

        Args:
            step (PipelineStep): The step to get the timeline estimation for.
            resource (SimpleComputingResource): The resource to get the timeline estimation for.
            previous_resource (SimpleComputingResource): The previous resource to get the timeline estimation for.

        Returns:
            StepTimelineEstimation: The timeline estimation for the specified step and resources.
        """
        # Initialize the result to None
        result = None

        # Iterate over each estimation in self.step_timeline_estimations
        for estimation in self.step_timeline_estimations:
            # Check if the pipeline_step of the estimation is the same as the step,
            # and if the resource and previous_resource of the estimation are the same as the input resource and previous_resource
            if estimation.pipeline_step.same_as(step) and estimation.resource == resource and estimation.previous_resource == previous_resource:
                # If they are, set the result to this estimation and break the loop
                result = estimation
                break

        # Return the result
        return result

        # return next((estimation for estimation in self.step_timeline_estimations if estimation.pipeline_step.same_as(step) and estimation.resource == resource and estimation.previous_resource == previous_resource), None)

    def get_step_hardware_requirements_estimation(self, step: PipelineStep, resource: SimpleComputingResource) -> StepHardwareRequirementsEstimation:
        """
        Gets the step hardware requirements estimation for a given step and resource.

        Args:
            step (PipelineStep): The step to get the hardware requirements estimation for.
            resource (SimpleComputingResource): The resource to get the hardware requirements estimation for.

        Returns:
            StepHardwareRequirementsEstimation: The hardware requirements estimation for the specified step and resource.
        """

        return next((estimation for estimation in self.step_resource_requirements_estimations if estimation.pipeline_step.same_as(step) and estimation.resource == resource), None)

    def calculate_max_scalability(self, timeline: Timeline, step: PipelineStep, resource: SimpleComputingResource) -> int:
        """
        Calculates the maximum scalability of a step.

        Args:
            timeline (Timeline): The timeline to consider.
            step (PipelineStep): The step to calculate the scalability for.
            resource (SimpleComputingResource): The resource to consider.

        Returns:
            int: The maximum scalability of the step.
        """
        # Find the step that is asynchronously dependent on the input step
        dependent_step = self.pipeline.get_asynchronously_dependent_step(step)
        if not dependent_step:
            return 1  # If no dependent step is found, the step is not scalable

        # Retrieve the resource of the dependent step
        dependent_step_resource = timeline.get_scheduled_resource_of_step(dependent_step)
        if not dependent_step_resource:
            # If the step has not been set to a resource, we set the dependent_step_resource to the worst performing resource
            dependent_step_resource = self.get_resource_of_worst_performing_timeline_estimation(dependent_step)


        # Retrieve the total time of the input step's timeline estimation
        step_timeline_estimation = self.get_step_timeline_estimation(step, resource, dependent_step_resource)
        if not step_timeline_estimation:
            raise ValueError("Missing timeline estimation for the input step")

        input_step_data_transfer_and_processing_time = step_timeline_estimation.timeline_estimation.get_step_processing_time() + step_timeline_estimation.timeline_estimation.get_data_transmission_time()

        # Retrieve the processing time of the last dependent step that is already in the timeline
        dependent_step_processing_time = timeline.get_all_scheduling_events_of_step(dependent_step)[-1].step_execution_timeline_estimation.timeline_estimation.get_step_processing_time()
        if dependent_step_processing_time == 0:
            raise ValueError("Dependent step is not scheduled in the timeline")

        # Calculate the scalability
        scalability = math.ceil(input_step_data_transfer_and_processing_time / dependent_step_processing_time)

        number_of_produced_outputs = dependent_step_processing_time = timeline.get_all_scheduling_events_of_step(dependent_step)[-1].step_execution_timeline_estimation.timeline_estimation.number_of_produced_outputs
        # No point to scale beyond the number of produced outputs by the producer step
        return scalability if scalability < number_of_produced_outputs else number_of_produced_outputs

    def calculate_timeline(self, current_timeline: Timeline, sequence_steps_to_schedule: List[PipelineStep], sequence_resources: List[SimpleComputingResource], scaled_step: PipelineStep = None, scale_amount: int = 1, scaled_step_remaining_inputs: int = 1, scaled_step_remaining_instances: int = 1, best_scaled_timeline_score: float = None) -> Timeline:
        """
        Calculates and returns the timeline for the candidate schedule.

        Args:
            current_timeline (Timeline): The current timeline to base the calculations on.
            sequence_steps_to_schedule (List[PipelineStep]): The sequence of steps to schedule.
            sequence_resources (List[SimpleComputingResource]): The sequence of resources to use for scheduling the steps.
            scaled_step (PipelineStep, optional): The step to be scaled. Defaults to None.
            scale_amount (int, optional): The amount by which the step should be scaled. Defaults to 1.
            scaled_step_remaining_inputs (int, optional): The remaining number of inputs for the scaled step. Defaults to 1.
            scaled_step_remaining_instances (int, optional): The remaining number of instances for the scaled step. Defaults to 1.

        Returns:
            Timeline: The calculated timeline for the candidate schedule.
        """

        if len(sequence_steps_to_schedule) == 0 or len(sequence_resources) == 0 or len(sequence_steps_to_schedule) != len(sequence_resources):
            raise Exception("Invalid input. Cannot calculate timeline.")

        step_to_schedule = sequence_steps_to_schedule[0]
        resource = sequence_resources[0]
        sequence_steps_to_schedule.remove(step_to_schedule)
        sequence_resources.remove(resource)

        if step_to_schedule is None:
            raise Exception("Missing starting step. Cannot calculate timeline.")

        # Create a copy of the timeline of using the resource
        candidate_timeline = copy.copy(current_timeline)

        # Find the estimated hardware requirements for the step on the resource and extract the CPU and memory requirements
        current_resource_hardware_estimation = self.get_step_hardware_requirements_estimation(step_to_schedule, resource)

        # If there are no hardware requirements estimations, skip the resource (we assume the step cannot be scheduled on it)
        if not current_resource_hardware_estimation:
            return None

        cpu_requirement = current_resource_hardware_estimation.cpu_reservation
        mem_requirement = current_resource_hardware_estimation.memory_reservation

        # Define a resource reservation for the step
        curr_step_reservation = ResourceReservation(resource, cpu_requirement, mem_requirement)

        # Get all steps that this depends on
        dependant_steps = self.pipeline.get_steps_with_incoming_dependency(step_to_schedule)
        if dependant_steps:
            # Get the last step that finishes processing data that will be used for synchronization
            latest_finishing_step = candidate_timeline.get_step_with_latest_finish_time(dependant_steps)

            if not latest_finishing_step:
                raise("Dependant step not scheduled previously. Cannot calculate timeline.")

            # Calculate the synchronization position of the latest finishing step
            sync_position = candidate_timeline.get_step_synchronization_position(latest_finishing_step)

            # Get the resource where the latest finishing step is scheduled
            latest_finishing_step_resource = candidate_timeline.get_scheduled_resource_of_step(latest_finishing_step)

            # Get the timeline estimation for the current step on the resource
            current_step_timeline_estimation = self.get_step_timeline_estimation(step_to_schedule, resource, latest_finishing_step_resource)

            if scale_amount > 1 and step_to_schedule == scaled_step:
                # Create a new timeline estimation for the scaled step based on the scale amount
                current_step_timeline_estimation_copy = copy.deepcopy(current_step_timeline_estimation)

                # The current scale level equals the scheduled events of the step plus the scaling we are currently doing
                scale_level = len(candidate_timeline.get_all_scheduling_events_of_step(step_to_schedule)) + 1

                # Calculate the synchronization position of the latest finishing step
                sync_position = candidate_timeline.get_step_synchronization_position(latest_finishing_step, scale_current_level=scale_level)

                # Check if the current_step_timeline_estimation_copy is of type ConsumerStepExecutionTimeline
                if isinstance(current_step_timeline_estimation_copy.timeline_estimation, ConsumerStepExecutionTimeline):
                    # Calculate how many inputs will be processed in the scaled step
                    if scaled_step_remaining_instances == 0:
                        inputs_to_process = scaled_step_remaining_inputs
                    else:
                        inputs_to_process = math.ceil(scaled_step_remaining_inputs / scaled_step_remaining_instances)
                        scaled_step_remaining_instances -= 1
                    current_step_timeline_estimation_copy.timeline_estimation.number_of_transmitted_inputs = inputs_to_process
                    current_step_timeline_estimation_copy.timeline_estimation.number_of_produced_outputs = inputs_to_process
                    scaled_step_remaining_inputs -= inputs_to_process

                current_step_timeline_estimation = current_step_timeline_estimation_copy

            if not current_step_timeline_estimation:
                raise Exception("Missing timeline estimation. Cannot calculate timeline. Step:" + step_to_schedule.name + " Resource:" + resource)

            # Get the provisioning and deployment time of the current step
            current_step_provisioning_and_deployment_time = current_step_timeline_estimation.timeline_estimation.get_provisioning_and_deployment_time()

            # Earliest start equals finish time of the latest finishing step minus the provisioning and deployment time of the step
            optimal_earliest_start_position = sync_position - current_step_provisioning_and_deployment_time
        else:
            # There are no dependants

            # Get the timeline estimation for the current step on the resource; no previous resource since there are no dependants
            current_step_timeline_estimation = self.get_step_timeline_estimation(step_to_schedule, resource, resource)
            if not current_step_timeline_estimation:
                raise Exception("Missing timeline estimation. Cannot calculate timeline. Step:" + step_to_schedule.name + " Resource:" + resource)

            # The optimal earliest start position is always 0 since there are no dependants
            optimal_earliest_start_position = 0

        estimated_total_step_time = current_step_timeline_estimation.timeline_estimation.get_total_time()

        current_step_schedule_position = candidate_timeline.get_earliest_available_resource_position_after(position=optimal_earliest_start_position, duration=estimated_total_step_time, resource=resource, required_memory_reservation=mem_requirement, required_cpu_reservation=cpu_requirement)

        step_scheduling_event = SchedulingEvent(current_step_schedule_position, curr_step_reservation, current_step_timeline_estimation)

        candidate_timeline.add_event(step_scheduling_event)

        # If the current score is higher than the best score while calculating timelines, we can skip the rest of scheduling
        current_score = self.calculate_total_timeline_score(candidate_timeline)
        if current_score > self.best_timeline_score:
            return None
        # If the score of the current level of scaling is lower than the best score, we can skip the rest of the scaling
        if step_to_schedule == scaled_step:
            # We set the non-scaled pipeline as best score initially
            if best_scaled_timeline_score is None:
                # Replace the timeline estimation to perform the calculation
                no_scaling_step_timeline_estimation = self.get_step_timeline_estimation(step_to_schedule, resource, latest_finishing_step_resource)
                no_scaling_scheduling_event = SchedulingEvent(current_step_schedule_position, curr_step_reservation, no_scaling_step_timeline_estimation)
                candidate_timeline.replace_event(step_scheduling_event, no_scaling_scheduling_event)
                best_scaled_timeline_score = self.calculate_total_timeline_score(candidate_timeline)
                candidate_timeline.replace_event(no_scaling_scheduling_event, step_scheduling_event)

            if current_score > best_scaled_timeline_score:
                return None
            else:
                if len(sequence_steps_to_schedule) > 0:
                    return self.calculate_timeline(candidate_timeline, sequence_steps_to_schedule, sequence_resources, scaled_step, scale_amount, scaled_step_remaining_inputs, scaled_step_remaining_instances, best_scaled_timeline_score)
                else:
                    return candidate_timeline

        if(len(sequence_steps_to_schedule) > 0):
            return self.calculate_timeline(candidate_timeline, sequence_steps_to_schedule, sequence_resources, scaled_step, scale_amount, scaled_step_remaining_inputs, scaled_step_remaining_instances, best_scaled_timeline_score)

        return candidate_timeline

    def get_resource_of_worst_performing_timeline_estimation(self, step: PipelineStep) -> SimpleComputingResource:
        """
        Retrieves the resource of the timeline estimation for an input step with the biggest value of 'average_time_to_produce_output'.

        Args:
            step (PipelineStep): The step to find the worst performing timeline estimation for.

        Returns:
            SimpleComputingResource: The resource of the worst performing timeline estimation.
        """
        worst_performing_resource = None
        max_average_time_to_produce_output = float('-inf')

        for estimation in self.step_timeline_estimations:
            if estimation.pipeline_step.same_as(step):
                if not isinstance(estimation.timeline_estimation, ProducerStepExecutionTimeline):
                    raise ValueError("Timeline estimation is not of type ConsumerStepExecutionTimeline")

                average_time_to_produce_output = estimation.timeline_estimation.average_time_to_produce_output
                if average_time_to_produce_output > max_average_time_to_produce_output:
                    max_average_time_to_produce_output = average_time_to_produce_output
                    worst_performing_resource = estimation.resource

        return worst_performing_resource

# CATS = Context Aware Timeline Scheduler
class TimelineScheduler:
    """
    Represents a scheduler for resources for a pipeline.

    Attributes:
        pipeline (Pipeline): The pipeline to be scheduled.
        network_graph (NetworkGraph): The network graph of resources that will be scheduled on.
        dry_runs (List[DryRun]): A list of DryRun instances used as a basis for the produced schedule.
        timeline (Timeline): The timeline of scheduling events produced by the scheduler.
        deadline (float): The deadline for the schedule in seconds.
        budget (float): The budget for the schedule in USD.
        input_volume (float): The input volume of the pipeline being scheduled in MB.
        forced_deployments (List[ForcedDeployment]): A list of forced deployments.
        max_scalability (Optional[int]): The maximum scalability for steps. Defaults to None.
    """

    def __init__(self, pipeline: Pipeline, network_graph: NetworkGraph, dry_runs: List[DryRun], deadline: float, budget: float, input_volume: float, forced_deployments: List[ForcedDeployment] = None, max_scalability: int = None):
        """
        Initializes a new instance of the TimelineScheduler class.

        Args:
            pipeline (Pipeline): The pipeline to be scheduled.
            network_graph (NetworkGraph): The network graph of resources that will be scheduled on.
            dry_runs (List[DryRun]): A list of DryRun instances used as a basis for the produced schedule.
            deadline (float): The deadline for the schedule.
            budget (float): The budget for the schedule.
            input_volume (float): The input volume of the pipeline being scheduled.
            forced_deployments (List[ForcedDeployment], optional): A list of forced deployments. Defaults to an empty list.
            max_scalability (int, optional): The maximum scalability for steps. Defaults to None.
        """
        self.pipeline = pipeline
        self.network_graph = network_graph
        self.dry_runs = dry_runs
        self.timelines: List[Timeline] = []
        self.deadline = deadline
        self.budget = budget
        self.input_volume = input_volume
        self.scheduled_steps = []
        self.forced_deployments = forced_deployments if forced_deployments is not None else []
        self.max_scalability = max_scalability

        # Get all computing resources from the network graph
        resources = network_graph.get_all_computing_resources()

        # Initialize a list to store all unique step timeline estimations
        self.all_step_timeline_estimations = []
        self.all_step_resource_requirements_estimations = []

        # Initialize resource requirements estimator
        resource_requirements_estimator = StepHardwareRequirementsEstimator(dry_runs=dry_runs)

        # Iterate over each pair of resources
        for step in pipeline.steps:
            for resource1 in resources:
                for resource2 in resources:
                    # Create a ContextAwareStepTimelineEstimator instance using the dry runs
                    timeline_estimator = ContextAwareStepTimelineEstimator(dry_runs=dry_runs, current_resource=resource1, previous_resource=resource2, network_graph=network_graph)
                    try:
                        # Estimate the timeline for steps using the current resource and input volume
                        timeline_estimation = timeline_estimator.estimate_timeline(step, resource1, input_volume)
                        if timeline_estimation is not None and timeline_estimation not in self.all_step_timeline_estimations:
                            self.all_step_timeline_estimations.append(timeline_estimation)

                            resource_estimation = resource_requirements_estimator.estimate_hardware_requirements(step, resource1, input_volume)

                            if(resource_estimation is not None and resource_estimation not in self.all_step_resource_requirements_estimations):
                                self.all_step_resource_requirements_estimations.append(resource_estimation)

                    except ValueError:
                        # If an error occurs during estimation
                        # (either a step does not have a dry run on a resource or the step is of unknown type),
                        # skip to the next resource
                        continue

    def schedule(self):
        """
        Schedules the pipeline on the network graph using the timeline scheduler.

        This function iterates over the steps in the pipeline and the resources in the network graph.
        For each pair of steps and resources, it creates a ContextAwareStepTimelineEstimator instance and uses it to estimate the timeline for the step using the resource. It then adds the estimated timeline to the timeline of the scheduler.

        Returns:
            Timeline: The timeline of scheduling events produced by the scheduler.
        """
        levels = self.pipeline.split_into_levels()
        candidate_schedules_for_previous_levels = []
        timelines_from_schedule: List[Timeline] = []
        start_time = time.time()
        for i, level in enumerate(levels):
            #  Create a new schedule for the current level
            new_candidate_schedule = CandidateSchedule(candidate_schedules_for_previous_levels=candidate_schedules_for_previous_levels, level=level, network_graph=self.network_graph, budget=self.budget, deadline=self.deadline, step_timeline_estimations=self.all_step_timeline_estimations, step_resource_requirements_estimations=self.all_step_resource_requirements_estimations, pipeline=self.pipeline, timeline=Timeline(), forced_deployments=self.forced_deployments, maximum_scalability=self.max_scalability)

            # Get the best timelines for the current level
            timelines_from_schedule = new_candidate_schedule.get_best_timelines()

            # Create candidate schedules for the next levels
            candidate_schedules_for_previous_levels: List[CandidateSchedule] = []
            for timeline in timelines_from_schedule:
                candidate_schedules_for_previous_levels.append(CandidateSchedule(candidate_schedules_for_previous_levels=[], level=level, network_graph=self.network_graph, budget=self.budget, deadline=self.deadline, step_timeline_estimations=self.all_step_timeline_estimations, step_resource_requirements_estimations=self.all_step_resource_requirements_estimations, pipeline=self.pipeline, timeline=timeline, forced_deployments=self.forced_deployments, maximum_scalability=self.max_scalability))

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Elapsed time: {elapsed_time:.2f} seconds")

        # Deduplicate the timelines_from_schedule list
        unique_timelines: List[Timeline] = []

        for timeline in timelines_from_schedule:
            if timeline not in unique_timelines:
                unique_timelines.append(timeline)

        self.timelines = unique_timelines

        return self.timelines
