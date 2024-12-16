from typing import List
from pipelines.pipeline import PipelineStep, ProducerStep, ProducerStepExecutionTimeline
from resources.computing_resources import *
from scheduling.estimations import StepTimelineEstimation
import copy
import matplotlib.pyplot as plt
import csv

class ResourceReservation:
    """
    Represents a reservation of resources on a computing resource.

    Attributes:
        resource (SimpleComputingResource): The computing resource on which the reservation is made.
        reserved_cpu (float): The amount of CPU reserved.
        reserved_memory (float): The amount of memory reserved.
    """

    def __init__(self, resource: SimpleComputingResource, reserved_cpu: float, reserved_memory: float):
        if not isinstance(resource, SimpleComputingResource):
            raise ValueError("Resource must be an instance of ComputingResource or its subclass")
        if reserved_cpu < 0:
            reserved_cpu = 0
            # raise ValueError("Reserved CPU must be non-negative")
        if reserved_memory < 0:
            reserved_memory = 0
            # raise ValueError("Reserved memory must be non-negative")
        
        self.resource = resource
        self.reserved_cpu = reserved_cpu
        self.reserved_memory = reserved_memory

    def __eq__(self, other):
        if not isinstance(other, ResourceReservation):
            return False
        return (self.resource == other.resource and
                self.reserved_cpu == other.reserved_cpu and
                self.reserved_memory == other.reserved_memory)

class SchedulingEvent:
    """
    Represents an event on a timeline.

    Attributes:
        name (str): The name of the event.
        position (int): The position of the event on the timeline.
        resource_reservation (ResourceReservation): The resource reservation associated with the event.
        step_execution_timeline (StepExecutionTimeline): The step execution timeline associated with the event.
    """

    def __init__(self, position: int, resource_reservation: ResourceReservation, step_execution_timeline_estimation: StepTimelineEstimation):
        """
        Initializes a new instance of the SchedulingEvent class.

        Args:
            position (int): The position of the event on the timeline.
            resource_reservation (ResourceReservation): The resource reservation associated with the event.
            step_execution_timeline_estimation (StepTimelineEstimation): The step execution timeline estimation associated with the event.
        """
        self.position = position
        self.resource_reservation = resource_reservation
        self.step_execution_timeline_estimation = step_execution_timeline_estimation
    
    def __eq__(self, other):
        if not isinstance(other, SchedulingEvent):
            return False
        return (self.position == other.position and
                self.resource_reservation == other.resource_reservation and
                self.step_execution_timeline_estimation == other.step_execution_timeline_estimation)

    def is_active_within(self, start_position: float, end_position: float) -> bool:
        """
        Returns True if the event is active within the provided start and end positions.

        Args:
            start_position (float): The start position to check.
            end_position (float): The end position to check.

        Returns:
            bool: True if the event is active within the start and end positions, False otherwise.
        """
        event_start = self.position
        event_end = self.position + self.step_execution_timeline_estimation.timeline_estimation.get_total_time()
        return event_start < end_position and event_end > start_position

    def is_active_at_position(self, position: float) -> bool:
        """
        Returns True if the event is active at the provided position.

        Args:
            position (float): The position to check.

        Returns:
            bool: True if the event is active at the position, False otherwise.
        """
        finish_time = self.position + self.step_execution_timeline_estimation.timeline_estimation.get_total_time()
        return self.position <= position < finish_time
    
    def __repr__(self):
        """Returns a string representation of the SchedulingEvent object."""
        return f"Event(position={self.position}, resource_reservation={self.resource_reservation}, step_execution_timeline={self.step_execution_timeline_estimation.timeline_estimation})"

class Timeline:
    """
    Represents a timeline of events.

    Attributes:
        events (List[SchedulingEvent]): A list of scheduling events.
    """

    def __init__(self):
        """Initializes a new instance of the Timeline class."""
        self.events: List[SchedulingEvent] = []
    
    def __eq__(self, other):
        if not isinstance(other, Timeline):
            return False
        return self.events == other.events

    def __copy__(self):
        """
        Creates a shallow copy of the Timeline instance.

        Returns:
            Timeline: A shallow copy of the Timeline instance.
        """
        new_timeline = Timeline()
        new_timeline.events = []
        for event in self.events:
            new_timeline.events.append(copy.copy(event))
        return new_timeline

    def sort_events(self):
        """Sorts the events by position and then by the name of the pipeline step if positions are the same."""
        self.events.sort(key=lambda event: (event.position, event.step_execution_timeline_estimation.pipeline_step.name))

    def get_earliest_available_resource_position_after(self, position: float, duration: float, resource: SimpleComputingResource, required_memory_reservation: float, required_cpu_reservation: float) -> Optional[float]:
        """
        Returns the earliest position after the provided position where the provided resource has enough memory and CPU available.

        Args:
            position (float): The position to start looking from.
            duration (float): The duration for which the resource is needed.
            resource (SimpleComputingResource): The resource to find the position for.
            required_memory_reservation (float): The required memory reservation.
            required_cpu_reservation (float): The required CPU reservation.

        Returns:
            Optional[float]: The earliest position where the resource has enough memory and CPU available. Returns None if no such position is found.
        """
        
        all_scheduling_events_of_resource = self.get_all_scheduling_events_of_resource(resource)
        if not all_scheduling_events_of_resource:
            return position
        
        positions_to_check = [position]
        # We need to check all start and end positions of all events that are active within the input position and the position plus the duration of the event
        for event in all_scheduling_events_of_resource:
            if event.is_active_within(position, position + duration):
                event_end = event.position + event.step_execution_timeline_estimation.timeline_estimation.get_total_time()
                if event.position not in positions_to_check and position < event.position < position + duration:
                    positions_to_check.append(event.position)

                if event_end not in positions_to_check and position < event_end:
                    positions_to_check.append(event_end)

        # Sort the positions to check in ascending order
        positions_to_check = sorted(positions_to_check)

        # If there is only one position to check it means it is only the input position, return it
        if len(positions_to_check) == 1:
            return position

        # Check if the resource has enough memory and CPU available at each position between position_candidate and position_candidate + duration
        # and return the first position where it does

        for position_candidate in positions_to_check:
            # Reset the available/reserved memory and CPU
            available_ram = resource.ram_capacity * 1024
            available_cpu = resource.num_cpus * 100
            reserved_memory = 0
            reserved_cpu = 0

            # At each point between the two positions, get all events that are active
            # Aggregate the reserved memory and CPU for each event active at the current position
            for event in all_scheduling_events_of_resource:
                if event.is_active_within(position_candidate, position_candidate + duration):
                    reserved_memory += event.resource_reservation.reserved_memory
                    reserved_cpu += event.resource_reservation.reserved_cpu

            # If at any point the resource does not have enough memory and CPU available, move to the next point    
            if available_ram - reserved_memory < required_memory_reservation or available_cpu - reserved_cpu < required_cpu_reservation:
                continue
            else:
                # The resource has enough memory and CPU available, return the position
                return position_candidate

        # In case we find no position to schedule, return the last position to check (that would correspond to the end of the latest ending scheduling event of the resource). It will always be possible to schedule the step at this position as the resource will be always free after that. 
        return positions_to_check[-1]

    def get_scheduled_resource_of_step(self, step: PipelineStep) -> Optional[SimpleComputingResource]:
        """
        Returns the resource that has been scheduled to be used by the provided step and is latest in the timeline.

        Args:
            step (PipelineStep): The step to find the resource for.

        Returns:
            SimpleComputingResource: The resource of the step. Returns None if no such step is found.
        """
        latest_resource = None
        for event in self.events:
            if event.step_execution_timeline_estimation.pipeline_step == step:
                latest_resource = event.resource_reservation.resource
        return latest_resource

    def get_step_end_position(self, step: PipelineStep) -> float:
        """
        Returns the end position of the provided step.

        Args:
            step (PipelineStep): The step to find the end position for.

        Returns:
            float: The end position of the step. Returns 0 if no such step is found.
        """
        end_position = 0
        for event in self.events:
            if event.step_execution_timeline_estimation.pipeline_step == step:
                finish_time = event.position + event.step_execution_timeline_estimation.timeline_estimation.get_total_time()
                if finish_time > end_position:
                    end_position = finish_time
        return end_position
    
    def get_step_synchronization_position(self, step: PipelineStep, scale_current_level: int = 1) -> float:
        """
        Returns the synchronization position of the provided step.

        Args:
            step (PipelineStep): The step to find the end position for.

        Returns:
            float: The end position of the step. Returns 0 if no such step is found.
        """
        if isinstance(step, ProducerStep):
            end_position = 0
            for event in self.events:
                if event.step_execution_timeline_estimation.pipeline_step == step:
                    producer_timeline_estimation = event.step_execution_timeline_estimation.timeline_estimation
                    if isinstance(producer_timeline_estimation, ProducerStepExecutionTimeline):
                        # We synchronize each instance of the scaled process by adding the provisioning and deployment time, data transmission time 
                        # and processing time of the producer step that emits the inputs multiplied by the current level of scaling
                        synchronization_time = event.position + producer_timeline_estimation.get_provisioning_and_deployment_time() + producer_timeline_estimation.get_data_transmission_time() + scale_current_level * producer_timeline_estimation.average_time_to_produce_output
                        if synchronization_time > end_position:
                            end_position = synchronization_time
                    else:
                        raise ValueError("Unexpected type of estimated timeline")
                    
            return end_position
        else:
            return self.get_step_end_position(step) 

    def get_step_with_latest_finish_time(self, steps: List[PipelineStep]) -> Optional[PipelineStep]:
        """
        Returns the step with the latest finish time among the provided steps.

        Args:
            steps (List[PipelineStep]): The list of steps to consider.

        Returns:
            PipelineStep: The step with the latest finish time. Returns None if no such step is found.
        """
        latest_finish_time = 0
        latest_step = None
        for event in self.events:
            for step in steps:
                if event.step_execution_timeline_estimation.pipeline_step.same_as(step):
                    finish_time = event.position + event.step_execution_timeline_estimation.timeline_estimation.get_total_time()
                    if finish_time >= latest_finish_time:
                        latest_finish_time = finish_time
                        latest_step = event.step_execution_timeline_estimation.pipeline_step
        return latest_step

    def get_all_scheduling_events_of_step(self, step: PipelineStep) -> List['SchedulingEvent']:
        """
        Retrieves all SchedulingEvents related to a specific PipelineStep.

        Args:
            step (PipelineStep): The step to find events for.

        Returns:
            List[SchedulingEvent]: A list of SchedulingEvents related to the specified step.
        """
        # Initialize an empty list to store the matching events
        matching_events = []

        # Iterate through all events in the timeline
        for event in self.events:
            # Check if the event's pipeline step matches the specified step
            if event.step_execution_timeline_estimation.pipeline_step.same_as(step):
                # If it matches, add the event to the list of matching events
                matching_events.append(event)

        # Return the list of matching events
        return matching_events
    
    def get_all_scheduling_events_of_resource(self, resource: 'SimpleComputingResource') -> List['SchedulingEvent']:
        """
        Retrieves all SchedulingEvents related to a specific SimpleComputingResource.

        Args:
            resource (SimpleComputingResource): The resource to find events for.

        Returns:
            List[SchedulingEvent]: A list of SchedulingEvents related to the specified resource.
        """
        return [event for event in self.events if event.resource_reservation.resource == resource]

    def calculate_total_data_transmission_cost(self):
        """
        Calculates the total data transmission cost for the timeline.

        Returns:
            float: The total data transmission cost.
        """
        total_cost = 0
        for event in self.events:
            estimation = event.step_execution_timeline_estimation
            resource = estimation.resource
            previous_resource = estimation.previous_resource
            data_amount = estimation.input_volume

            if previous_resource is not None:
                total_cost += previous_resource.calculate_price_to_transmit_data(resource, data_amount)
                total_cost += resource.calculate_price_to_receive_data(previous_resource, data_amount)

        return total_cost
    
    def calculate_timeline_resource_cost(self):
        # Reset reservation time for all resources of the timeline
        for event in self.events:
            resource = event.resource_reservation.resource
            if issubclass(type(resource), OnDemandInstance):
                resource.reservation_time_in_hours = 0
                resource.total_price = 0

        # Add reservation time for all resources of the timeline
        for event in self.events:
            resource = event.resource_reservation.resource
            if issubclass(type(resource), OnDemandInstance):
                resource.add_reservation_time(event.step_execution_timeline_estimation.timeline_estimation.get_total_time())

        total_cost = 0
        processed_resources = set()
        for event in self.events:
            resource = event.resource_reservation.resource
            if resource not in processed_resources:
                total_cost += resource.calculate_total_price()
                processed_resources.add(resource)
        
        # Reset reservation time and total price for all processed resources
        for resource in processed_resources:
            if issubclass(type(resource), OnDemandInstance):
                resource.reservation_time_in_hours = 0
                resource.total_price = 0

        return total_cost

    def calculate_timeline_total_time(self):
        """
        Calculates the total time of the timeline.

        Returns:
            float: The total time.
        """
        total_time = 0
        for event in self.events:
            time_of_end = event.position + event.step_execution_timeline_estimation.timeline_estimation.get_total_time()
            if time_of_end > total_time:
                total_time = time_of_end
        return total_time

    def add_event(self, new_event: SchedulingEvent):
        """
        Adds a new event to the timeline.

        Args:
            new_event (SchedulingEvent): The new event to be added to the timeline.
        """

        index = 0
        for i, event in enumerate(self.events):
            if new_event.position > event.position:
                index = i + 1
            else:
                break

        self.events.insert(index, new_event)
        if new_event.position < 0:
            self.shift_timeline(-new_event.position)
        self.sort_events()

    def remove_event(self, event: SchedulingEvent):
        """
        Removes an event from the timeline.

        Args:
            event (SchedulingEvent): The event to be removed from the timeline.
        """
        if event in self.events:
            self.events.remove(event)
            self.sort_events()

    def replace_event(self, old_event: SchedulingEvent, new_event: SchedulingEvent):
        """
        Replaces an existing event with a new event in the timeline.

        Args:
            old_event (SchedulingEvent): The event to be replaced.
            new_event (SchedulingEvent): The new event to replace the old event.
        """
        if old_event in self.events:
            index = self.events.index(old_event)
            self.events[index] = new_event
            self.sort_events()

    def shift_timeline(self, shift_amount: float):
        """
        Shifts the timeline by the specified amount.

        Args:
            shift_amount (float): The amount to shift the timeline by.
        """
        for event in self.events:
            event.position += shift_amount

    def time_spans(self):
        """
        Returns a list of time spans between the first event and each subsequent event.

        Returns:
            list: A list of time spans.
        """
        return [event.position - self.events[0].position for event in self.events]

    def get_events_within_timespan(self, event_name, timespan):
        """
        Returns a list of events that occur within a certain timespan after a specified event.

        Args:
            event_name (str): The name of the event.
            timespan (int): The timespan.

        Returns:
            list: A list of events within the timespan.

        Raises:
            ValueError: If no event with the specified name is found.
        """
        start_event = None
        for event in self.events:
            if event.name == event_name:
                start_event = event
                break

        if start_event is None:
            raise ValueError(f"No event with name '{event_name}' found.")

        within_timespan = []
        for event in self.events:
            if start_event.position < event.position <= start_event.position + timespan:
                within_timespan.append(event)

        return within_timespan

    @staticmethod
    def merge(timeline1: 'Timeline', timeline2: 'Timeline') -> 'Timeline':
        """
        Merges two timelines into one.

        Args:
            timeline1 (Timeline): The first timeline.
            timeline2 (Timeline): The second timeline.

        Returns:
            Timeline: The merged timeline.
        """
        merged_timeline = Timeline()
        merged_timeline.events = sorted(timeline1.events + timeline2.events, key=lambda event: event.position)
        return merged_timeline
    
    def __repr__(self):
        """Returns a string representation of the Timeline object."""
        return f"Timeline(events={self.events})"

    def display_timeline(self):
        """
        Displays a timeline of events as horizontal bars.

        Args:
            timeline (Timeline): The timeline to display.
        """
        # Sort the events by their start position
        self.events.sort(key=lambda event: event.position)

        # Create a figure and a subplot
        fig, ax = plt.subplots()

        # List to store the step names
        step_names = []

        # For each event in the timeline
        for i, event in enumerate(self.events):
            # Calculate the start and end positions of the event
            start_position = event.position
            timeline_estimation = event.step_execution_timeline_estimation.timeline_estimation

            # Calculate the times for each component
            provisioning_and_deployment_time = timeline_estimation.get_provisioning_and_deployment_time()
            data_transmission_time = timeline_estimation.get_data_transmission_time()
            step_processing_time = timeline_estimation.get_step_processing_time()

            # Draw a horizontal bar for each component
            ax.broken_barh([(start_position, provisioning_and_deployment_time)], (i-0.4, 0.8), facecolors='blue', label='Provisioning and Deployment Time' if i == 0 else "")
            ax.broken_barh([(start_position + provisioning_and_deployment_time, data_transmission_time)], (i-0.4, 0.8), facecolors='red', label='Data Transmission Time' if i == 0 else "")
            ax.broken_barh([(start_position + provisioning_and_deployment_time + data_transmission_time, step_processing_time)], (i-0.4, 0.8), facecolors='green', label='Step Processing Time' if i == 0 else "")

            # Add a text label for the event
            ax.text(start_position, i, f'{i+1}: {event.step_execution_timeline_estimation.pipeline_step.name}; Resource {event.step_execution_timeline_estimation.resource.name} - CPU:{event.step_execution_timeline_estimation.resource.num_cpus}/RAM:{event.step_execution_timeline_estimation.resource.ram_capacity}; CPU_res: {event.resource_reservation.reserved_cpu}, mem_res: {event.resource_reservation.reserved_memory}, t: {math.ceil(event.step_execution_timeline_estimation.timeline_estimation.get_total_time())}', color='black', va='center')

            # Add the step name to the list
            step_names.append(event.step_execution_timeline_estimation.pipeline_step.name)
        
        
        # Set the y-axis limits
        ax.set_ylim(-0.5, len(self.events) - 0.5)

        # Set the y-axis labels
        ax.set_yticks(range(len(self.events)))
        ax.set_yticklabels(step_names)

        # Set the x-axis label
        ax.set_xlabel('Time')

        total_time = self.calculate_timeline_total_time()
        total_cost = self.calculate_timeline_resource_cost() + self.calculate_total_data_transmission_cost()

        # Set the title of the plot
        ax.set_title(f'Total execution cost: {total_cost}, Total time of execution: {total_time}')

        # Add a legend at the bottom right
        ax.legend(loc='lower right')

        # Maximize the window
        mng = plt.get_current_fig_manager()
        mng.window.state('zoomed')

        # Show the plot
        plt.show()

    def serialize_to_csv(self, file_path: str):
        """
        Serializes the timeline to a CSV file.

        Args:
            file_path (str): The path to the CSV file.
        """
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the header
            writer.writerow(['Step Name', 'Start Position', 'End Position', 'Resource Name', 'Reserved CPU', 'Reserved Memory'])

            # Write the events
            for event in self.events:
                step_name = event.step_execution_timeline_estimation.pipeline_step.name
                start_position = event.position
                end_position = start_position + event.step_execution_timeline_estimation.timeline_estimation.get_total_time()
                resource_name = event.resource_reservation.resource.name
                reserved_cpu = event.resource_reservation.reserved_cpu
                reserved_memory = event.resource_reservation.reserved_memory

                writer.writerow([step_name, start_position, end_position, resource_name, reserved_cpu, reserved_memory])