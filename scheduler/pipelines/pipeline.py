from typing import List, Set
import networkx as nx
import matplotlib.pyplot as plt
from collections import deque

class PipelineStep:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"PipelineStep(name='{self.name}')"
    
    def same_as(self, other):
        """
        Check if the current instance of a Pipeline Step is the same as another instance. Use when comparing to copies of the same instance.

        Parameters:
            other (object): The other instance to compare with.

        Returns:
            bool: True if the instances are the same, False otherwise.
        """
        if isinstance(other, self.__class__):
            return self.name == other.name
        return False
    
class DataSource(PipelineStep):
    """A class representing a data source in a data processing pipeline.
    Data sources do not perform processing or produce outputs.
    """
    pass

class DataSink(PipelineStep):
    """A class representing a data sink in a data processing pipeline.
    Data sinks only perform data transmission from the previous step and do not produce outputs.
    """
    pass

class DataProcessing(PipelineStep):
    pass

class BatchStep(DataProcessing):
    """A class representing a batch step in a data processing pipeline.
    This step involves processing a batch of data at once and creating a single output.
    Inherits from the DataProcessing class.
    """
    def __init__(self, name):
        super().__init__(name)

class ConsumerStep(DataProcessing):
    """A class representing a consumer step in a data processing pipeline.
    This step involves consuming multiple inputs that are received from a ProducerStep and producing multiple outputs as a result.
    Inherits from the DataProcessing class.
    """
    def __init__(self, name):
        super().__init__(name)

class ProducerStep(DataProcessing):
    """A class representing a producer step in a data processing pipeline.
    This step involves producing or generating multiple outputs to be processed by a ConsumerStep.
    Inherits from the DataProcessing class.
    """
    def __init__(self, name):
        super().__init__(name)

# TODO Producer AND Consumer step

class StepExecutionTimeline:
    """
    Represents the timeline for executing a step in a pipeline.

    Attributes:
        provisioning_and_deployment_time (float): The time (in seconds) required to provision and deploy resources for the step.
        data_transmission_time (float): The time (in seconds) required to transmit data to the step.
        step_processing_time (float): The time (in seconds) required for the step to process the data.

    Methods:
        get_provisioning_and_deployment_time() -> float:
            Returns the time (in seconds) required to provision and deploy resources for the step.

        get_data_transmission_time() -> float:
            Returns the time (in seconds) required to transmit data to the step.

        get_step_processing_time() -> float:
            Returns the time (in seconds) required for the step to process the data.

        get_total_time() -> float:
            Returns the total time (in seconds) required for the step, which is the sum of the provisioning and deployment time, the data transmission time, and the step processing time.

        get_time_to_first_result() -> float:
            Returns the time (in seconds) to the first result, which is the sum of the provisioning and deployment time, the data transmission time, and the step processing time.
    """
    
    def __init__(self, provisioning_and_deployment_time: float, data_transmission_time: float, step_processing_time: float):
        if provisioning_and_deployment_time < 0:
            raise ValueError("Provisioning and deployment time must be non-negative")
        if data_transmission_time < 0:
            raise ValueError("Data transmission time must be non-negative")
        if step_processing_time < 0:
            raise ValueError("Step processing time must be non-negative")
        
        self.provisioning_and_deployment_time = provisioning_and_deployment_time
        self.data_transmission_time = data_transmission_time
        self.step_processing_time = step_processing_time

    def __repr__(self):
        return f"StepExecutionTimeline(provisioning_and_deployment_time={self.provisioning_and_deployment_time}, data_transmission_time={self.data_transmission_time}, step_processing_time={self.step_processing_time})"
    
    def __eq__(self, other):
        if isinstance(other, StepExecutionTimeline):
            return (self.provisioning_and_deployment_time == other.provisioning_and_deployment_time and
                    self.data_transmission_time == other.data_transmission_time and
                    self.step_processing_time == other.step_processing_time)
        return False
    
    def get_provisioning_and_deployment_time(self):
        """
        Returns the time required to provision and deploy resources for the step.

        Returns:
            float: The provisioning and deployment time in seconds.
        """
        return self.provisioning_and_deployment_time

    def get_data_transmission_time(self):
        """
        Returns the time required to transmit data to the step.

        Returns:
            float: The data transmission time in seconds.
        """
        return self.data_transmission_time

    def get_step_processing_time(self):
        """
        Returns the time required for the step to process the data.

        Returns:
            float: The step processing time in seconds.
        """
        return self.step_processing_time

    def get_total_time(self):
        """
        Returns the total time required for the step.

        This is the sum of the provisioning and deployment time, the data transmission time, and the step processing time.

        Returns:
            float: The total time in seconds.
        """
        return self.get_provisioning_and_deployment_time() + self.get_data_transmission_time() + self.get_step_processing_time()
    
    def get_time_to_first_result(self):
        """
        Returns the time to the first result.

        This is the sum of the provisioning and deployment time, the data transmission time, and the step processing time.

        Returns:
            float: The time to the first result in seconds.
        """
        return self.get_provisioning_and_deployment_time() + self.get_data_transmission_time() + self.get_step_processing_time()

class BatchStepExecutionTimeline(StepExecutionTimeline):
    """
    Represents the timeline for executing a BatchStep.
    Inherits from StepExecutionTimeline.
    """
    pass

class ProducerStepExecutionTimeline(StepExecutionTimeline):
    """
    Represents the timeline for executing a ProducerStep.
    Replaces step_processing_time with average_time_to_produce_output and number_of_produced_outputs.

    Attributes:
        average_time_to_produce_output (float): The average time required to produce an output.
        number_of_produced_outputs (int): The number of outputs produced.
    """

    def __init__(self, provisioning_and_deployment_time: float, data_transmission_time: float, average_time_to_produce_output: float, number_of_produced_outputs: int):
        if average_time_to_produce_output < 0:
            raise ValueError("Average time to produce output must be non-negative")
        if number_of_produced_outputs < 0:
            raise ValueError("Number of produced outputs must be non-negative")
        super().__init__(provisioning_and_deployment_time, data_transmission_time, 0)
        self.average_time_to_produce_output = average_time_to_produce_output
        self.number_of_produced_outputs = number_of_produced_outputs

    def get_step_processing_time(self):
        return self.average_time_to_produce_output * self.number_of_produced_outputs
    
    def get_time_to_first_result(self):
        return self.get_provisioning_and_deployment_time() + self.get_data_transmission_time() + self.average_time_to_produce_output

class DataSinkExecutionTimeline(StepExecutionTimeline):
    """
    Represents the timeline for executing a DataSink.
    Only has provisioning and deployment time and data transmission time.

    Attributes:
        step_processing_time (None): Not applicable for DataSink
        """
    def __init__(self, provisioning_and_deployment_time, data_transmission_time, number_of_transmitted_inputs):
        if data_transmission_time < 0:
            raise ValueError("Data transmission time must be non-negative")
        if number_of_transmitted_inputs < 0:
            raise ValueError("Number of transmitted inputs must be non-negative")
        super().__init__(provisioning_and_deployment_time, 0, 0)
        self.data_transmission_time = data_transmission_time
        self.number_of_transmitted_inputs = number_of_transmitted_inputs

    def get_data_transmission_time(self):
        return self.data_transmission_time * self.number_of_transmitted_inputs

    def get_step_processing_time(self):
        return 0

class DataSourceExecutionTimeline(StepExecutionTimeline):
    """
    Represents the timeline for executing a DataSource.
    Only has provisioning and deployment time.

    Attributes:
        data_transmission_time (None): Not applicable for DataSource
        step_processing_time (None): Not applicable for DataSource
    """
    def __init__(self, provisioning_and_deployment_time: float):
        super().__init__(provisioning_and_deployment_time, 0, 0)

    def get_data_transmission_time(self):
        return 0

    def get_step_processing_time(self):
        return 0

class ConsumerStepExecutionTimeline(StepExecutionTimeline):
    """
    Represents the timeline for executing a ConsumerStep.
    Modifies data_transmission_time and step_processing_time to account for multiple inputs and outputs.

    Attributes:
        average_data_transmission_time (float): The average time required to transmit a single input.
        number_of_transmitted_inputs (int): The number of inputs transmitted.
        average_data_processing_time (float): The average time required to process a single output.
        number_of_produced_outputs (int): The number of outputs produced.
    """
    def __init__(self, provisioning_and_deployment_time: float, average_data_transmission_time: float, number_of_transmitted_inputs: int, average_data_processing_time: float, number_of_produced_outputs: int):
        if average_data_transmission_time < 0:
            raise ValueError("Average data transmission time must be non-negative")
        if number_of_transmitted_inputs < 0:
            raise ValueError("Number of transmitted inputs must be non-negative")
        if average_data_processing_time < 0:
            raise ValueError("Average data processing time must be non-negative")
        if number_of_produced_outputs < 0:
            raise ValueError("Number of produced outputs must be non-negative")
        
        super().__init__(provisioning_and_deployment_time, 0, 0)
        self.average_data_transmission_time = average_data_transmission_time
        self.number_of_transmitted_inputs = number_of_transmitted_inputs
        self.average_data_processing_time = average_data_processing_time
        self.number_of_produced_outputs = number_of_produced_outputs

    def get_data_transmission_time(self):
        return self.average_data_transmission_time * self.number_of_transmitted_inputs

    def get_step_processing_time(self):
        return self.average_data_processing_time * self.number_of_produced_outputs
    
    def get_time_to_first_result(self):
        return self.get_provisioning_and_deployment_time() + self.average_data_transmission_time + self.average_data_processing_time

class PipelineStepDependency:
    """
    Represents a dependency between two PipelineSteps.

    Attributes:
        prerequisite_step (PipelineStep): The PipelineStep that is a prerequisite.
        dependent_step (PipelineStep): The PipelineStep that is dependent on the prerequisite step.
        dependency_type (str): The type of the dependency. Can be 'synchronous', 'asynchronous', or 'simultaneous'.
    """
    def __init__(self, dependency_type, dependent_step, prerequisite_step):
        if dependency_type not in ["synchronous", "asynchronous", "simultaneous"]:
            raise ValueError("Invalid dependency_type, choose from: 'synchronous', 'asynchronous', 'simultaneous'")

        if not isinstance(dependent_step, PipelineStep) or not isinstance(prerequisite_step, PipelineStep):
            raise ValueError("Both step and dependent_step must be instances of PipelineStep or its subclass.")

        self.dependency_type = dependency_type
        self.dependent_step = dependent_step
        self.prerequisite_step = prerequisite_step

    def __str__(self):
        return f"{self.step.name} --({self.dependency_type})-> {self.dependent_step.name}"

class DataTransmissionConnection:
    """
    Represents a data transmission connection between two PipelineSteps.

    Attributes:
        source_step (PipelineStep): The source PipelineStep of the data transmission.
        target_step (PipelineStep): The target PipelineStep of the data transmission.
    """
    def __init__(self, source_step, target_step):
        self.source_step = source_step
        self.target_step = target_step

    def __repr__(self):
        return f"DataTransmissionConnection(source_step={self.source_step}, target_step={self.target_step})"
    
class Pipeline:
    """
    Represents a Big Data pipeline which contains PipelineSteps and DataTransmissionConnections.

    Attributes:
        steps (list of PipelineStep): The steps in the pipeline.
        connections (list of DataTransmissionConnection): The data transmission connections between steps.
        dependencies (list of PipelineStepDependency): The dependencies between steps.
    """
    def __init__(self):
        self.steps: Set[PipelineStep] = set()
        self.connections: List[DataTransmissionConnection] = []
        self.dependencies: List[PipelineStepDependency] = []

    def find_step_by_name(self, name):
        for step in self.steps:
            if step.name == name:
                return step
        return None

    def add_connection(self, source_step: PipelineStep, target_step: PipelineStep):
        """Adds a connection and a dependency from the source step to the target step."""

        if not isinstance(source_step, PipelineStep) or not isinstance(target_step, PipelineStep):
            raise ValueError("Both source_step and target_step must be instances of PipelineStep or its subclass.")
        
        if isinstance(target_step, DataSource):
            raise ValueError("A DataSource step cannot be the target of a DataTransmissionConnection.")

        if isinstance(source_step, DataSink):
            raise ValueError("A DataSink step cannot be the source of a DataTransmissionConnection.")

        if not isinstance(source_step, DataProcessing) and not isinstance(target_step, DataProcessing):
            raise ValueError("At least one of source_step or target_step must be an instance of DataProcessing.")

        self.steps.add(source_step)
        self.steps.add(target_step)
        connection = DataTransmissionConnection(source_step, target_step)
        self.connections.append(connection)
        
        self.add_dependency("synchronous", target_step, source_step)
    
    def get_steps_with_incoming_dependency(self, target_step: PipelineStep) -> List[PipelineStep]:
        """
        Returns a list of steps that have incoming dependencies to the specified target step.

        Parameters:
        - target_step (PipelineStep): The target step to find incoming dependencies for.

        Returns:
        - dependent_steps (List[PipelineStep]): A list of PipelineStep objects that have incoming dependencies to the target step.
        """
        dependent_steps = []
        for dependency in self.dependencies:
            if dependency.dependent_step.same_as(target_step):
                dependent_steps.append(dependency.prerequisite_step)
        return dependent_steps

    def get_steps_with_incoming_data_transmission(self, target_step: PipelineStep) -> List[PipelineStep]:
        """
        Returns a list of steps that have incoming data transmission connections to the specified target step.

        Parameters:
        - target_step (PipelineStep): The target step to find incoming data transmission connections for.

        Returns:
        - connected_steps (List[PipelineStep]): A list of PipelineStep objects that have incoming data transmission connections to the target step.
        """
        connected_steps = []
        for connection in self.connections:
            if connection.target_step == target_step:
                connected_steps.append(connection.source_step)
        return connected_steps

    def _creates_cycle(self, new_dependency):
        def visit(step, visited, rec_stack):
            visited.add(step)
            rec_stack.add(step)

            for dep in self.dependencies:
                if dep.dependent_step == step:
                    next_step = dep.prerequisite_step
                    if next_step not in visited:
                        if visit(next_step, visited, rec_stack):
                            return True
                    elif next_step in rec_stack:
                        return True

            rec_stack.remove(step)
            return False

        temp_dependencies = self.dependencies.copy()
        temp_dependencies.append(new_dependency)

        visited = set()
        rec_stack = set()
        for step in self.steps:
            if step not in visited:
                if visit(step, visited, rec_stack):
                    return True

        return False  

    def add_dependency(self, dependency_type, dependent_step, prerequisite_step):
        """Adds a dependency between two steps, replacing any existing dependency between them."""
        if not isinstance(dependent_step, PipelineStep) or not isinstance(prerequisite_step, PipelineStep):
            raise ValueError("Both step and dependent_step must be instances of PipelineStep or its subclass.")

        temp_dependency = PipelineStepDependency(dependency_type, dependent_step, prerequisite_step)

        if self._creates_cycle(temp_dependency):
            raise ValueError("Adding this dependency would create a cycle in the dependency graph, which is not allowed.")

        # Remove existing dependency, if any
        self.dependencies = [dep for dep in self.dependencies if dep.dependent_step != dependent_step or dep.prerequisite_step != prerequisite_step]

        # Add the new dependency
        self.dependencies.append(temp_dependency)

    def _validate_no_cycles(self):
        visited = set()
        visiting = set()

        def visit(step):
            if step in visiting:
                return False
            if step not in visited:
                visited.add(step)
                visiting.add(step)
                for connection in self.connections:
                    if connection.source_step == step:
                        if not visit(connection.target_step):
                            return False
                visiting.remove(step)
            return True

        for step in self.steps:
            if not visit(step):
                return False
        return True
    
    def split_into_levels(self):
        """
        Splits the steps of the pipeline into levels. 

        The function begins with steps that are not dependent on other steps. 
        It creates a new level with these steps and any steps that are connected to them 
        through an 'asynchronous' dependency type. 

        For each subsequent level, the function includes steps that are directly 
        dependent on steps in the previous level and have a 'synchronous' dependency type. 

        It also includes any steps connected to the added steps through an 'asynchronous' 
        dependency type. 

        This process continues until all steps are assigned to a level.

        Returns:
            list of lists of PipelineStep: A list of levels, each containing a list of PipelineSteps.
        """
        def add_prerequisites(step, level):
            for dep in self.dependencies:
                if dep.dependent_step == step and dep.prerequisite_step not in visited:
                    visited.add(dep.prerequisite_step)
                    levels[level].add(dep.prerequisite_step)
                    add_prerequisites(dep.prerequisite_step, level)
                elif dep.dependent_step not in visited and dep.prerequisite_step in levels[level]:
                    queue.append((dep.dependent_step, level + 1))

        levels = []
        visited = set()
        queue = deque()

        # Find the nodes that are not dependent on other steps
        no_incoming_deps = set(self.steps)
        for dep in self.dependencies:
            no_incoming_deps.discard(dep.dependent_step)

        # Initialize the queue with the nodes that are not dependent on other steps
        for node in no_incoming_deps:
            queue.append((node, 0))

        # Modified BFS to build levels
        while queue:
            current_step, level = queue.popleft()

            # If this level doesn't exist yet, create it
            if level == len(levels):
                levels.append(set())

            # Add the current_step to the current level if not visited
            if current_step not in visited:
                levels[level].add(current_step)
                visited.add(current_step)

                # Add dependent steps with asynchronous dependency to the current level
                for dep in self.dependencies:
                    if dep.dependency_type == "asynchronous" and dep.prerequisite_step == current_step:
                        levels[level].add(dep.dependent_step)
                        visited.add(dep.dependent_step)
                        add_prerequisites(dep.dependent_step, level)

            # Add dependent steps with synchronous and simultaneous dependencies to the next level
            for dep in self.dependencies:
                if dep.dependency_type != "asynchronous" and dep.prerequisite_step == current_step and dep.dependent_step not in visited:
                    queue.append((dep.dependent_step, level + 1))

        # Filter out empty levels
        non_empty_levels = [level for level in levels if level]

        return non_empty_levels

    def display_dependency_graph(self):
        """
        Displays the dependency graph of the pipeline.

        This function uses the 'networkx' library to create a directed graph of the pipeline steps, 
        where the nodes represent the steps and the edges represent the dependencies between steps.

        The graph is then displayed using the 'matplotlib' library, with the 'kamada_kawai_layout' 
        function used to arrange the nodes.

        Note: This function does not return anything. It displays the graph as a side effect.
        """
        # Create a directed graph object
        G = nx.DiGraph()

        # Add nodes to the graph
        for step in self.steps:
            G.add_node(step.name)

        # Add edges to the graph based on dependencies
        for dep in self.dependencies:
            G.add_edge(dep.dependent_step.name, dep.prerequisite_step.name, dependency_type=dep.dependency_type)

        # Draw the graph with labels
        pos = nx.kamada_kawai_layout(G)
        nx.draw(G, pos, with_labels=True, node_size=2500, node_color="lightblue", font_size=10)
        edge_labels = {(u, v): d["dependency_type"] for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=10)

        plt.show()
    
    def display_pipeline_graph(self):
        """
        Displays the pipeline graph.

        This function uses the 'networkx' library to create a directed graph of the pipeline steps, 
        where the nodes represent the steps and the edges represent the data transmission connections 
        between steps.

        The graph is then displayed using the 'matplotlib' library, with the 'kamada_kawai_layout' 
        function used to arrange the nodes.

        Note: This function does not return anything. It displays the graph as a side effect.
        """

        # Create a directed graph object
        G = nx.DiGraph()

        # Add nodes to the graph
        for step in self.steps:
            G.add_node(step.name)

        # Add edges to the graph based on connections
        for connection in self.connections:
            G.add_edge(connection.source_step.name, connection.target_step.name)

        # Draw the graph with labels using kamada_kawai_layout
        pos = nx.kamada_kawai_layout(G)
        nx.draw(G, pos, with_labels=True, node_size=2500, node_color="lightblue", font_size=10)

        plt.show()

    def is_step_scalable(self, step: PipelineStep) -> bool:
        """
        Checks if a step is scalable. A step is scalable if there is an asynchronous outgoing dependency to another step.

        Args:
            step (PipelineStep): The step to check for scalability.

        Returns:
            bool: True if the step is scalable, False otherwise.
        """
        for dependency in self.dependencies:
            if dependency.dependent_step.same_as(step) and dependency.dependency_type == "asynchronous":
                return True
        return False

    def get_asynchronously_dependent_step(self, step: PipelineStep) -> PipelineStep:
        """
        Returns the step that is the target_step of an input PipelineStep where the dependency is of type 'asynchronous'.

        Args:
            step (PipelineStep): The step to check for asynchronous dependencies.

        Returns:
            PipelineStep: The step that is the target of an asynchronous dependency from the input step, or None if no such step exists.
        """
        for dependency in self.dependencies:
            if dependency.dependent_step.same_as(step) and dependency.dependency_type == "asynchronous":
                return dependency.prerequisite_step
        return None

    def __repr__(self):
        return f"Pipeline(steps={self.steps}, connections={self.connections})"