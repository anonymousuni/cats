import math
from typing import Optional

class CloudResourceProvider:
    """
    Represents a cloud resource provider.

    Attributes:
        name (str): The name of the cloud resource provider.
        price_per_gb (float): The price for transmission of a gigabyte of data.

    Methods:
        calculate_price(source_zone, destination_zone, data_in_gb):
            Calculates the price based on the data size.

    """
    def __init__(self, name: str, price_per_gb: float):
        self.name = name
        self.price_per_gb = price_per_gb

    def calculate_price(self, source_zone: str, destination_zone: str, data_in_gb: float) -> float:
        """
        Calculates the price based on the data size.

        Args:
            source_zone (str): The source zone of the data.
            destination_zone (str): The destination zone of the data.
            data_in_gb (float): The size of the data in gigabytes.

        Returns:
            float: The calculated price.

        """
        # Assuming that the price does not depend on the zones for now
        return self.price_per_gb * data_in_gb

class AWSResourceProvider():
    """
    Represents a resource provider for AWS cloud services.

    Attributes:
        price_per_gb (float): The price per gigabyte transferred out of AWS.
        price_per_gb_internet (float): The price per gigabyte of data transferred from the internet.
        price_per_gb_zones (float): The price per gigabyte of data transferred between availability zones.
    """
    price_per_gb = 0.09
    price_per_gb_internet = 0.09
    price_per_gb_zones = 0.02

    @staticmethod
    def calculate_data_transmission_price(source_zone: Optional[str], destination_zone: str, data_in_gb: float) -> float:
        """
        Calculates the price of data transmission based on the source and destination zones.

        Args:
            source_zone (str): The source zone of the data transmission.
            destination_zone (str): The destination zone of the data transmission.
            data_in_gb (float): The amount of data to be transmitted in gigabytes.

        Returns:
            float: The price of data transmission.

        Raises:
            None
        """
        if source_zone is not None and destination_zone is None:
            # Transferring outside regions
            return AWSResourceProvider.price_per_gb * data_in_gb
        
        if source_zone is None:
            # Transferring to AWS from the Internet
            return 0
        elif source_zone == destination_zone:
            # Transferring within the same region
            return 0
        elif source_zone.split("-")[0] == destination_zone.split("-")[0]:
            # Transferring between availability zones in the same region
            return AWSResourceProvider.price_per_gb_zones * data_in_gb
        

class ContainerRepository:
    """
    TODO NOT USED AT THE MOMENT
    This class represents a container repository.

    Attributes:
        name (str): The name of the container repository.
    """

    def __init__(self, name):
        """
        Constructs all the necessary attributes for the ContainerRepository object.

        Parameters:
            name (str): The name of the container repository.
        """
        self.name = name

    def __repr__(self):
        """
        Returns a string representation of the ContainerRepository object.

        Returns:
            str: A string representation of the ContainerRepository object.
        """
        return f"ContainerRepository(name={self.name})"

class NetworkGraph:
    """
    This class represents a network graph.

    Attributes:
        nodes (List[Union[SimpleComputingResource, ContainerRepository]]): The nodes in the network graph.
        edges (Dict[Tuple[Union[SimpleComputingResource, ContainerRepository], Union[SimpleComputingResource, ContainerRepository]], float]): The edges in the network graph, represented as a dictionary where the keys are tuples of nodes and the values are the bandwidths between the nodes.
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the NetworkGraph object.
        """
        self.nodes = []
        self.edges = {}

    def get_all_computing_resources(self):
        """
        Returns a list of all eligible SimpleComputingResource objects in the network graph that are schedulable.
    
        Returns:
            list: A list of SimpleComputingResource objects that are schedulable.
        """
        return [node for node in self.nodes if isinstance(node, SimpleComputingResource)]

    def get_eligible_computing_resources(self):
        """
        Returns a list of all eligible SimpleComputingResource objects in the network graph that are schedulable.
    
        Returns:
            list: A list of SimpleComputingResource objects that are schedulable.
        """
        return [node for node in self.nodes if isinstance(node, SimpleComputingResource) and node.is_schedulable]

    def add_node(self, node, container_repositories):
        """
        Adds a node to the network graph and connects it to the specified container repositories.

        Parameters:
            node (Union[SimpleComputingResource, ContainerRepository]): The node to add.
            container_repositories (List[Tuple[ContainerRepository, float]]): A list of tuples, where each tuple contains a container repository and the associated bandwidth.
        """
        assert isinstance(node, (SimpleComputingResource, ContainerRepository)), "node must be an instance of SimpleComputingResource or ContainerRepository"
        assert isinstance(container_repositories, list), "container_repositories must be a list"
        for cr in container_repositories:
            assert isinstance(cr, tuple) and len(cr) == 2, "Each item in container_repositories must be a tuple of length 2"
            assert isinstance(cr[0], ContainerRepository), "The first item in each tuple in container_repositories must be an instance of ContainerRepository"
            assert isinstance(cr[1], (int, float)), "The second item in each tuple in container_repositories must be an integer or a float"

        self.nodes.append(node)
        for container_repository, bandwidth in container_repositories:
            self.add_edge(node, container_repository, bandwidth)

    def add_edge(self, node1, node2, bandwidth):
        """
        Adds an edge to the network graph.

        Parameters:
            node1 (Union[SimpleComputingResource, ContainerRepository]): The first node of the edge.
            node2 (Union[SimpleComputingResource, ContainerRepository]): The second node of the edge.
            bandwidth (float): The bandwidth between the nodes in Mbps.
        """
        assert isinstance(node1, (SimpleComputingResource, ContainerRepository)), "node1 must be an instance of SimpleComputingResource or ContainerRepository"
        assert isinstance(node2, (SimpleComputingResource, ContainerRepository)), "node2 must be an instance of SimpleComputingResource or ContainerRepository"
        assert isinstance(bandwidth, (int, float)), "bandwidth must be an integer or a float"

        self.edges[(node1, node2)] = bandwidth

    def get_bandwidth(self, node1, node2):
        """
        Returns the bandwidth between two nodes.

        Parameters:
                node1 (Union[SimpleComputingResource, ContainerRepository]): The first node.
        node2 (Union[SimpleComputingResource, ContainerRepository]): The second node.

        Returns:
            float: The bandwidth between the nodes, or None if there is no connection between the nodes and 0 if the nodes are the same.
        """
        assert isinstance(node1, (SimpleComputingResource, ContainerRepository)), "node1 must be an instance of SimpleComputingResource or ContainerRepository"
        assert isinstance(node2, (SimpleComputingResource, ContainerRepository)), "node2 must be an instance of SimpleComputingResource or ContainerRepository"

        if(node1 == node2):
            return 0

        return self.edges.get((node1, node2))
    
    def get_bandwidth_to_container_repository(self, node, container_repository):
        """
        Returns the bandwidth between a node and a container repository.

        Parameters:
            node (Union[SimpleComputingResource, ContainerRepository]): The node.
            container_repository (ContainerRepository): The container repository.

        Returns:
            float: The bandwidth between the node and the container repository, or None if there is no connection between them.
        """
        assert isinstance(node, (SimpleComputingResource, ContainerRepository)), "node must be an instance of SimpleComputingResource or ContainerRepository"
        assert isinstance(container_repository, ContainerRepository), "container_repository must be an instance of ContainerRepository"

        return self.edges.get((node, container_repository))

    def __repr__(self):
        """
        Returns a string representation of the NetworkGraph object.

        Returns:
            str: A string representation of the NetworkGraph object.
        """
        return f"NetworkGraph(nodes={self.nodes}, edges={self.edges})"



class SimpleComputingResource:
    """
    This class represents a simple computing resource.

    Attributes:
        name (str): The name of the computing resource.
        num_cpus (int): The number of CPUs in the computing resource.
        cpu_frequency (float): The frequency of the CPUs in GHz.
        ram_capacity (float): The RAM capacity of the computing resource in GiB.
        total_price (float): The total price of the computing resource. Default is 0.

    Methods:
        calculate_total_price(): Returns the total price of the computing resource.
    """

    def __init__(self, name: str, num_cpus: int, cpu_frequency: float, ram_capacity: float, total_price: float = 0):
        """
        Constructs all the necessary attributes for the SimpleComputingResource object.

        Parameters:
            name (str): The name of the computing resource.
            num_cpus (int): The number of CPUs in the computing resource.
            cpu_frequency (float): The frequency of the CPUs in GHz.
            ram_capacity (float): The RAM capacity of the computing resource in GB.
            total_price (float): The total price of the computing resource. Default is 0.
        """
        self.name = name
        self.num_cpus = num_cpus
        self.cpu_frequency = cpu_frequency
        self.ram_capacity = ram_capacity
        self.total_price = total_price
        self.availability_zone = None
        self.is_schedulable = True

    def calculate_total_price(self):
        """
        Returns the total price of the computing resource.

        Returns:
            float: The total price of the computing resource.
        """
        return self.total_price

    def calculate_price_to_transmit_data(self, destination_resource, data_amount):
        """
        Calculates the price to transmit data to the destination resource.

        Parameters:
            destination_resource (SimpleComputingResource): The destination resource.

        Returns:
            float: The price to transmit data. Currently returns 0.
        """
        return 0

    def calculate_price_to_receive_data(self, source_resource, data_amount):
        """
        Calculates the price to receive data from the source resource.

        Parameters:
            source_resource (SimpleComputingResource): The source resource.

        Returns:
            float: The price to receive data. Currently returns 0.
        """
        return 0
    
    def disable_scheduling(self):
        """
        Sets the is_schedulable attribute to False.
        """
        self.is_schedulable = False
        

    def __repr__(self):
        """
        Returns a string representation of the SimpleComputingResource object.

        Returns:
            str: A string representation of the SimpleComputingResource object.
        """
        return (f"ComputingResource(name={self.name}, num_cpus={self.num_cpus}, cpu_frequency={self.cpu_frequency}, "
                f"available_ram={self.ram_capacity}, total_price={self.total_price})")

class OnDemandInstance(SimpleComputingResource):
    """
    Represents an on-demand computing instance.

    Args:
        name (str): The name of the computing resource.
        num_cpus (int): The number of CPUs.
        cpu_frequency (float): The CPU frequency in GHz.
        ram_capacity (int): The RAM capacity in GiB.
        on_demand_price_per_hour (float): The on-demand price per hour in USD.
        total_price (float, optional): The total price. Defaults to 0.

    Attributes:
        on_demand_price_per_hour (float): The on-demand price per hour.
        reservation_time_in_hours (float): The total reservation time in hours.

    Methods:
        get_on_demand_total_price(): Calculates the total price for the on-demand instance based on the time of usage.
        add_reservation_time(additional_time_in_seconds): Adds additional reservation time to the on-demand instance.
        get_added_price_for_reservation(time_period_in_seconds): Calculates the added price if the on-demand instance is reserved for a specific time period.

    """

    def __init__(self, name: str, num_cpus: int, cpu_frequency: float, ram_capacity: int, on_demand_price_per_hour: float, total_price: float = 0):
        super().__init__(name, num_cpus, cpu_frequency, ram_capacity, total_price)
        self.on_demand_price_per_hour = on_demand_price_per_hour
        self.reservation_time_in_hours = 0

    def get_on_demand_total_price(self):
        """
        Calculates the total price for the on-demand instance based on the time of usage.

        Returns:
            float: The total price.
        """
        return self.on_demand_price_per_hour * self.reservation_time_in_hours

    def add_reservation_time(self, additional_time_in_seconds):
        """
        Adds additional reservation time to the on-demand instance.

        Args:
            additional_time_in_seconds (int): The additional time in seconds.
        """
        additional_time_in_hours = additional_time_in_seconds / 3600  # Convert seconds to hours
        self.reservation_time_in_hours += additional_time_in_hours
        self.total_price += self.get_on_demand_total_price()
    
    def get_added_price_for_reservation(self, time_period_in_seconds):
        """
        Calculates the added price if the on-demand instance is reserved for a specific time period.

        Args:
            time_period_in_seconds (int): The time period in seconds.

        Returns:
            float: The added price.
        """
        time_period_in_hours = math.ceil(time_period_in_seconds / 3600)  # Convert seconds to hours and round up
        current_reservation_price = self.on_demand_price_per_hour * self.reservation_time_in_hours
        new_reservation_price = self.on_demand_price_per_hour * time_period_in_hours
        added_price = new_reservation_price - current_reservation_price
        return added_price if added_price > 0 else 0
    
    def calculate_total_price(self):
        """
        Returns the total price of the computing resource.

        Returns:
            float: The total price of the computing resource.
        """
        return self.get_on_demand_total_price()

class AmazonOnDemandEC2Instance(OnDemandInstance):
    """
    Represents an Amazon EC2 instance that is available on-demand.

    Args:
        name (str): The name of the computing resource.
        num_cpus (int): The number of CPUs of the instance.
        cpu_frequency (float): The CPU frequency of the instance in GHz.
        ram_capacity (int): The RAM capacity of the instance in GiB.
        availability_zone (str): The availability zone where the instance is located.
        on_demand_price_per_hour (float): The on-demand price per hour of the instance in USD.

    Attributes:
        availability_zone (str): The availability zone where the instance is located.
        reservation_time_in_hours (float): The time of usage in hours.
        total_price (float): The total price of using the instance for the specified time of usage.

    Methods:
        __repr__(): Returns a string representation of the instance.

    Inherits:
        OnDemandInstance: The base class for on-demand instances.
    """

    def __init__(self, name: str, num_cpus: int, cpu_frequency: float, ram_capacity: int, availability_zone: str, on_demand_price_per_hour: float):
        super().__init__(name, num_cpus, cpu_frequency, ram_capacity, on_demand_price_per_hour)
        self.availability_zone = availability_zone
        self.reservation_time_in_hours = 0
        self.total_price = self.get_on_demand_total_price()
        self.resource_provider = AWSResourceProvider

    def calculate_price_to_transmit_data(self, destination_resource: SimpleComputingResource, data_amount: float):
        """
        Calculates the price to transmit data to the destination resource.

        Parameters:
            destination_resource (SimpleComputingResource): The destination resource.
            data_amount (float): The amount of data to be transmitted in gigabytes.

        Returns:
            float: The price to transmit data.
        """
        return AWSResourceProvider.calculate_data_transmission_price(self.availability_zone, destination_resource.availability_zone, data_amount)

    def calculate_price_to_receive_data(self, source_resource: SimpleComputingResource, data_amount: float):
        """
        Calculates the price to receive data from the source resource.

        Parameters:
            source_resource (SimpleComputingResource): The source resource.
            data_amount (float): The amount of data to be received in gigabytes.

        Returns:
            float: The price to receive data.
        """
        return AWSResourceProvider.calculate_data_transmission_price(source_resource.availability_zone, self.availability_zone, data_amount)
    
    def __repr__(self):
        return (f"AmazonOnDemandEC2Instance(name={self.name}, num_cpus={self.num_cpus}, cpu_frequency={self.cpu_frequency}, "
                f"available_ram={self.ram_capacity}, availability_zone='{self.availability_zone}', "
                f"on_demand_price_per_hour={self.on_demand_price_per_hour}, time_of_usage={self.reservation_time_in_hours}, "
                f"total_price={self.total_price})")



# class ComputingResourceState:
#     """
#     TODO NOT USED AT THE MOMENT
#     Represents a state of a resource in time.


#     Attributes:
#         resource (SimpleComputingResource): The resource whose state is represented.
#         available_cpu_percentage (float): The percentage of CPU available at this state.
#         available_ram (float): The amount of RAM available at this state.
#     """

#     def __init__(self, resource: SimpleComputingResource, available_cpu_percentage: float, available_ram: float):
#         """
#         Initializes a new instance of the ComputingResourceState class.

#         Args:
#             resource (SimpleComputingResource): The resource whose state is represented.
#             available_cpu_percentage (float): The percentage of CPU available at this state.
#             available_ram (float): The amount of RAM available at this state.
#         """
#         assert isinstance(resource, SimpleComputingResource), "resource must be an instance of SimpleComputingResource or its subclass"
#         assert isinstance(available_cpu_percentage, float), "available_cpu_percentage must be a float"
#         assert isinstance(available_ram, float), "available_ram must be a float"

#         self.resource = resource
#         self.available_cpu_percentage = available_cpu_percentage
#         self.available_ram = available_ram