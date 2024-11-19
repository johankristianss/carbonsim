import csv

class EdgeCluster:
    def __init__(self, name, nodes, gpu_per_node, carbon_csv_file, cost, utilization_threshold):
        self.__name = name
        self.__nodes = nodes
        self.__gpu_per_node = gpu_per_node
        self.__carbon_csv_file = carbon_csv_file
        self.__carbon_intensity_dict = {}
        self.__gpus = nodes * gpu_per_node
        self.__timestep = 0
        self.__processes = []
        self.__cumulative_energy = 0.0
        self.__cumulative_emission = 0.0
        self.__gpu_cost = cost
        self.__total_gpu_cost = 0.0
        self.__total_processing_time = 0
        self.__finsihed_processes = 0
        self.__utilization_threshold = utilization_threshold 

        with open(carbon_csv_file, mode='r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                timestamp = int(row['datetime'])
                carbon_intensity = float(row['carbonIntensity'])
                self.__carbon_intensity_dict[float(timestamp)] = carbon_intensity
        
        print(f'EdgeCluster <{name}> created with {nodes} nodes and {gpu_per_node} GPUs per node')

    def run(self, process):
        if self.utilization > self.__utilization_threshold:
            return False
        if self.available:
            self.__processes.append(process)
            return True
        
        return False

    def tick(self):
        current_carbon_intensity = self.__carbon_intensity_dict.get(self.__timestep, 0.0)
        # print(f'EdgeCluster <{self.__name}> timestep {self.__timestep} with carbon intensity {current_carbon_intensity}')

        for process in self.__processes:
            process.carbon_intensity = current_carbon_intensity
            if process.tick():
                print(f'Process <{process.name}> finished at timestep {self.__timestep}')
                self.__processes.remove(process)  
                self.__finsihed_processes += 1
                continue 

            self.__cumulative_energy += process.energy
            self.__cumulative_emission += process.emission
            self.__total_gpu_cost += self.__gpu_cost
            self.__total_processing_time += 1
            
            # print edge cluster name and utilization
            # print(f'EdgeCluster <{self.__name}> utilization: {self.utilization}')

        self.__timestep += 1

    def integrate_carbon_intensity(self, total_length_seconds):
        integrated_carbon = 0.0

        for timestep in range(self.__timestep, self.__timestep + total_length_seconds):
            carbon_intensity = self.__carbon_intensity_dict.get(float(timestep), 0.0)
            integrated_carbon += carbon_intensity

        return integrated_carbon

    @property
    def carbon_intensity(self):
        return self.__carbon_intensity_dict.get(self.__timestep, 0.0)

    @property
    def num_running_processes(self):
        return len(self.__processes)

    @property
    def name(self):
        return self.__name

    @property
    def nodes(self):
        return self.__nodes

    @property
    def gpu_per_node(self):
        return self.__gpu_per_node
  
    @property
    def gpus(self):
        return self.__gpus

    @property
    def carbon_csv_file(self):
        return self.__carbon_csv_file

    @property
    def cumulative_energy(self):
        return self.__cumulative_energy / 1000

    @property
    def cumulative_emission(self):
        return self.__cumulative_emission / 1000

    @property
    def timestep(self):
        return self.__timestep

    @property
    def available(self):
        return self.__gpus - len(self.__processes) > 0

    @property
    def utilization(self):
        total = self.__nodes * self.__gpu_per_node
        used = len(self.__processes)
        return used / total

    @property
    def total_gpu_cost(self):
        return self.__total_gpu_cost

    @property
    def total_processing_time(self):
        return self.__total_processing_time

    @property
    def finished_processes(self):
        return self.__finsihed_processes
