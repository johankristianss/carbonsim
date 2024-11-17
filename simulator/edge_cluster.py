import csv

class EdgeCluster:
    def __init__(self, name, nodes, gpu_per_node, carbon_csv_file):
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

        with open(carbon_csv_file, mode='r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                timestamp = int(row['datetime'])
                carbon_intensity = float(row['carbonIntensity'])
                self.__carbon_intensity_dict[float(timestamp)] = carbon_intensity

    def run(self, process):
        if self.available:
            self.__processes.append(process)
            return True
        
        return False

    def tick(self):
        current_carbon_intensity = self.__carbon_intensity_dict.get(self.__timestep, 0.0)

        for process in self.__processes:
            process.carbon_intensity = current_carbon_intensity
            if process.tick():
                print(f'process <{process.name}> finished at timestep {self.__timestep}')
                self.__processes.remove(process)  
                continue 

            self.__cumulative_energy += process.energy
            self.__cumulative_emission += process.emission

        self.__timestep += 1

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
