import csv

class Process:
    def __init__(self, name, idx, initial_timestep, workload_csv_file):
        self.__name = name
        self.__idx = idx
        self.__initial_timestep = initial_timestep
        self.__timestep = self.__initial_timestep
        self.__duration = 0
        self.__workload_csv_file = workload_csv_file
        self.__cumulative_energy = 0.0
        self.__emission = 0.0
        self.__cumulative_emission = 0.0
        self.__current_carbon_intensity = 0.0
        self.__current_energy = 0.0
        self.__current_gpu_utilization = 0.0
        self.__current_memory_utilization = 0.0
        self.__power_draw_W_dict = {}
        self.__utilization_gpu_pct_dict = {}
        self.__utilization_memory_pct_dict = {}

        with open(self.__workload_csv_file, mode='r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                # print("----------------- row", idx)
                # print(row)
                # print("----------------- row end")

                timestamp = float(row['timestamp'])
                power_draw_W = float(row['power_draw_W'])
                utilization_gpu_pct = float(row['utilization_gpu_pct'])
                utilization_memory_pct = float(row['utilization_memory_pct'])
                self.__power_draw_W_dict[timestamp] = power_draw_W
                self.__utilization_gpu_pct_dict[timestamp] = utilization_gpu_pct
                self.__utilization_memory_pct_dict[timestamp] = utilization_memory_pct

    def tick(self):
        self.__timestep += 1
        self.__duration += 1
        if self.__timestep not in self.__power_draw_W_dict:
            return True

        current_energy = self.__power_draw_W_dict.get(self.__timestep, 0.0) / 3600.0
        self.__current_gpu_utilization = self.__utilization_gpu_pct_dict.get(self.__timestep, 0.0)
        self.__current_memory_utilization = self.__utilization_memory_pct_dict.get(self.__timestep, 0.0)
        
        # trapezoidal rule
        if self.__timestep > 1:
            self.__cumulative_energy += 0.5 * (self.__previous_energy + current_energy)
        else:
            self.__cumulative_energy += current_energy

        self.__previous_energy = current_energy

        self.__emission = current_energy * self.__current_carbon_intensity
        self.__cumulative_emission += self.__emission

        self.__current_energy = current_energy

    @property
    def name(self):
        return self.__name

    @property
    def carbon_intensity(self):
        return self.__current_carbon_intensity
    
    @carbon_intensity.setter
    def carbon_intensity(self, value):
        self.__current_carbon_intensity = value 
    
    @property
    def energy(self):
        return self.__current_energy

    @property
    def cumulative_energy(self):
        return self.__cumulative_energy / 1000
    
    @property
    def emission(self):
        return self.__emission
    
    @property
    def cumulative_emission(self):
        return self.__cumulative_emission / 1000
    
    @property
    def gpu_utilization(self):
        return self.__current_gpu_utilization

    @property
    def memory_utilization(self):
        return self.__current_memory_utilization

    @property
    def timestep(self):
        return self.__timestep

    @property
    def duration(self):
        return self.__duration

    @property
    def idx(self):
        return self.__idx
