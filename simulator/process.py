import csv

class Process:
    def __init__(self, name, initial_timestep, workload_csv_file):
        self.__name = name
        self.__initial_timestep = initial_timestep
        self.__timestep = self.__initial_timestep
        self.__workload_csv_file = workload_csv_file
        self.__cumulative_energy = 0.0
        self.__emission = 0.0
        self.__cumulative_emission = 0.0
        self.__current_carbon_intensity = 0.0
        self.__current_energy = 0.0
        self.__workload_dict = {}

        with open(self.__workload_csv_file, mode='r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                timestamp = float(row['timestamp'])
                power_draw_W = float(row['power_draw_W'])
                self.__workload_dict[timestamp] = power_draw_W

    def tick(self):
        self.__timestep += 1
        if self.__timestep not in self.__workload_dict:
            return True

        current_energy = self.__workload_dict.get(self.__timestep, 0.0) / 3600.0
        
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
    def timestep(self):
        return self.__timestep
