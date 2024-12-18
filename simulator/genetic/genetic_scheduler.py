import random
from deap import base, creator, tools, algorithms
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator')
from stats import get_process_power_draw_stat
import pandas as pd
import math

class GeneticScheduler:
    def __init__(self, workload_stats_dir):
        self.edgeclusters = {} 
        self.workload_stats_dir = workload_stats_dir
        self.toolbox = base.Toolbox()
        self.stats = None
        self.workloads = []
        self.workloads_indices = []
        
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)

    def set_clusters(self, edgeclusters):
        for edgecluster in edgeclusters:
            self.edgeclusters[edgecluster.name] = edgecluster
        
        self.sorted_edge_clusters = [edge_cluster for edge_cluster in self.edgeclusters.values()]
        self.sorted_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
            
    def calc_emission_forecast(self, workload):
        forecast_data = {}
        deadline_hours = math.ceil(workload["deadline"] / 3600)
        forecast_hours = range(0, deadline_hours)
        for cluster_name, cluster_obj in self.edgeclusters.items():
            hourly_values = []

            for hour in forecast_hours:
                future_seconds = hour * 3600
                intensity = cluster_obj.carbon_intensity_future(future_seconds)
                hourly_values.append(intensity)
                forecast_data[cluster_name] = hourly_values

        df = pd.DataFrame.from_dict(forecast_data,
                orient='index',
                columns=[f'H+{h}' for h in forecast_hours])
        #print(df)
        best_hour = df.min().idxmin()
        #print(best_hour)
        return best_hour    

    def print_clusters(self):
        for cluster in self.edgeclusters:
            print(cluster, self.edgeclusters[cluster].name, self.edgeclusters[cluster].available_gpus, self.edgeclusters[cluster].carbon_intensity)

    def set_workloads(self, workload_indices):
        self.workloads = []
        for idx in workload_indices:
            power_draw_mean, power_draw_median, total_power_consumption, duration = get_process_power_draw_stat(self.workload_stats_dir, idx)
            self.workloads.append({
                "id": idx,
                "power": power_draw_mean,
                "energy": total_power_consumption,
                "duration": duration,
                "deadline": 60 * 60 * 24,
            })
        return self.workloads
    
    def set_workloads_processes(self, processes):
        self.workloads = []
        for process in processes:
            power_draw_mean, power_draw_median, total_power_consumption, duration = get_process_power_draw_stat(self.workload_stats_dir, process.idx)
            self.workloads.append({
                "id": process.idx,
                "power": power_draw_mean,
                "energy": total_power_consumption,
                "duration": duration,
                "deadline": process.deadline,
            })
        return self.workloads

    def print_workloads(self):
        for workload in self.workloads:
            print(workload)

    def calculate_fitness(self, schedule):
        penalty = 0
        gpu_availability = {cluster: self.edgeclusters[cluster].available_gpus for cluster in self.edgeclusters}

        energy_penalty = 0
        deadline_penalty = 0

        #print("Schedule:", schedule)
        
        for workload, cluster in zip(self.workloads, schedule):
            best_hour = self.calc_emission_forecast(workload)
            #print("best_hour", best_hour)
            if cluster == "wait":
                if workload["deadline"] <= 0:
                   # severely penalize if deadline is missed 
                   deadline_penalty += 1000000
                else:
                    if best_hour == "H+7": 
                        co2_intensity = self.sorted_edge_clusters[0].carbon_intensity
                        deadline_penalty += workload["power"] * co2_intensity
                continue

            print(cluster) 
            print("best cluster", self.sorted_edge_clusters[0].name)
            print("gpu_availability", gpu_availability[cluster])
            if cluster == self.sorted_edge_clusters[0].name and gpu_availability[cluster] > 0:
                # if best cluster is not fullt utilized
                penalty += 1000

            # severely penalize if over using GPUs
            if gpu_availability[cluster] <= 0:
                penalty += 1000000
            
            gpu_availability[cluster] -= 1

            if gpu_availability[cluster] > 0:
                penalty += 1000

            co2_intensity = self.edgeclusters[cluster].carbon_intensity
            power = workload["power"]
            emission = co2_intensity * power
            energy_penalty += emission

        min_energy_penalty = 0
        max_energy_penalty = 350 * 1000

        if max_energy_penalty > min_energy_penalty:
            normalized_energy_penalty = (energy_penalty - min_energy_penalty) / (max_energy_penalty - min_energy_penalty) * 9999
        else:
            normalized_energy_penalty = 0

        penalty += normalized_energy_penalty
        penalty += deadline_penalty

        return (-penalty,)

    def mutate_individual(self, individual):
        for i in range(len(individual)):
            if random.random() < 0.2:
                #individual[i] = random.choice(list(self.clusters.keys()))
                individual[i] = random.choice(list(self.edgeclusters.keys()) + ["wait"])
        return individual,

    def initialize_deap(self):
        self.toolbox.register("attr_cluster", lambda: random.choice(list(self.edgeclusters.keys()) + ["wait"]))
        self.toolbox.register("individual", tools.initRepeat, creator.Individual, self.toolbox.attr_cluster, n=len(self.workloads))
        self.toolbox.register("population", tools.initRepeat, list, self.toolbox.individual)
        self.toolbox.register("evaluate", self.calculate_fitness)
        self.toolbox.register("mutate", self.mutate_individual)
        self.toolbox.register("mate", tools.cxUniform, indpb=0.5)
        self.toolbox.register("select", tools.selTournament, tournsize=3)

        self.stats = tools.Statistics(lambda ind: ind.fitness.values)
        self.stats.register("avg", lambda x: round(sum(v[0] for v in x) / len(x), 2))
        self.stats.register("min", lambda x: min(v[0] for v in x))
        self.stats.register("max", lambda x: max(v[0] for v in x))

    def run(self, generations=500, population_size=50):
        self.initialize_deap()
        population = self.toolbox.population(n=population_size)
        population, _ = algorithms.eaSimple(population, self.toolbox, cxpb=0.7, mutpb=0.2, ngen=generations,
                                   stats=self.stats, verbose=False)

        best_individual = tools.selBest(population, k=1)[0]

        schedule = {}

        for workload, cluster in zip(self.workloads, best_individual):
            schedule[workload["id"]] = cluster

        return schedule, best_individual.fitness.values[0]
