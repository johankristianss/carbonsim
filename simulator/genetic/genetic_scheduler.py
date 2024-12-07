import random
from deap import base, creator, tools, algorithms
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator')
from stats import get_process_power_draw_stat

class GeneticScheduler:
    def __init__(self, workload_stats_dir):
        self.clusters = {} 
        self.workload_stats_dir = workload_stats_dir
        self.toolbox = base.Toolbox()
        self.stats = None
        self.workloads = []
        self.workloads_indices = []
        
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)

    def set_clusters_status(self, edgeclusters):
        self.clusters = {}
        for edgecluster in edgeclusters:
            self.clusters[edgecluster.name] = {
                "GPUs": edgecluster.gpus,
                "CO2": edgecluster.carbon_intensity
            }

    def set_clusters_status_raw(self, clusters):
        self.clusters = clusters

    def print_clusters(self):
        for cluster in self.clusters:
            print(cluster, self.clusters[cluster])  

    def set_workloads(self, workload_indices):
        self.workloads = []
        for idx in workload_indices:
            power_draw_mean, _, _, duration = get_process_power_draw_stat(self.workload_stats_dir, idx)
            self.workloads.append({
                "id": idx,
                "power": power_draw_mean,
                "duration": duration,
                "deadline": 0
            })
        return self.workloads

    def print_workloads(self):
        for workload in self.workloads:
            print(workload)

    def calculate_fitness(self, schedule):
        penalty = 0
        gpu_availability = {cluster: self.clusters[cluster]["GPUs"] for cluster in self.clusters}

        energy_penalty = 0
        deadline_penalty = 0

        # print("Schedule:", schedule)
        
        for workload, cluster in zip(self.workloads, schedule):
            if cluster == "wait":
                if workload["deadline"] <= 0:
                   deadline_penalty += 1000
                elif workload["deadline"] > 0: # and workload["power"] > 120:
                    # find the lowest co2 intensity cluster in all available clusters
                    lowest_co2_intensity = 0
                    for cluster in self.clusters:
                        if gpu_availability[cluster] > 0:
                            if lowest_co2_intensity == 0 or self.clusters[cluster]["CO2"] < lowest_co2_intensity:
                                lowest_co2_intensity = self.clusters[cluster]["CO2"]

                    deadline_penalty += workload["power"] # * lowest_co2_intensity / 2
                continue

            if gpu_availability[cluster] <= 0:
                penalty += 10000
            
            gpu_availability[cluster] -= 1

            co2_intensity = self.clusters[cluster]["CO2"]
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
                individual[i] = random.choice(list(self.clusters.keys()) + ["wait"])
        return individual,

    def initialize_deap(self):
        self.toolbox.register("attr_cluster", lambda: random.choice(list(self.clusters.keys()) + ["wait"]))
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
