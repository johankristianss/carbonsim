import random
from deap import base, creator, tools, algorithms
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator')
from stats import get_process_power_draw_stat

class GeneticPool:
    def __init__(self, clusters, workload_stats_dir):
        self.clusters = clusters
        self.workload_stats_dir = workload_stats_dir
        self.toolbox = base.Toolbox()
        self.stats = None
        self.workloads = []
        self.workloads_indices = []

    def add_workloads(self, workload_indices):
        self.workloads = []
        for idx in workload_indices:
            power_draw_mean, _, _, duration = get_process_power_draw_stat(self.workload_stats_dir, idx)
            self.workloads.append({
                "id": idx,
                "power": power_draw_mean,
                "duration": duration,
                "deadline": 0
            })
        self.workloads[0]["deadline"] = 100
        self.workloads[0]["power"] = 3
        return self.workloads

    def print_workloads(self):
        for workload in self.workloads:
            print(workload)

    def calculate_fitness(self, schedule):
        penalty = 0
        gpu_availability = {cluster: self.clusters[cluster]["GPUs"] for cluster in self.clusters}

        energy_penalty = 0
        deadline_penalty = 0

        print("Schedule:", schedule)
        for workload, cluster in zip(self.workloads, schedule):
            #print(workload["id"], cluster)

            if cluster == "wait": # Should we wait deploying this workload?
                # if workload["deadline"] <= 0: # If deadline is positive, we should wait
                #     deadline_penalty += 1000 # 350 - workload["power"] # 10000 # workload["power"]
                deadline_penalty += (200-workload["deadline"]) * workload["power"] 
                continue

            if gpu_availability[cluster] <= 0:
                penalty += 10000
            
            gpu_availability[cluster] -= 1

            co2_intensity = self.clusters[cluster]["CO2"]
            power = workload["power"]
            emission = co2_intensity * power
            energy_penalty += emission

            # if workload["deadline"] < 0:
            #     deadline_penalty += abs(workload["deadline"]) * 100

        min_energy_penalty = 0
        max_energy_penalty = 350 * 1000

        if max_energy_penalty > min_energy_penalty:
            normalized_energy_penalty = (energy_penalty - min_energy_penalty) / (max_energy_penalty - min_energy_penalty) * 9999
        else:
            normalized_energy_penalty = 0

        print("DEADLINE PENALTY:", deadline_penalty)

        penalty += normalized_energy_penalty
        penalty += deadline_penalty

        return (-penalty,)

    def mutate_individual(self, individual):
        for i in range(len(individual)):
            if random.random() < 0.2:
                #individual[i] = random.choice(list(self.clusters.keys()))
                individual[i] = random.choice(list(clusters.keys()) + ["wait"])
        return individual,

    def initialize_deap(self):
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)

        #self.toolbox.register("attr_cluster", lambda: random.choice(list(self.clusters.keys())))
        self.toolbox.register("attr_cluster", lambda: random.choice(list(clusters.keys()) + ["wait"]))
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
        return algorithms.eaSimple(population, self.toolbox, cxpb=0.7, mutpb=0.2, ngen=generations,
                                   stats=self.stats, verbose=False)

# Example usage:
if __name__ == "__main__":
    clusters = {
        "A": {"GPUs": 2, "CO2": 10},
        "B": {"GPUs": 3, "CO2": 100},
    }
    workload_stats_dir = "../../filtered_workloads_1s_stats"
    workload_indices = [1, 20, 3, 4, 400]

    gp = GeneticPool(clusters, workload_stats_dir)
    gp.add_workloads(workload_indices)
    population, _ = gp.run()
 
    print("==================================== INPUTS ====================================")
    print("Workloads:")
    gp.print_workloads()

    print("==================================== RESULTS ====================================")
    best_individual = tools.selBest(population, k=1)[0]
    print("Best Schedule:", best_individual)
    print("Best Fitness:", best_individual.fitness.values[0])