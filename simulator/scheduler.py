import random
import csv
from stats import get_process_power_draw_stat
from pool import ProcessPool
from timepool import ProcessTimePool
import os

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv', workloads_stats_dir='./filtered_workloads_1s_stats', alg='random'):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization', 'total_gpu_cost'])
        self.writer.writeheader()
        self.workloads_stats_dir = workloads_stats_dir
        self.__alg = alg
        self.__pool = ProcessPool(pool_size=50)
        self.__timepool = ProcessTimePool(power_threshold=150)

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def search_tree(self, current_idx, next_processes_idx, clusters, memo={}):
        # Check if the result is already computed
        memo_key = (current_idx, tuple(next_processes_idx), tuple(c.name for c in clusters))
        if memo_key in memo:
            return memo[memo_key]
    
        # Get power stats for the current process
        _, _, _, total_length_seconds = get_process_power_draw_stat(self.workloads_stats_dir, current_idx)
    
        # Base case: if no next processes, calculate the emission for the current process on all clusters
        if not next_processes_idx:
            min_emission = float('inf')
            best_cluster = None
            for cluster in clusters:
                current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
                if current_emission < min_emission:
                    min_emission = current_emission
                    best_cluster = cluster
            # Return both the emission and the branch (a single-cluster list in this case)
            memo[memo_key] = (min_emission, [best_cluster])
            return min_emission, [best_cluster]
    
        # Recursive case: try each cluster for the current process and calculate emissions
        min_total_emission = float('inf')
        best_branch = None
    
        for cluster in clusters:
            # Calculate current process emission on the selected cluster
            current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
    
            # Calculate emissions and branch for the remaining processes
            next_emission, next_branch = self.minimize_emissions(
                next_processes_idx[0],
                next_processes_idx[1:],
                clusters,
                memo
            )
    
            # Total emission for this path
            total_emission = current_emission + next_emission
    
            # Track the branch with the smallest total emission
            if total_emission < min_total_emission:
                min_total_emission = total_emission
                best_branch = [cluster] + next_branch
    
        # Store the result in memo
        memo[memo_key] = (min_total_emission, best_branch)
        return min_total_emission, best_branch
   
    def finalize(self):
        if self.__alg == 'pool':
            self.empty_pool()
        if self.pool_size > 0:
            return False
        return True

    def empty_pool(self):
        while self.__pool.is_not_empty():
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if len(available_edge_clusters) == 0:
                return True
            selected_process = self.__pool.select_process()
            if selected_process is None:
                return False
            available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
            selected_edge_cluster = available_edge_clusters[0]
            selected_edge_cluster.run(selected_process)

    def run(self, process, next_process_csv_files=[]):
        print("=============================== run ================================")
        if self.__alg == 'timepool':
            print("----------------------- time pool scheduling -----------------------")
            if self.__timepool.add_process(process) == False:
                print("pool.add_process failed")
                return False
          
            selected_processes = self.__timepool.select_processes()
            if selected_processes is None:
                print("selected_processes is None")
                return True # processes are queued in the pool, but don't need to run now 

            if len(selected_processes) == 0:
                print("selected_processes is empty")
                return True  # processes are queued in the pool, but don't need to run now

            print("            ------------------------------------- ")
            for selected_process in selected_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                if len(available_edge_clusters) == 0:
                    return True # we return True because the process was added to the timepool
                print("selected process: ", selected_process.name)
                selected_edge_cluster = available_edge_clusters[0]
                if selected_edge_cluster.run(selected_process):
                    self.__timepool.remove_process(selected_process.name)

            return True
        if self.__alg == 'pool':
            print("----------------------- pool scheduling -----------------------")
            if self.__pool.add_process(process) == False:
                print("pool.add_process failed")
                return False
            
            if self.__pool.is_full():
                self.empty_pool()
            return True
        elif self.__alg == 'lookahead':
            print("----------------------- lookahead scheduling -----------------------")
            ############# lookahead scheduling ############
            next_processes_idx = [int(os.path.basename(f).split('.')[0]) for f in next_process_csv_files]
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if not available_edge_clusters:
                return False
            _, best_branch = self.search_tree(process.idx, next_processes_idx, available_edge_clusters) 
            if best_branch is None:
                print("best_branch is None")
                return False
            selected_edge_cluster = best_branch[0]
            if selected_edge_cluster is None:
                print("edge_cluster is None")
                return False
            print("selected best edge cluster: ", selected_edge_cluster.name)
            return selected_edge_cluster.run(process)
        elif self.__alg == 'greedy':
            print("----------------------- greedy scheduling -----------------------")
            ############# greedy scheduling ############
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if not available_edge_clusters:
                return False
            available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)

            selected_edge_cluster = available_edge_clusters[0]
            return selected_edge_cluster.run(process)
        elif self.__alg == 'random':
            print("-----------------------  random scheduling -----------------------")
            ############# random scheduling ############
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if not available_edge_clusters:
                return False
            selected_edge_cluster = random.choice(available_edge_clusters)
            print("selected edge cluster: ", selected_edge_cluster.name)
            return selected_edge_cluster.run(process)
        else :
            print("Invalid scheduling algorithm")
            return False

    def tick(self):
        for edge_cluster in self.__edge_clusters_dict.values():
            edge_cluster.tick()

        self.writer.writerow({
            'tick': self.tick_count,
            'cumulative_emission': self.cumulative_emission,
            'cumulative_energy': self.cumulative_energy,
            'utilization': self.get_total_utilization(),
            'total_gpu_cost': self.total_gpu_cost,
        })
        self.tick_count += 1

    @property
    def num_edge_clusters(self):
        return len(self.__edge_clusters_dict)

    @property
    def num_available_edge_clusters(self):
        return len([edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available])

    @property
    def num_running_processes(self):
        return sum([edge_cluster.num_running_processes for edge_cluster in self.__edge_clusters_dict.values()])

    def get_total_utilization(self):
        total_utilization = sum(edge_cluster.utilization for edge_cluster in self.__edge_clusters_dict.values())
        return total_utilization / self.num_edge_clusters if self.num_edge_clusters > 0 else 0

    @property
    def cumulative_energy(self):
        return sum([edge_cluster.cumulative_energy for edge_cluster in self.__edge_clusters_dict.values()])

    @property
    def cumulative_emission(self):
        return sum([edge_cluster.cumulative_emission for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def total_gpu_cost(self):
        return sum([edge_cluster.total_gpu_cost for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def total_processing_time(self):
        return sum([edge_cluster.total_processing_time for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def finished_processes(self):
        return sum([edge_cluster.finished_processes for edge_cluster in self.__edge_clusters_dict.values()])
   
    @property
    def total_processes(self):
        return sum([edge_cluster.total_processes for edge_cluster in self.__edge_clusters_dict.values()])

    @property
    def pool_size(self):
        return self.__pool.size()

    @property
    def total_gpus(self):
        return sum([edge_cluster.gpus for edge_cluster in self.__edge_clusters_dict.values()])
