import random
import csv
from stats import get_process_power_draw_stat
from pool import ProcessPool
from timepool import ProcessTimePool
import os
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator/genetic')
from genetic_timepool import GeneticTimePool

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv', workloads_stats_dir='./filtered_workloads_1s_stats', alg='random', timepool_power_threshold=150, pool_size=50, pool_alg="mean"):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization', 'total_gpu_cost'])
        self.writer.writeheader()
        self.workloads_stats_dir = workloads_stats_dir
        self.__alg = alg
        self.__pool = ProcessPool(pool_size=pool_size, pool_alg=pool_alg)
        self.__timepool = ProcessTimePool(power_threshold=timepool_power_threshold)
        self.__genetic_timepool = GeneticTimePool(workloads_stats_dir)

        self.average_utilization_sum = 0

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def search_tree(self, current_idx, next_processes_idx, clusters, memo={}):
        """
        Recursively searches for the optimal process-to-cluster assignment
        based on carbon emissions and resource constraints.
        """
        # Create a unique memoization key
        memo_key = (current_idx, tuple(next_processes_idx), tuple(c.gpus for c in clusters))
        if memo_key in memo:
            return memo[memo_key]
    
        # Get power stats for the current process
        _, _, _, total_length_seconds = get_process_power_draw_stat(self.workloads_stats_dir, current_idx)
    
        # Base case: If no more processes are left to assign
        if not next_processes_idx:
            min_emission = float('inf')
            best_cluster = None
    
            for cluster in clusters:
                if cluster.gpus > 0:  # Only consider clusters with available GPUs
                    current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
                    if current_emission < min_emission:
                        min_emission = current_emission
                        best_cluster = cluster
    
            # If no valid placement is found, return infinity
            if best_cluster is None:
                memo[memo_key] = (float('inf'), [])
                return float('inf'), []
    
            # Return the best placement for the last process
            memo[memo_key] = (min_emission, [best_cluster])
            return min_emission, [best_cluster]
    
        # Recursive case: Assign the current process and proceed with the rest
        min_total_emission = float('inf')
        best_branch = None
    
        for cluster in clusters:
            # Skip clusters without available GPUs
            if cluster.gpus <= 0:
                continue
    
            # Simulate assigning the process to this cluster
            cluster.gpus -= 1  # Temporarily decrease available GPUs
            current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
    
            # Recursively calculate emissions for the remaining processes
            next_emission, next_branch = self.search_tree(
                next_processes_idx[0],  # Assign the next process
                next_processes_idx[1:],  # Remaining processes
                clusters,  # Pass updated clusters
                memo  # Memoization dictionary
            )
            cluster.gpus += 1  # Restore GPU count after recursion
    
            # Total emission for the current path
            total_emission = current_emission + next_emission
    
            # Update the best branch if the current path is better
            if total_emission < min_total_emission:
                min_total_emission = total_emission
                best_branch = [cluster] + next_branch
    
        # Store the result in memo
        memo[memo_key] = (min_total_emission, best_branch)
        return min_total_emission, best_branch

    def search_tree_old(self, current_idx, next_processes_idx, clusters, memo={}):
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
            next_emission, next_branch = self.search_tree(
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
                print("timepool.add_process failed")
                return False
          
            selected_processes = self.__timepool.select_processes()
            if selected_processes is None:
                print("timepool, selected_processes is None")
                return True # processes are queued in the pool, but don't need to run now 

            if len(selected_processes) == 0:
                print("timepool, selected_processes is empty")
                return True  # processes are queued in the pool, but don't need to run now

            for selected_process in selected_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                if len(available_edge_clusters) == 0:
                    return True # we return True because the process was added to the timepool
                #print("selected process: ", selected_process.name)
                selected_edge_cluster = available_edge_clusters[0]
                if selected_edge_cluster.run(selected_process):
                    self.__timepool.remove_process(selected_process.name)
            return True

        if self.__alg == 'genetic_timepool':
            print("----------------------- genetic time pool scheduling -----------------------")
            if self.__genetic_timepool.add_process(process) == False:
                print("genetic_timepool.add_process failed")
                return False
         
            # convert edge clusters dict to an array
            edge_clusters = list(self.__edge_clusters_dict.values())

            schedule = self.__genetic_timepool.calc_schedule(edge_clusters)
            print("schedule: ", schedule)
            if schedule is None:
                print("genetic_timepool, schedule is None")
                return True # processes are queued in the pool, but don't need to run now 

            if len(schedule) == 0:
                print("genetic_timepool, schedule is empty")
                return True  # processes are queued in the pool, but don't need to run now

            for process_idx, edge_cluster_name in schedule.items():
                print("process_idx: ", process_idx)
                print("edge_cluster_name: ", edge_cluster_name)
                if edge_cluster_name == 'wait':
                    continue
                edge_cluster = self.__edge_clusters_dict[edge_cluster_name]
                process = self.__genetic_timepool.get_process(process_idx)
                print("selected process: ", process.name)
                if edge_cluster.run(process):
                    self.__genetic_timepool.remove_process(process_idx)
            return True

        if self.__alg == 'pool':
            print("----------------------- pool scheduling -----------------------")
            if self.__pool.is_full():
                self.empty_pool()
            if self.__pool.add_process(process) == False:
                print("pool.add_process failed")
                return False
            return True

        elif self.__alg == 'lookahead':
            print("----------------------- lookahead scheduling -----------------------")
            ############# lookahead scheduling ############
            next_processes_idx = [int(os.path.basename(f).split('.')[0]) for f in next_process_csv_files]
            print("next_processes_idx: ", next_processes_idx)
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            print("available_edge_clusters: ", [edge_cluster.name for edge_cluster in available_edge_clusters])
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

        total_utilization = self.get_total_utilization()
        self.average_utilization_sum += total_utilization

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
    def avg_utilization(self):
        return self.average_utilization_sum / self.tick_count

    @property
    def total_gpus(self):
        return sum([edge_cluster.gpus for edge_cluster in self.__edge_clusters_dict.values()])
