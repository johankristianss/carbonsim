import random
import csv
import itertools
import pandas as pd
import numpy as np
from itertools import combinations 
import os

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv', workloads_stats='./filtered_workloads_1s_stats', alg='random'):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization'])
        self.writer.writeheader()  # Write the header
        self.workloads_stats = workloads_stats
        self.__alg = alg

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def get_process_power_draw_stat(self, idx):
        power_draw_mean = 0.0
        power_draw_median = 0.0
        total_length_seconds = 0

        stats_file = os.path.join(self.workloads_stats, f"{idx}_stats.csv")
    
        if not os.path.isfile(stats_file):
            print(f"Stats file not found: {stats_file}")
            return power_draw_mean, power_draw_median, total_length_seconds
    
        try:
            df = pd.read_csv(stats_file)
    
            power_draw_row = df[df['column_name'] == 'power_draw_W']
    
            if power_draw_row.empty:
                print(f"'power_draw_W' not found in stats file: {stats_file}")
                return power_draw_mean, power_draw_median, total_length_seconds
   
            power_draw_mean = float(power_draw_row.iloc[0]['mean'])
            power_draw_median = float(power_draw_row.iloc[0]['median'])
            total_length_seconds = int(power_draw_row.iloc[0]['total_length_seconds'])
    
        except Exception as e:
            print(f"Error reading stats file {stats_file}: {e}")

        return power_draw_mean, power_draw_median, total_length_seconds

    def minimize_emissionsOld(self, current_idx, next_process_idxs, clusters, memo={}):
        # Check if the result is already computed
        memo_key = (current_idx, tuple(next_process_idxs), tuple(c.name for c in clusters))
        if memo_key in memo:
            return memo[memo_key]
    
        # Get power stats for the current process
        _, _, total_length_seconds = self.get_process_power_draw_stat(current_idx)
    
        # Base case: if no next processes, calculate the emission for current process on all clusters
        if not next_process_idxs:
            min_emission = float('inf')
            best_cluster = None
            for cluster in clusters:
                current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
                if current_emission < min_emission:
                    min_emission = current_emission
                    best_cluster = cluster
            # Return both the minimum emission and the best cluster for this step
            memo[memo_key] = (min_emission, best_cluster)
            return min_emission, best_cluster
    
        # Recursive case: try each cluster for the current process and calculate emissions
        min_total_emission = float('inf')
        best_cluster = None
    
        for cluster in clusters:
            # Calculate current process emission on the selected cluster
            current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
    
            # Calculate emissions for the remaining processes
            next_emission, _ = self.minimize_emissionsOld(
                next_process_idxs[0],
                next_process_idxs[1:],
                clusters,
                memo
            )
    
            # Total emission for this path
            total_emission = current_emission + next_emission
    
            # Track the cluster that results in the smallest total emission
            if total_emission < min_total_emission:
                min_total_emission = total_emission
                best_cluster = cluster
    
        # Store the result in memo
        memo[memo_key] = (min_total_emission, best_cluster)
        return min_total_emission, best_cluster

    def minimize_emissions(self, current_idx, next_process_idxs, clusters, memo={}):
        # Check if the result is already computed
        memo_key = (current_idx, tuple(next_process_idxs), tuple(c.name for c in clusters))
        if memo_key in memo:
            return memo[memo_key]
    
        # Get power stats for the current process
        _, _, total_length_seconds = self.get_process_power_draw_stat(current_idx)
    
        # Base case: if no next processes, calculate the emission for the current process on all clusters
        if not next_process_idxs:
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
                next_process_idxs[0],
                next_process_idxs[1:],
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
    

    def run(self, process, next_process_idxs=[]):
        available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]

        if not available_edge_clusters:
            return False

        # print("current process idxs: ", process.idx)
        # print("next processn idxs: ", next_process_idxs)

        # extract the power draw stats for the current process
        # power_draw_mean, power_draw_median, total_length_seconds = self.get_process_power_draw_stat(process.idx)
        # print("power_draw_mean: ", power_draw_mean)
        # print("power_draw_median: ", power_draw_median)
        # print("total_length_seconds: ", total_length_seconds)

        # # extract the power draw stats for the next processes
        # next_process_power_draw_means = []
        # next_process_power_draw_medians = []
        # next_process_total_length_seconds = []
        #
        # for idx in next_process_idxs:
        #     power_draw_mean, power_draw_median, total_length_seconds = self.get_process_power_draw_stat(idx)
        #     next_process_power_draw_means.append(power_draw_mean)
        #     next_process_power_draw_medians.append(power_draw_median)
        #     next_process_total_length_seconds.append(total_length_seconds)
        #
        # print("next_process_power_draw_means: ", next_process_power_draw_means)
        # print("next_process_power_draw_medians: ", next_process_power_draw_medians)
        # print("next_process_total_length_seconds: ", next_process_total_length_seconds)

    #     for cluster in available_edge_clusters:
    #         # Calculate emissions for the current process
    #         current_emission = cluster.integrate_carbon_intensity(total_length_seconds)
    # 
    #         # Calculate emissions for the next processes
    #         next_emissions = []
    #         for next_idx in next_process_idxs:
    #             power_draw_mean, power_draw_median, total_length_seconds = self.get_process_power_draw_stat(next_idx)
    #             next_emissions.append(cluster.integrate_carbon_intensity(total_length_seconds))
    # 
    #         # Print the emissions
    #         print(f"Cluster: {cluster._EdgeCluster__name}")
    #         print(f"  Emissions for Current Process ({process.idx}): {current_emission}")
    #         print(f"  Emissions for Next Processes ({next_process_idxs}): {next_emissions}")
    #         print("-" * 50)
      
        if self.__alg == 'lookahead':
            ############# lookahead scheduling ############
            min_emission_total, best_branch = self.minimize_emissions(process.idx, next_process_idxs, available_edge_clusters) 
            # print("min_emission_total: ", min_emission_total)
            # print("best_branch: ", best_branch)
            if best_branch is None:
                print("best_branch is None")
                return False
            edge_cluster = best_branch[0]
            if edge_cluster is None:
                print("edge_cluster is None")
                return False
            print("selected best edge cluster: ", edge_cluster.name)
            return edge_cluster.run(process)
        elif self.__alg == 'random':
            ############# random scheduling ############
            edge_cluster = random.choice(available_edge_clusters)
            print("random edge cluster: ", edge_cluster.name)
            return edge_cluster.run(process)
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
            'utilization': self.get_total_utilization()
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
    def total_gpus(self):
        return sum([edge_cluster.gpus for edge_cluster in self.__edge_clusters_dict.values()])
