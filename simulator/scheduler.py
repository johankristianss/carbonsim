import random
import csv
import itertools
import pandas as pd
import numpy as np
from itertools import combinations 

class Scheduler:
    def __init__(self, csv_filename='/scratch/cognit/scheduler_metrics_best2.csv'):
    #def __init__(self, csv_filename='/scratch/cognit/scheduler_metrics_best.csv'):
    #def __init__(self, csv_filename='/scratch/cognit/scheduler_metrics_random.csv'):
    #def __init__(self, csv_filename='/scratch/cognit/scheduler_metrics_smallest.csv'):
    #def __init__(self, csv_filename='/scratch/cognit/scheduler_metrics_test.csv'):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization'])
        self.writer.writeheader()  # Write the header

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def rank_edge_clusters(self, process_duration, max_lookahead=86400):
        print(f"Ranking edge clusters based on carbon intensity over {process_duration} seconds")
        carbon_intensity_dict = {}
        lookahead_duration = min(process_duration, max_lookahead)

        available_edge_clusters = [cluster for cluster in self.__edge_clusters_dict.values() if cluster.available]

        for edge_cluster in available_edge_clusters:
            total_intensity = 0
            carbon_df = edge_cluster.carbon_df[edge_cluster.carbon_df['datetime'] >= self.tick_count].copy()
            carbon_df['adjusted_datetime'] = carbon_df['datetime'] - self.tick_count

            # Filter based on lookahead duration
            relevant_carbon_df = carbon_df[carbon_df['adjusted_datetime'] < lookahead_duration]

            if relevant_carbon_df.empty:
                average_intensity = 0
            else:
                total_intensity = relevant_carbon_df['carbonIntensity'].sum()
                average_intensity = total_intensity / lookahead_duration

            carbon_intensity_dict[edge_cluster] = average_intensity
            print(f"Edge cluster <{edge_cluster.name}> has average carbon intensity of {carbon_intensity_dict[edge_cluster]}")

        # Return edge clusters sorted by average carbon intensity
        return sorted(carbon_intensity_dict, key=carbon_intensity_dict.get)

    def simulate_best_cluster(self, clusters, process, next_processes):
        best_cluster_combination = None
        least_emissions = float('inf')

        # Total number of processes (main + next processes)
        total_processes = 1 + len(next_processes)  # Main process + next processes

        # Find all clusters with at least 1 available GPU
        available_clusters = [cluster for cluster in clusters if cluster.num_available >= 1]

        if len(available_clusters) < total_processes:
            print("Not enough clusters with available GPUs to run all processes.")
            return None

        # Generate all possible combinations of clusters
        possible_combinations = combinations(available_clusters, total_processes)

        for cluster_combination in possible_combinations:
            total_emissions = 0
            all_processes_fit = True

            print(f"Simulating combination {[cluster.name for cluster in cluster_combination]}")
            for idx, current_cluster in enumerate(cluster_combination):
                if idx == 0:
                    current_process = process
                else:
                    current_process = next_processes[idx - 1]

                print(f"Simulating process <{current_process.name}> on edge cluster <{current_cluster.name}>")
                current_emissions = current_cluster.calc_exact_emission(current_process, self.tick_count)
                total_emissions += current_emissions

                # Check if this cluster can still handle the process
                if current_cluster.num_available <= 0:
                    all_processes_fit = False
                    print(f"Not enough GPUs for process <{current_process.name}> on edge cluster <{current_cluster.name}>.")
                    break

            if all_processes_fit and total_emissions < least_emissions:
                least_emissions = total_emissions
                best_cluster_combination = cluster_combination

        if best_cluster_combination is None:
            print("No suitable cluster combination found for simulation.")
            return None

        # Return the first cluster to run the main process
        return best_cluster_combination[0]

    def run(self, process, next_processes=[]):
        available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]

        if not available_edge_clusters:
            return False

        ########### known emission ############
        # emissions_per_second = {}
        # for edge_cluster in available_edge_clusters:
        #     exact_emission = edge_cluster.calc_exact_emission(process, self.tick_count)
        #     duration = process.end_time - process.start_time
        #     emissions_per_second[edge_cluster] = exact_emission / duration
        #
        # edge_cluster = min(emissions_per_second, key=emissions_per_second.get)


        ########### random scheduling ############
        #edge_cluster = random.choice(available_edge_clusters)
        
        ########### smallest carbon intensity ############
        #available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
        #edge_cluster = available_edge_clusters[0]
     
        ################ look ahead 24 hours ############
        all_processes = [process] + next_processes
        max_duration = max(p.end_time - p.start_time for p in all_processes)

        ranked_clusters = self.rank_edge_clusters(max_duration)
        print(f"Ranked clusters: {[cluster.name for cluster in ranked_clusters]}")

        # Select the top clusters based on the number of processes
        selected_clusters = ranked_clusters[:len(all_processes)]

        # Simulate and find the best cluster for the main process
        edge_cluster = self.simulate_best_cluster(selected_clusters, process, next_processes)

        print(f"running process <{process.name}> on edge cluster <{edge_cluster.name}>")
        return edge_cluster.run(process)

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
