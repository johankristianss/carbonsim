import random
import csv
import itertools
import pandas as pd
import numpy as np
from itertools import combinations 

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv'):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization'])
        self.writer.writeheader()  # Write the header

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    
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
        edge_cluster = random.choice(available_edge_clusters)
        
        ########### smallest carbon intensity ############
        #available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
        #edge_cluster = available_edge_clusters[0]
     
        ################ look ahead 24 hours ############
        # all_processes = [process] + next_processes
        # max_duration = max(p.end_time - p.start_time for p in all_processes)
        #
        # ranked_clusters = self.rank_edge_clusters(max_duration)
        # print(f"Ranked clusters: {[cluster.name for cluster in ranked_clusters]}")
        #
        # # Select the top clusters based on the number of processes
        # selected_clusters = ranked_clusters[:len(all_processes)]
        #
        # # Simulate and find the best cluster for the main process
        # edge_cluster = self.simulate_best_cluster(selected_clusters, process, next_processes)
        #
        # print(f"running process <{process.name}> on edge cluster <{edge_cluster.name}>")
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
